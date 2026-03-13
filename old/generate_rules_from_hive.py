#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据 Hive 元数据自动生成 rules.generated.json

功能：
- 直连 Hive Metastore 所在的 MySQL，一次性读取指定库下所有表和字段类型；
- 为每张表生成一条规则：
  - metrics 固定包含 row_count；
  - 对所有 decimal / numeric 类型字段生成 sum(metric) 指标；
  - 不生成 keys（主键去重校验不做）。
- 所有配置从 config.json 文件中读取，避免硬编码；
- 自动从元数据中读取分区列和 decimal 配置，无需手动配置；
- 支持命令行参数和配置文件两种方式配置测试模式。

使用示例（最简）：
  python generate_rules_from_hive.py \
    --output rules.generated.json

使用示例（指定测试模式）：
  # 测试所有表（默认）
  python generate_rules_from_hive.py \
    --output rules.generated.json \
    --test-mode all

  # 仅测试指定表（可接受多个表名）
  python generate_rules_from_hive.py \
    --output rules.generated.json \
    --test-mode selected \
    --include-tables gmall.dim_activity_full gmall.dim_coupon_full

  # 排除特定表
  python generate_rules_from_hive.py \
    --output rules.generated.json \
    --test-mode excluded \
    --exclude-tables gmall.ads_page_path gmall.tmp_dim_date_info

依赖：
- PyMySQL（通过 Metastore MySQL 获取字段时）
- JSON（Python 标准库，无需额外安装）
"""

import argparse
import json
import sys
from typing import List, Dict, Tuple


def dataclass(cls):
    """Python 3.6 compatible dataclass decorator (minimal implementation)."""
    def init(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def repr(self):
        attrs = ', '.join(f'{k}={v!r}' for k, v in self.__dict__.items())
        return f'{self.__class__.__name__}({attrs})'

    def eq(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.__dict__ == other.__dict__
    cls.__init__ = init
    cls.__repr__ = repr
    cls.__eq__ = eq
    return cls


class Config:
    """从 config.json 加载配置的类"""
    def __init__(self, filename="config.json"):
        self.config = {}
        try:
            with open(filename, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        except FileNotFoundError:
            print(f"ERROR: {filename} 文件未找到，请确保文件存在", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: 读取 {filename} 文件失败：{e}", file=sys.stderr)
            sys.exit(1)

    def get_required(self, path: str):
        """支持点号路径访问嵌套配置，如果不存在则报错"""
        keys = path.split(".")
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                raise ValueError(f"缺少必需的配置项: {path}")
        return value

try:
    import pymysql  # type: ignore
except ImportError:
    pymysql = None

@dataclass
class ColumnInfo:
    name: str
    type: str

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate rules.generated.json from Hive metadata")
    parser.add_argument(
        "--output",
        required=True,
        help="Output JSON path"
    )
    parser.add_argument(
        "--test-mode",
        choices=["all", "selected", "excluded"],
        default="all",
        help="Test mode: 'all' (default), 'selected', or 'excluded'"
    )
    parser.add_argument(
        "--include-tables",
        nargs="*",
        default=[],
        help="List of table names to include (only used in 'selected' mode)"
    )
    parser.add_argument(
        "--exclude-tables",
        nargs="*",
        default=[],
        help="List of table names to exclude (only used in 'excluded' mode)"
    )
    parser.add_argument(
        "--default-where",
        default="dt='${biz_date}'",
        help="Default where clause template, default \"dt='${biz_date}'\""
    )
    return parser.parse_args()


def get_partition_columns_from_metastore_mysql(
    host: str, port: int, user: str, password: str, db: str,
    database_name: str, table_name: str
) -> list:
    """
    从 Hive Metastore 中获取表的分区列信息。

    """
    if pymysql is None:
        print(
            "ERROR: pymysql 未安装，请先安装：pip install pymysql",
            file=sys.stderr,
        )
        sys.exit(1)

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )

    sql = """SELECT
      p.PKEY_NAME
    FROM PARTITION_KEYS p
    JOIN TBLS t ON t.TBL_ID = p.TBL_ID
    JOIN DBS d ON t.DB_ID = d.DB_ID
    WHERE d.NAME = %s AND t.TBL_NAME = %s
    ORDER BY p.INTEGER_IDX
    """

    partition_cols = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (database_name, table_name))
            for col_name, in cur.fetchall():
                c = (col_name or "").strip()
                if c:
                    partition_cols.append(c)
    finally:
        conn.close()

    return partition_cols


def get_decimal_precision_and_scale(type_str: str) -> tuple:
    """从字段类型字符串解析 decimal 的精度和小数位数"""
    if '(' in type_str:
        # 提取括号内的内容，例如 (18,2)
        content = type_str[type_str.find('(')+1:type_str.rfind(')')]
        parts = [p.strip() for p in content.split(',')]
        if len(parts) >= 1:
            precision = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
            return precision, scale
    # 默认值
    return 18, 2


def get_columns_from_metastore_mysql(config: Config) -> dict:
    """
    直接从 Hive Metastore 所在的 MySQL 中一次性查询指定库下所有表的列信息。

    """
    if pymysql is None:
        print(
            "ERROR: pymysql 未安装，请先安装：pip install pymysql",
            file=sys.stderr,
        )
        sys.exit(1)

    host = config.get_required("metastore_mysql.host")
    port = config.get_required("metastore_mysql.port")
    user = config.get_required("metastore_mysql.user")
    password = config.get_required("metastore_mysql.password")
    db = config.get_required("metastore_mysql.db")
    database_name = config.get_required("metastore_mysql.hive_database")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )

    sql = """SELECT
      t.TBL_NAME,
      c.COLUMN_NAME,
      c.TYPE_NAME
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
    WHERE d.NAME = %s
    ORDER BY t.TBL_NAME, c.INTEGER_IDX
    """

    columns_by_table = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (database_name,))
            for tbl_name, col_name, type_name in cur.fetchall():
                t = (tbl_name or "").strip()
                c = (col_name or "").strip()
                dt = (type_name or "").strip().lower()
                if not t or not c:
                    continue
                columns_by_table.setdefault(t, []).append(
                    ColumnInfo(name=c, type=dt)
                )
    finally:
        conn.close()

    return columns_by_table


def build_table_rule(
    config: Config,
    database: str,
    table: str,
    columns: list,
    default_where: str,
    test_mode: str,
    include_tables: List[str],
    exclude_tables: List[str]
) -> Dict:
    full_name = f"{database}.{table}"

    # 确定是否启用该表测试（基于测试模式）
    should_test = True
    if test_mode == "selected":
        if full_name not in include_tables:
            should_test = False
    elif test_mode == "excluded":
        if full_name in exclude_tables:
            should_test = False

    # 构建表规则：如果不应测试，则只设置 enabled=False
    rule = {
        "table": full_name,
        "enabled": should_test,
        "partition_cols": [],
        "keys": [],
        "metrics": [
            {
                "name": "row_count",
                "expr": "count(1)"
            }
        ]
    }

    # 获取分区列（如果需要）
    if "${biz_date}" in default_where and should_test:
        try:
            host = config.get_required("metastore_mysql.host")
            port = config.get_required("metastore_mysql.port")
            user = config.get_required("metastore_mysql.user")
            password = config.get_required("metastore_mysql.password")
            db = config.get_required("metastore_mysql.db")
            partition_cols = get_partition_columns_from_metastore_mysql(
                host, port, user, password, db, database, table
            )
            rule["partition_cols"] = partition_cols
        except Exception as e:
            print(f"WARNING: 获取分区列信息失败，将使用默认条件: {e}", file=sys.stderr)
            # 失败时不设置 partition_cols

    # 为 decimal/numeric 字段添加 sum metrics
    for col in columns:
        if col.type.startswith("decimal") or col.type.startswith("numeric"):
            precision, scale = get_decimal_precision_and_scale(col.type)
            metric_name = f"{col.name}_sum"
            expr = f"sum(cast({col.name} as decimal({precision},{scale})))"
            rule["metrics"].append({
                "name": metric_name,
                "expr": expr
            })

    return rule


def main() -> None:
    # 加载配置文件
    config = Config("config.json")

    # 解析命令行参数
    args = parse_args()

    # 必需配置
    output_path = args.output
    test_mode = args.test_mode
    include_tables = args.include_tables
    exclude_tables = args.exclude_tables
    default_where = args.default_where

    # 从 Metastore MySQL 获取列信息
    columns_by_table = {}
    try:
        columns_by_table = get_columns_from_metastore_mysql(config)
        if columns_by_table:
            database_name = config.get_required("metastore_mysql.hive_database")
            print(
                f"Loaded table metadata from Metastore MySQL: "
                f"database={database_name}, tables={len(columns_by_table)}"
            )
        else:
            database_name = config.get_required("metastore_mysql.hive_database")
            print(
                f"No tables found in Metastore for database={database_name}",
                file=sys.stderr,
            )
    except Exception as e:  # noqa: BLE001
        print(
            f"ERROR: 从 Metastore MySQL 获取列元数据失败，原因：{e!r}",
            file=sys.stderr,
        )
        sys.exit(1)

    table_rules = []

    # 只处理指定数据库中的表（这里假设处理数据库中的所有表）
    target_tables = sorted(columns_by_table.keys())

    print(f"Generating rules for database={config.get_required('metastore_mysql.hive_database')}, tables={len(target_tables)}")

    for t in target_tables:
        cols = columns_by_table.get(t)
        if not cols:
            print(f"Skipping table without metadata: {t}", file=sys.stderr)
            continue

        # 构建表规则
        rule = build_table_rule(
            config=config,
            database=config.get_required("metastore_mysql.hive_database"),
            table=t,
            columns=cols,
            default_where=default_where,
            test_mode=test_mode,
            include_tables=include_tables,
            exclude_tables=exclude_tables
        )
        table_rules.append(rule)

    # 生成最终的配置对象，并加入测试配置部分
    output_config = {
        "mode": {
            "include_all": True,
            "exclude_tables": exclude_tables,
            "selected_tables": include_tables
        },
        "tables": table_rules
    }

    # 写入文件时，先添加文档说明作为注释，然后写入实际配置
    with open(output_path, "w", encoding="utf-8") as f:
        # 写入文档说明作为注释（注意：JSON格式中不能有注释，所以使用//开头的注释）
        f.write("// ### 表选择模式配置说明\n")
        f.write("//\n")
        f.write("// `mode` 对象中的三个参数遵循严格的互斥逻辑，共有三种合法配置方式：\n")
        f.write("//\n")
        f.write("// 1. **全量包含（无排除）**\n")
        f.write("// ```json\n")
        f.write("// \"mode\": {\n")
        f.write("//   \"include_all\": true,\n")
        f.write("//   \"exclude_tables\": [],\n")
        f.write("//   \"selected_tables\": []\n")
        f.write("// }\n")
        f.write("// ```\n")
        f.write("// - 处理 `tables` 数组中的所有表\n")
        f.write("// - 这是默认的最简配置\n")
        f.write("//\n")
        f.write("// 2. **全量包含 + 排除列表**\n")
        f.write("// ```json\n")
        f.write("// \"mode\": {\n")
        f.write("//   \"include_all\": true,\n")
        f.write("//   \"exclude_tables\": [\"gmall.dim_user_zip\", \"gmall.tmp_dim_date_info\"],\n")
        f.write("//   \"selected_tables\": []\n")
        f.write("// }\n")
        f.write("// ```\n")
        f.write("// - 处理所有表，但明确排除 `exclude_tables` 中列出的表\n")
        f.write("// - 用于生产环境日常运行，排除测试表、临时表或敏感数据表\n")
        f.write("//\n")
        f.write("// 3. **白名单精确选择**\n")
        f.write("// ```json\n")
        f.write("// \"mode\": {\n")
        f.write("//   \"include_all\": false,\n")
        f.write("//   \"exclude_tables\": [],\n")
        f.write("//   \"selected_tables\": [\"gmall.dwd_trade_pv_inc\", \"gmall.dwd_traffic_page_view_inc\"]\n")
        f.write("// }\n")
        f.write("// ```\n")
        f.write("// - 只处理 `selected_tables` 中明确列出的表\n")
        f.write("// - 用于安全敏感场景、特定主题分析或性能优化\n")
        f.write("//\n")
        f.write("// > **注意**：这三种模式是互斥的。系统根据 `include_all` 的布尔值自动切换到对应模式，并严格检查另外两个数组是否符合空值要求。\n")
        f.write("//\n")

        # 写入实际的配置内容（JSON）
        json.dump(output_config, f, indent=2, ensure_ascii=False)

    print(f"规则文件已生成到: {output_path}")


if __name__ == "__main__":
    main()