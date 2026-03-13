#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据 Hive 元数据自动生成 rules.generated.json

功能：
- 直连 Hive Metastore 所在的 MySQL，一次性读取所有库下所有表和字段类型；
- 为每张表生成一条规则：
  - metrics 固定包含 row_count；
  - 对所有 decimal / numeric 类型字段生成 sum(metric) 指标；
  - 不生成 keys（主键去重校验不做）。
- Metastore 连接配置从 config.json 文件读取，避免硬编码；
- 自动从元数据中读取分区列和 decimal 配置，无需手动配置。

使用示例：
  python generate_rules_from_hive_metastore.py \
    --output rules.generated.json

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
    return parser.parse_args()


def get_partition_columns_from_metastore_mysql(
    config: Config,
    database_name: str,
    table_name: str
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

    host = config.get_required("metastore_mysql.host")
    port = config.get_required("metastore_mysql.port")
    user = config.get_required("metastore_mysql.user")
    password = config.get_required("metastore_mysql.password")
    db = config.get_required("metastore_mysql.db")

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


def get_all_databases(config: Config) -> list:
    """
    从 Hive Metastore MySQL 中获取所有数据库列表。
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

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )

    sql = """SELECT NAME FROM DBS ORDER BY NAME"""

    databases = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            for (db_name,) in cur.fetchall():
                d = (db_name or "").strip()
                if d:
                    databases.append(d)
    finally:
        conn.close()

    return databases


def get_columns_from_metastore_mysql(config: Config) -> dict:
    """
    直接从 Hive Metastore 所在的 MySQL 中一次性查询所有库下所有表的列信息。
    返回格式: {(database, table): [ColumnInfo, ...], ...}
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
      d.NAME,
      t.TBL_NAME,
      c.COLUMN_NAME,
      c.TYPE_NAME
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
    ORDER BY d.NAME, t.TBL_NAME, c.INTEGER_IDX
    """

    columns_by_table = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            for db_name, tbl_name, col_name, type_name in cur.fetchall():
                db = (db_name or "").strip()
                t = (tbl_name or "").strip()
                c = (col_name or "").strip()
                dt = (type_name or "").strip().lower()
                if not db or not t or not c:
                    continue
                key = (db, t)
                columns_by_table.setdefault(key, []).append(
                    ColumnInfo(name=c, type=dt)
                )
    finally:
        conn.close()

    return columns_by_table


def build_table_rule(
    config: Config,
    database: str,
    table: str,
    columns: list
) -> Dict:
    full_name = f"{database}.{table}"

    # 构建表规则
    rule = {
        "table": full_name,
        "enabled": True,
        "partition_cols": [],
        "keys": [],
        "metrics": [
            {
                "name": "row_count",
                "expr": "count(1)"
            }
        ]
    }

    # 获取分区列
    try:
        partition_cols = get_partition_columns_from_metastore_mysql(
            config, database, table
        )
        rule["partition_cols"] = partition_cols
    except Exception as e:
        print(f"WARNING: 获取分区列信息失败: {e}", file=sys.stderr)

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

    # 从 Metastore MySQL 获取所有库的数据库列表
    try:
        databases = get_all_databases(config)
        print(
            f"Loaded databases from Metastore MySQL: "
            f"databases={len(databases)}"
        )
    except Exception as e:
        print(
            f"ERROR: 从 Metastore MySQL 获取数据库列表失败，原因：{e!r}",
            file=sys.stderr,
        )
        sys.exit(1)

    # 从 Metastore MySQL 获取列信息
    columns_by_table = {}
    try:
        columns_by_table = get_columns_from_metastore_mysql(config)
        if columns_by_table:
            print(
                f"Loaded table metadata from Metastore MySQL: "
                f"tables={len(columns_by_table)}"
            )
        else:
            print(
                f"No tables found in Metastore",
                file=sys.stderr,
            )
    except Exception as e:  # noqa: BLE001
        print(
            f"ERROR: 从 Metastore MySQL 获取列元数据失败，原因：{e!r}",
            file=sys.stderr,
        )
        sys.exit(1)

    table_rules = []

    # 处理所有库中的所有表
    target_tables = sorted(columns_by_table.keys())

    print(f"Generating rules for {len(target_tables)} tables across {len(databases)} databases")

    for db, tbl in target_tables:
        cols = columns_by_table.get((db, tbl))
        if not cols:
            print(f"Skipping table without metadata: {db}.{tbl}", file=sys.stderr)
            continue

        # 构建表规则
        rule = build_table_rule(
            config=config,
            database=db,
            table=tbl,
            columns=cols
        )
        table_rules.append(rule)

    # 生成最终的配置对象
    output_config = {
        "mode": {
            "include_all": True,
            "exclude_tables": [],
            "selected_tables": []
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