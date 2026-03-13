#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据 Hive 元数据自动生成 rules 和 SQL 文件

功能：
- 直连 Hive Metastore 所在的 MySQL，一次性读取所有库下所有表和字段类型；
- 为每张表生成一条规则：
  - metrics 固定包含 row_count；
  - 对所有 decimal / numeric 类型字段生成 sum(metric) 指标；
  - 不生成 keys（主键去重校验不做）。
- Metastore 连接配置从 config.json 文件读取，避免硬编码；
- 自动从元数据中读取分区列和 decimal 配置，无需手动配置。
- 可选生成预编译的 SQL 文件，执行时直接使用，无需每次重新生成。

使用示例：
  # 只生成 rules 文件
  python generate_rules_from_hive_metastore.py \
    --output rules.generated.json

  # 生成 rules + SQL
  python generate_rules_from_hive_metastore.py \
    --output rules.generated.json \
    --generate-sql \
    --run-id test_001

  # 只生成 SQL（基于已有的 rules 文件）
  python generate_rules_from_hive_metastore.py \
    --input rules.generated.json \
    --generate-sql \
    --run-id test_001

依赖：
- PyMySQL（通过 Metastore MySQL 获取字段时）
- JSON（Python 标准库，无需额外安装）
"""

import argparse
import hashlib
import json
import os
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
    def __init__(self, filename="config.json", override_path: str = None):
        if override_path:
            filename = override_path
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

def escape_sql_string(text: str) -> str:
    return text.replace("'", "''")


def generate_hash(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


def _build_partition_spec_expr(partition_cols: List[str]) -> str:
    if not partition_cols:
        return "''"
    parts = [f"concat('{col}=', cast({col} as string))" for col in partition_cols]
    return f"concat_ws('/', {', '.join(parts)})"


def _build_group_by_sql(partition_cols: List[str]) -> str:
    if not partition_cols:
        return ""
    return f" GROUP BY {', '.join(partition_cols)}"


def build_summary_sql(
    table: dict,
    run_id: str,
    biz_date: str = None,
    default_where: str = "1=1",
) -> str:
    """Build SQL for a single table."""
    t = table
    table_name = t["table"]
    where = t.get("where", default_where)
    partition_cols = t.get("partition_cols", [])

    if partition_cols:
        if biz_date:
            where = where.replace("${biz_date}", biz_date)
    else:
        if "${biz_date}" in where or where == default_where:
            where = "1=1"

    keys = t.get("keys", [])
    metrics = t.get("metrics", [])

    where_hash = generate_hash(where)
    where_escaped = escape_sql_string(where)
    partition_spec_expr = _build_partition_spec_expr(partition_cols)
    group_by_sql = _build_group_by_sql(partition_cols)

    parts: List[str] = []

    for m in metrics:
        name, expr = m["name"], m["expr"]
        expr_escaped = escape_sql_string(expr)
        select_cols = [
            f"'{table_name}' as table_name",
            "'metric' as check_type",
            f"'{name}' as metric_name",
            f"'{expr_escaped}' as metric_expr",
            f"cast({expr} as string) as value",
            f"'{where_escaped}' as where_clause",
            f"'{where_hash}' as where_hash",
            f"{partition_spec_expr} as partition_spec",
            "current_timestamp() as computed_at",
            f"'{run_id}' as run_id",
        ]
        parts.append(
            f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE {where}{group_by_sql}"
        )

    if keys:
        keys_expr = ", ".join(keys)
        select_cols = [
            f"'{table_name}' as table_name",
            "'pk_dup' as check_type",
            "'pk_dup_count' as metric_name",
            f"'count(*) - count(distinct {keys_expr})' as metric_expr",
            f"cast(count(*) - count(distinct {keys_expr}) as string) as value",
            f"'{where_escaped}' as where_clause",
            f"'{where_hash}' as where_hash",
            f"{partition_spec_expr} as partition_spec",
            "current_timestamp() as computed_at",
            f"'{run_id}' as run_id",
        ]
        parts.append(
            f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE {where}{group_by_sql}"
        )

    return "\nUNION ALL\n".join(parts)


def generate_sql_files(
    config: dict,
    run_id: str,
    biz_date: str = None,
    output_dir: str = "output",
    default_where: str = "1=1",
) -> None:
    """Generate SQL files, one per database."""
    tables = config.get("tables", [])
    mode = config.get("mode", {})

    include_all = mode.get("include_all", True)
    exclude_tables = set(mode.get("exclude_tables", []))
    selected_tables = set(mode.get("selected_tables", []))

    if not include_all:
        filtered_tables = [t for t in tables if t.get("enabled", True) and f"{t['table']}" in selected_tables]
    else:
        filtered_tables = [t for t in tables if t.get("enabled", True) and f"{t['table']}" not in exclude_tables]

    os.makedirs(output_dir, exist_ok=True)

    # Group tables by database
    tables_by_db: Dict[str, List[dict]] = {}
    for t in filtered_tables:
        table_name = t["table"]
        db_name = table_name.split(".")[0] if "." in table_name else "default"
        tables_by_db.setdefault(db_name, []).append(t)

    # Generate one SQL file per database
    for db_name, db_tables in tables_by_db.items():
        sql_statements = []
        for t in db_tables:
            sql = build_summary_sql(t, run_id, biz_date, default_where)
            sql_statements.append(sql)

        combined_sql = "\n\nUNION ALL\n\n".join(sql_statements)

        sql_file = os.path.join(output_dir, f"{db_name}.sql")
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(f"-- Database: {db_name}\n")
            f.write(f"-- Run ID: {run_id}\n")
            f.write(f"-- Biz Date: {biz_date or 'N/A'}\n")
            f.write(f"-- Tables: {len(db_tables)}\n")
            f.write(f"-- Generated from rules\n")
            f.write("\n")
            f.write(combined_sql)
            f.write(";\n")

    print(f"Generated {len(tables_by_db)} database SQL files in {output_dir}/")
    print(f"Databases: {list(tables_by_db.keys())}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate rules and SQL from Hive metadata")
    parser.add_argument(
        "--output",
        help="Output JSON rules path (required if not using --input)"
    )
    parser.add_argument(
        "--input",
        help="Input JSON rules file (for generating SQL only)"
    )
    parser.add_argument(
        "--generate-sql",
        action="store_true",
        help="Generate SQL files in addition to rules"
    )
    parser.add_argument(
        "--run-id",
        help="Run ID for SQL generation (required if --generate-sql is used)"
    )
    parser.add_argument(
        "--biz-date",
        help="Business date for SQL generation"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory for SQL files (default: output)"
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Config file path (default: config.json)"
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


def load_json(path: str) -> Dict:
    """Load JSON file with support for // comments."""
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    # Remove // comments
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        # Skip comment-only lines
        if stripped.startswith('//'):
            continue
        # Remove inline // comments
        if '//' in line:
            idx = line.find('//')
            before = line[:idx].rstrip()
            if before:
                cleaned_lines.append(before)
        else:
            cleaned_lines.append(line.rstrip())

    return json.loads('\n'.join(cleaned_lines))


def main() -> None:
    # 解析命令行参数
    args = parse_args()

    # 判断模式：从输入文件加载 还是 从 Metastore 生成
    if args.input:
        # 从已有的 rules 文件加载
        print(f"Loading rules from: {args.input}")
        output_config = load_json(args.input)
        config = None  # 不需要 config
    else:
        # 需要生成 rules
        if not args.output:
            print("ERROR: --output is required when not using --input", file=sys.stderr)
            sys.exit(1)

        # 加载配置文件
        config = Config(args.config)

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

        # 写入 rules 文件
        with open(args.output, "w", encoding="utf-8") as f:
            # 写入文档说明作为注释
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

        print(f"规则文件已生成到: {args.output}")

    # 生成 SQL 文件
    if args.generate_sql:
        if not args.run_id:
            print("ERROR: --run-id is required when --generate-sql is used", file=sys.stderr)
            sys.exit(1)

        print(f"\nGenerating SQL files...")
        generate_sql_files(
            config=output_config,
            run_id=args.run_id,
            biz_date=args.biz_date,
            output_dir=args.output_dir,
            default_where="1=1",
        )


if __name__ == "__main__":
    main()