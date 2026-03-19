#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据 Hive 元数据直接生成 SQL 文件

功能：
- 直连 Hive Metastore 所在的 MySQL，读取指定表或所有表的字段类型；
- 为每张表生成规则：
  - metrics 固定包含 row_count；
  - 对所有 decimal / numeric 类型字段生成 sum(metric) 指标；
  - 不生成 keys（主键去重校验不做）。
- 支持从JSON配置文件读取表列表和字段信息
- Metastore 连接配置从 config.json 文件读取，避免硬编码；
- 自动从元数据中读取分区列和 decimal 配置，无需手动配置。
- 生成的 SQL 使用 INSERT INTO 写入结果表，每个 metric 一条语句，用分号分隔。

使用示例：
  # 指定表列表文件生成 SQL
  python hive_rules_sql_generator.py \
    --table-list tables.txt \
    --data-dt 2024-01-01

  # 指定JSON配置文件生成 SQL
  python hive_rules_sql_generator.py \
    --table-list test_tables.json \
    --data-dt 2024-01-01

  # 不指定表列表则处理所有表
  python hive_rules_sql_generator.py --data-dt 2024-01-01

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


def get_validation_db(config: Config) -> str:
    """从配置文件获取校验数据库名"""
    try:
        return config.get_required("validation_db")
    except ValueError:
        return "validation_db"


def build_metric_sql(
    table_name: str,
    metric_name: str,
    metric_expr: str,
    keys: list,
    partition_cols: list,
    where: str,
    data_dt: str,
    config: Config,
) -> str:
    """Build INSERT INTO SQL for a single metric."""
    metric_expr_escaped = escape_sql_string(metric_expr)
    partition_spec_expr = _build_partition_spec_expr(partition_cols)
    group_by_sql = _build_group_by_sql(partition_cols)

    if keys:
        # PK duplicate check
        keys_expr = ", ".join(keys)
        select_cols = [
            f"'{table_name}' as table_name",
            "'pk_dup' as check_type",
            "'pk_dup_count' as metric_name",
            f"'count(*) - count(distinct {keys_expr})' as metric_expr",
            f"cast(count(*) - count(distinct {keys_expr}) as string) as value",
            f"{partition_spec_expr} as partition_spec",
            "current_timestamp() as computed_at",
            f"'{data_dt}' as data_dt",
        ]
    else:
        # Regular metric
        select_cols = [
            f"'{table_name}' as table_name",
            "'metric' as check_type",
            f"'{metric_name}' as metric_name",
            f"'{metric_expr_escaped}' as metric_expr",
            f"cast({metric_expr} as string) as value",
            f"{partition_spec_expr} as partition_spec",
            "current_timestamp() as computed_at",
            f"'{data_dt}' as data_dt",
        ]

    select_sql = f"SELECT {', '.join(select_cols)} FROM {table_name} WHERE {where}{group_by_sql}"
    validation_db = get_validation_db(config)
    return f"INSERT INTO TABLE {validation_db}.old_summary {select_sql}"


def generate_sql_files(
    table_rules: list,
    data_dt: str,
    config: Config,
    output_dir: str = "output",
    default_where: str = "1=1",
) -> None:
    """Generate SQL file with INSERT INTO statements (one per metric)."""
    os.makedirs(output_dir, exist_ok=True)

    # Collect all SQL statements (one per metric)
    all_sql_statements = []
    for t in table_rules:
        table_name = t["table"]
        where = t.get("where", default_where)
        partition_cols = t.get("partition_cols", [])
        keys = t.get("keys", [])
        metrics = t.get("metrics", [])

        # Process each metric
        for m in metrics:
            metric_name = m["name"]
            metric_expr = m["expr"]
            sql = build_metric_sql(
                table_name=table_name,
                metric_name=metric_name,
                metric_expr=metric_expr,
                keys=[],
                partition_cols=partition_cols,
                where=where,
                data_dt=data_dt,
                config=config,
            )
            all_sql_statements.append(sql)

        # Process PK duplicate check
        if keys:
            sql = build_metric_sql(
                table_name=table_name,
                metric_name="pk_dup_count",
                metric_expr="",
                keys=keys,
                partition_cols=partition_cols,
                where=where,
                data_dt=data_dt,
                config=config,
            )
            all_sql_statements.append(sql)

    # Write all statements to a single file
    sql_file = os.path.join(output_dir, "all.sql")
    with open(sql_file, "w", encoding="utf-8") as f:
        f.write(f"-- Data Date: {data_dt}\n")
        f.write(f"-- Total statements: {len(all_sql_statements)}\n")
        f.write("\n")

        for i, sql in enumerate(all_sql_statements):
            f.write(sql)
            f.write(";\n")
            if i < len(all_sql_statements) - 1:
                f.write("\n")

    print(f"Generated SQL file: {sql_file}")
    print(f"Total statements: {len(all_sql_statements)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate SQL from Hive metadata")
    parser.add_argument(
        "--table-list",
        help="Input file containing list of database.table names (one per line)"
    )
    parser.add_argument(
        "--data-dt",
        help="Data date (format: YYYY-MM-DD)",
        required=True
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


def get_columns_for_tables(config: Config, table_list: List[Tuple[str, str]]) -> dict:
    """
    从 Hive Metastore MySQL 中获取指定表的列信息。
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

    # Build query for specific tables - each (db, table) pair needs its own placeholder tuple
    num_tables = len(table_list)
    placeholders = ','.join(['(%s,%s)'] * num_tables)
    sql = f"""SELECT
      d.NAME,
      t.TBL_NAME,
      c.COLUMN_NAME,
      c.TYPE_NAME
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
    WHERE (d.NAME, t.TBL_NAME) IN ({placeholders})
    ORDER BY d.NAME, t.TBL_NAME, c.INTEGER_IDX
    """

    # Flatten table_list for query params
    params = []
    for db_name, tbl_name in table_list:
        params.extend([db_name, tbl_name])

    columns_by_table = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
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


def load_table_list(path: str) -> List[Tuple[str, str]]:
    """
    从表名列表文件加载表名，格式：database.table (每行一个)
    返回: [(database, table), ...]
    """
    tables = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '.' in line:
                db, tbl = line.split('.', 1)
                tables.append((db.strip(), tbl.strip()))
    return tables


def load_table_list_from_json(path: str) -> List[Tuple[str, str]]:
    """
    从JSON配置文件加载表名列表
    返回: [(database, table), ...]
    """
    try:
        with open(path, 'r', encoding="utf-8") as f:
            data = json.load(f)

        tables = []
        if "tables" in data:
            for table_info in data["tables"]:
                table_name = table_info["name"]
                if '.' in table_name:
                    db, tbl = table_name.split('.', 1)
                    tables.append((db.strip(), tbl.strip()))
        return tables
    except Exception as e:
        print(f"WARNING: 从JSON文件加载表列表失败: {e}", file=sys.stderr)
        return []


def get_fields_from_json(path: str, table_name: str) -> List[str]:
    """
    从JSON配置文件获取特定表的字段列表
    返回: [field1, field2, ...] 或 [] 如果未找到
    """
    try:
        with open(path, 'r', encoding="utf-8") as f:
            data = json.load(f)

        if "tables" in data:
            for table_info in data["tables"]:
                if table_info["name"] == table_name and "fields" in table_info:
                    return table_info["fields"]
        return []
    except Exception as e:
        print(f"WARNING: 从JSON文件获取字段信息失败: {e}", file=sys.stderr)
        return []


def main() -> None:
    # 解析命令行参数
    args = parse_args()

    # 加载配置文件
    config = Config(args.config)

    # 从 Metastore MySQL 获取列信息
    columns_by_table = {}
    databases = []

    try:
        # 如果指定了 --table-list，从列表中只获取指定的表
        if args.table_list:
            # 检查是否为JSON文件
            if args.table_list.endswith('.json'):
                target_table_list = load_table_list_from_json(args.table_list)
                print(f"Loading tables from JSON file: {args.table_list}, count={len(target_table_list)}")
            else:
                target_table_list = load_table_list(args.table_list)
                print(f"Loading tables from list file: {args.table_list}, count={len(target_table_list)}")

            columns_by_table = get_columns_for_tables(config, target_table_list)
            databases = list(set(db for db, _ in target_table_list))
            print(
                f"Loaded metadata for {len(columns_by_table)} tables from Metastore MySQL"
            )
        else:
            # 获取所有库的数据库列表
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

            # 获取所有表的列信息
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

    if args.table_list:
        print(f"Building rules for {len(target_tables)} tables from table list")
    else:
        print(f"Building rules for {len(target_tables)} tables across {len(databases)} databases")

    for db, tbl in target_tables:
        cols = columns_by_table.get((db, tbl))
        if not cols:
            print(f"Skipping table without metadata: {db}.{tbl}", file=sys.stderr)
            continue

        # 检查是否有JSON配置文件并获取字段信息
        json_fields = []
        if args.table_list and args.table_list.endswith('.json'):
            full_table_name = f"{db}.{tbl}"
            json_fields = get_fields_from_json(args.table_list, full_table_name)

        # 如果JSON中有字段定义，则使用JSON中的字段；否则使用MySQL元数据
        if json_fields:
            # 创建仅包含JSON中指定字段的列信息
            filtered_cols = [col for col in cols if col.name in json_fields]
            print(f"Using {len(filtered_cols)} fields from JSON config for {full_table_name}")
        else:
            # 使用所有从MySQL获取的字段
            filtered_cols = cols
            print(f"Using all {len(filtered_cols)} fields from MySQL metadata for {db}.{tbl}")

        # 构建表规则
        rule = build_table_rule(
            config=config,
            database=db,
            table=tbl,
            columns=filtered_cols
        )
        table_rules.append(rule)

    # 生成 SQL 文件
    print(f"\nGenerating SQL files...")
    generate_sql_files(
        table_rules=table_rules,
        data_dt=args.data_dt,
        output_dir=args.output_dir,
        default_where="1=1",
        config=config,
    )

if __name__ == "__main__":
    main()
