#!/usr/bin/env python3
"""
Hive SQL 模板生成脚本
读取库表配置 JSON，生成 SQL 模板（仅 {{data_dt}} 占位符）
"""

import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Tuple

try:
    import pymysql
except ImportError:
    pymysql = None


def load_config(config_path: str) -> dict:
    """加载库表配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_metastore_config(config_path: str) -> dict:
    """加载元数据 MySQL 配置"""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    return config['metastore_mysql']


def load_env_config(config_path: str) -> dict:
    """加载环境配置"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_table_list_from_file(file_path: str) -> List[str]:
    """从文本文件加载表名列表（每行一个表名）"""
    table_names = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            table_name = line.strip()
            if table_name and not table_name.startswith('#'):
                table_names.append(table_name)
    return table_names


def get_table_columns(metastore_conf: dict, table_list: List[Tuple[str, str]]) -> Dict:
    """从 Hive 元数据获取列信息"""
    if not table_list:
        return {}

    conn = pymysql.connect(
        host=metastore_conf['host'],
        port=metastore_conf['port'],
        user=metastore_conf['user'],
        password=metastore_conf['password'],
        database=metastore_conf['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
    )

    placeholders = ','.join(['(%s,%s)'] * len(table_list))
    sql = (
        "SELECT d.NAME, t.TBL_NAME, c.COLUMN_NAME, c.TYPE_NAME "
        "FROM TBLS t "
        "JOIN DBS d ON t.DB_ID = d.DB_ID "
        "JOIN SDS s ON t.SD_ID = s.SD_ID "
        "JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID "
        f"WHERE (d.NAME, t.TBL_NAME) IN ({placeholders}) "
        "ORDER BY d.NAME, t.TBL_NAME, c.INTEGER_IDX"
    )

    params = []
    for db_name, table_name in table_list:
        params.extend([db_name, table_name])

    columns_by_table = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for db_name, tbl_name, col_name, type_name in cur.fetchall():
                key = ((db_name or '').strip(), (tbl_name or '').strip())
                if not key[0] or not key[1] or not col_name:
                    continue
                columns_by_table.setdefault(key, []).append({
                    'name': col_name.strip(),
                    'type': (type_name or '').strip().lower()
                })
    finally:
        conn.close()

    return columns_by_table


def get_partition_columns(metastore_conf: dict, database: str, table: str) -> List[str]:
    """从 Hive 元数据获取分区列"""
    conn = pymysql.connect(
        host=metastore_conf['host'],
        port=metastore_conf['port'],
        user=metastore_conf['user'],
        password=metastore_conf['password'],
        database=metastore_conf['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT p.PKEY_NAME "
                "FROM PARTITION_KEYS p "
                "JOIN TBLS t ON t.TBL_ID = p.TBL_ID "
                "JOIN DBS d ON t.DB_ID = d.DB_ID "
                "WHERE d.NAME = %s AND t.TBL_NAME = %s "
                "ORDER BY p.INTEGER_IDX",
                (database, table)
            )
            return [row[0] for row in cur.fetchall() if row[0]]
    finally:
        conn.close()


def get_tables_from_metastore(
    metastore_conf: dict,
    metadata_limit: Optional[int] = None,
    database: Optional[str] = None,
    table_names: Optional[List[str]] = None
) -> List[Tuple[str, str]]:
    """从 Hive 元数据获取库表列表"""
    conn = pymysql.connect(
        host=metastore_conf['host'],
        port=metastore_conf['port'],
        user=metastore_conf['user'],
        password=metastore_conf['password'],
        database=metastore_conf['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
    )
    try:
        with conn.cursor() as cur:
            sql = (
                "SELECT d.NAME, t.TBL_NAME "
                "FROM TBLS t JOIN DBS d ON t.DB_ID = d.DB_ID "
                "WHERE 1=1 "
            )
            params = []
            if database:
                sql += "AND d.NAME = %s "
                params.append(database)
            if table_names:
                placeholders = ','.join(['%s'] * len(table_names))
                sql += f"AND t.TBL_NAME IN ({placeholders}) "
                params.extend(table_names)
            sql += "ORDER BY d.NAME, t.TBL_NAME "
            if metadata_limit is not None:
                sql += " LIMIT %s"
                params.append(metadata_limit)

            cur.execute(sql, params)
            return [(row[0], row[1]) for row in cur.fetchall() if row[0] and row[1]]
    finally:
        conn.close()


def enrich_config_from_metastore(
    config: dict,
    metastore_conf: dict,
    metadata_limit: Optional[int] = None
) -> dict:
    """用元数据补全表字段和分区信息（仅补空，不覆盖已有配置）"""
    tables = config.get('tables', [])
    table_list = []

    for table in tables:
        name = table.get('name', '')
        if '.' not in name:
            continue
        db_name, tbl_name = name.split('.', 1)
        table_list.append((db_name.strip(), tbl_name.strip()))

    if metadata_limit is not None:
        table_list = table_list[:metadata_limit]

    limited_table_set = set(table_list)
    columns_by_table = get_table_columns(metastore_conf, table_list)

    for table in tables:
        name = table.get('name', '')
        if '.' not in name:
            continue
        db_name, tbl_name = name.split('.', 1)
        key = (db_name.strip(), tbl_name.strip())
        if key not in limited_table_set:
            continue

        if not table.get('fields'):
            table['fields'] = columns_by_table.get(key, [])

        if not table.get('partition_cols'):
            table['partition_cols'] = get_partition_columns(
                metastore_conf, key[0], key[1]
            )

    return config


def build_config_from_metastore(
    metastore_conf: dict,
    metadata_limit: Optional[int] = None,
    database: Optional[str] = None,
    table_names: Optional[List[str]] = None
) -> dict:
    """仅从元数据构建 tables 配置"""
    table_list = get_tables_from_metastore(metastore_conf, metadata_limit, database, table_names)
    columns_by_table = get_table_columns(metastore_conf, table_list)

    tables = []
    for db_name, tbl_name in table_list:
        key = (db_name, tbl_name)
        table_info = {
            'name': '{}.{}'.format(db_name, tbl_name),
            'fields': columns_by_table.get(key, []),
            'partition_cols': get_partition_columns(metastore_conf, db_name, tbl_name)
        }
        tables.append(table_info)

    return {'tables': tables}


def generate_sql_for_table(table: dict) -> str:
    """
    为单张表生成 SQL 语句

    - 分区字段统一别名为 partition_col
    - 占位符仅 {{data_dt}}
    - WHERE 条件根据 partition_cols 是否为空决定
    """
    table_name = table['name']
    partition_cols = table.get('partition_cols', [])
    fields = table.get('fields', [])

    # 表名
    select_cols = [f"'{table_name}' as table_name"]

    # 基础列：data_dt
    select_cols.append("'{{data_dt}}' as data_dt")

    # 分区字段：直接输出分区字段名作为字符串
    if partition_cols:
        partition_col = partition_cols[0]
        select_cols.append(f"'{partition_col}' as partition_col")
    else:
        # 无分区时输出空字符串别名
        select_cols.append("'' as partition_col")

    # 添加 etl_tm 时间戳列
    select_cols.append("current_timestamp() as etl_tm")

    # row_counts 指标
    select_cols.append("count(1) as row_counts")

    # decimal 字段的 sum 聚合
    for field in fields:
        field_name = field['name']
        field_type = field.get('type', 'decimal(38,2)')
        if 'decimal' in field_type.lower():
            col_alias = f"{field_name}_sum"
            select_cols.append(f"sum(cast({field_name} as {field_type})) as {col_alias}")

    # 构造 SELECT 语句
    select_clause = ",\n    ".join(select_cols)

    # 构造 WHERE 条件
    if partition_cols:
        where_clause = f"WHERE {partition_cols[0]} = '{{{{data_dt}}}}'"
    else:
        where_clause = ""

    # 组合完整 SQL
    sql = f"""SELECT
    {select_clause}
FROM {table_name}
{where_clause}"""
    return sql


def generate_sql_template(config: dict) -> str:
    """生成完整的 SQL 模板（多条语句以分号分隔）"""
    tables = config.get('tables', [])
    sql_statements = []

    for table in tables:
        sql = generate_sql_for_table(table)
        sql_statements.append(sql)

    # 以分号分隔各语句
    return ";\n\n".join(sql_statements) + ";"


def main():
    default_config_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'env_config.json'
    )
    default_table_list_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'sample_tables.json'
    )

    parser = argparse.ArgumentParser(
        description='生成 Hive SQL 模板（仅 {{data_dt}} 占位符）'
    )
    parser.add_argument(
        '--table-json',
        default=default_table_list_path,
        help='库表配置文件路径 (JSON，file 模式默认 sample_tables.json)'
    )
    parser.add_argument(
        '--source',
        choices=['file', 'metadata'],
        default='file',
        help='配置来源：file(本地 JSON) 或 metadata(元数据 MySQL)'
    )
    parser.add_argument(
        '--output-dir',
        help='输出目录（不传则从配置 file_dir 读取）'
    )
    parser.add_argument(
        '--config',
        default=default_config_path,
        help='环境配置文件路径 (JSON，包含 metastore_mysql)'
    )
    parser.add_argument(
        '--metadata-limit',
        type=int,
        default=0,
        help='从元数据补全的表数量上限，0 表示不限制'
    )
    parser.add_argument(
        '--database',
        help='元数据模式下限定数据库名（如 gmall）'
    )
    parser.add_argument(
        '--table-list-file',
        help='元数据模式下从文件读取表名列表（每行一个表名，配合 --database 使用）'
    )

    args = parser.parse_args()

    if args.config and (not os.path.exists(args.config)):
        print(f'ERROR: 配置文件不存在: {args.config}')
        sys.exit(1)

    if args.source == 'metadata' and pymysql is None:
        print('ERROR: metadata 模式需要先安装 PyMySQL: pip install pymysql')
        sys.exit(1)

    if args.metadata_limit < 0:
        print('ERROR: --metadata-limit 不能为负数')
        sys.exit(1)

    if args.table_list_file and args.source != 'metadata':
        print('ERROR: --table-list-file 仅在 --source metadata 模式下可用')
        sys.exit(1)

    if args.table_list_file and not args.database:
        print('ERROR: --table-list-file 需要配合 --database 使用')
        sys.exit(1)

    env_config = load_env_config(args.config)

    # 加载配置（互斥来源：file 或 metadata）
    if args.source == 'file':
        if not os.path.exists(args.table_list):
            print(f'ERROR: table_list 文件不存在: {args.table_list}')
            sys.exit(1)
        config = load_config(args.table_list)
    else:
        metastore_conf = load_metastore_config(args.config)
        limit = args.metadata_limit if args.metadata_limit > 0 else None
        table_names = None
        if args.table_list_file:
            if not os.path.exists(args.table_list_file):
                print(f'ERROR: 表名列表文件不存在: {args.table_list_file}')
                sys.exit(1)
            table_names = load_table_list_from_file(args.table_list_file)
            if not table_names:
                print(f'ERROR: 表名列表文件为空: {args.table_list_file}')
                sys.exit(1)
        config = build_config_from_metastore(metastore_conf, limit, args.database, table_names)

    # 生成 SQL 模板
    sql_template = generate_sql_template(config)

    # 输出目录优先使用参数，否则取 env_config.file_dir
    output_dir = args.output_dir or env_config.get('file_dir')
    if not output_dir:
        print('ERROR: 未指定输出目录，且配置中缺少 file_dir')
        sys.exit(1)

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 写入文件
    output_file = os.path.join(output_dir, 'metrics_queries.sql')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(sql_template)

    print(f"SQL 模板已生成: {output_file}")


if __name__ == '__main__':
    main()
