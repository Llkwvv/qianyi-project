#!/usr/bin/env python3
"""
Hive SQL 模板生成脚本
读取库表配置 JSON，生成 SQL 模板（仅 {{data_dt}} 占位符）
"""

import argparse
import json
import os
from pathlib import Path


def load_config(config_path: str) -> dict:
    """加载库表配置文件"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


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

    # 添加 computed_at 时间戳列
    select_cols.append("current_timestamp() as computed_at")

    # row_count 指标
    select_cols.append("count(1) as row_count")

    # decimal 字段的 sum 聚合
    for field in fields:
        field_name = field['name']
        field_type = field['type']
        if 'decimal' in field_type.lower():
            col_alias = f"{field_name}_sum"
            select_cols.append(f"sum(cast({field_name} as decimal(38,2))) as {col_alias}")

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
    parser = argparse.ArgumentParser(
        description='生成 Hive SQL 模板（仅 {{data_dt}} 占位符）'
    )
    parser.add_argument(
        '--table-list',
        required=True,
        help='库表配置文件路径 (JSON)'
    )
    parser.add_argument(
        '--output-dir',
        required=True,
        help='输出目录'
    )

    args = parser.parse_args()

    # 加载配置
    config = load_config(args.table_list)

    # 生成 SQL 模板
    sql_template = generate_sql_template(config)

    # 确保输出目录存在
    os.makedirs(args.output_dir, exist_ok=True)

    # 写入文件
    output_file = os.path.join(args.output_dir, 'metrics_queries.sql')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(sql_template)

    print(f"SQL 模板已生成: {output_file}")


if __name__ == '__main__':
    main()
