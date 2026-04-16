#!/usr/bin/env python3
"""
从 Hive 元数据库(MySQL)获取表信息并导出
"""

import argparse
import csv
import json
import os
import datetime


def load_env_config(config_path: str = 'env_config.json') -> dict:
    """加载环境配置文件"""
    config_file = os.path.join(os.path.dirname(__file__), config_path)
    if not os.path.exists(config_file):
        return {}
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_table_stats(mysql_config: dict, data_dt: str):
    """从 MySQL 获取 Hive 表统计信息
    1. 非分区表：直接获取表级别统计信息
    2. 分区表：获取特定分区（如 dt=20250324）的统计数据
    """
    try:
        import pymysql
    except ImportError:
        print("请安装 pymysql: pip install pymysql")
        return []

    conn = pymysql.connect(
        host=mysql_config['host'],
        port=mysql_config.get('port'),
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config.get('database') or mysql_config.get('db'),
        charset='utf8mb4'
    )

    cursor = conn.cursor()

    # 将 20250324 转换为日期格式 2025-03-24
    if len(data_dt) == 8:
        date_pattern = f"{data_dt[:4]}-{data_dt[4:6]}-{data_dt[6:8]}"
    else:
        date_pattern = data_dt

    # 需求：
    # 1. 非分区表数据
    # 2. 分区表且特定分区（如 dt=20250324）有数据
    # 3. 不要分区表但特定分区为空的数据

    # 使用 UNION ALL 合并两类数据：
    # 1. 非分区表（没有分区键的表）
    # 2. 分区表中有特定分区的数据
    sql = f"""
    -- 1. 非分区表：没有分区键的表
    SELECT
        d.NAME as database_name,
        t.TBL_NAME as table_name,
        t.TBL_TYPE as table_type,
        '' as partition_name,
        tp_num_rows.PARAM_VALUE as num_rows,
        tp_total_size.PARAM_VALUE as total_size,
        tp_num_files.PARAM_VALUE as num_files,
        tp_raw_size.PARAM_VALUE as raw_data_size,
        t.LAST_ACCESS_TIME as last_access_time,
        '{data_dt}' as data_dt
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    LEFT JOIN PARTITION_KEYS pk ON t.TBL_ID = pk.TBL_ID
    LEFT JOIN TABLE_PARAMS tp_num_rows ON t.TBL_ID = tp_num_rows.TBL_ID AND tp_num_rows.PARAM_KEY = 'numRows'
    LEFT JOIN TABLE_PARAMS tp_total_size ON t.TBL_ID = tp_total_size.TBL_ID AND tp_total_size.PARAM_KEY = 'totalSize'
    LEFT JOIN TABLE_PARAMS tp_num_files ON t.TBL_ID = tp_num_files.TBL_ID AND tp_num_files.PARAM_KEY = 'numFiles'
    LEFT JOIN TABLE_PARAMS tp_raw_size ON t.TBL_ID = tp_raw_size.TBL_ID AND tp_raw_size.PARAM_KEY = 'rawDataSize'
    WHERE pk.TBL_ID IS NULL

    UNION ALL

    -- 2. 分区表中有特定分区的数据（日期可能在任意一级分区）
    SELECT
        d.NAME as database_name,
        t.TBL_NAME as table_name,
        t.TBL_TYPE as table_type,
        p.PART_NAME as partition_name,
        pp_num_rows.PARAM_VALUE as num_rows,
        pp_total_size.PARAM_VALUE as total_size,
        pp_num_files.PARAM_VALUE as num_files,
        pp_raw_size.PARAM_VALUE as raw_data_size,
        p.LAST_ACCESS_TIME as last_access_time,
        '{data_dt}' as data_dt
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN PARTITIONS p ON t.TBL_ID = p.TBL_ID
    LEFT JOIN PARTITION_PARAMS pp_num_rows ON p.PART_ID = pp_num_rows.PART_ID AND pp_num_rows.PARAM_KEY = 'numRows'
    LEFT JOIN PARTITION_PARAMS pp_total_size ON p.PART_ID = pp_total_size.PART_ID AND pp_total_size.PARAM_KEY = 'totalSize'
    LEFT JOIN PARTITION_PARAMS pp_num_files ON p.PART_ID = pp_num_files.PART_ID AND pp_num_files.PARAM_KEY = 'numFiles'
    LEFT JOIN PARTITION_PARAMS pp_raw_size ON p.PART_ID = pp_raw_size.PART_ID AND pp_raw_size.PARAM_KEY = 'rawDataSize'
    WHERE p.PART_NAME LIKE CONCAT('%=', '{date_pattern}', '%')
    """

    cursor.execute(sql)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results


def main():
    parser = argparse.ArgumentParser(
        description='从 Hive 元数据库(MySQL)获取表信息'
    )
    parser.add_argument(
        '--data-dt',
        required=True,
        help='分区日期，如 20250324'
    )
    parser.add_argument(
        '--cluster',
        required=True,
        help='集群名称，用于输出文件名，如 old 或 new'
    )
    parser.add_argument(
        '--output-csv',
        help='输出 CSV 文件路径'
    )
    parser.add_argument(
        '--mysql-host',
        help='MySQL 主机地址'
    )
    parser.add_argument(
        '--mysql-port',
        type=int,
        help='MySQL 端口'
    )
    parser.add_argument(
        '--mysql-user',
        help='MySQL 用户名'
    )
    parser.add_argument(
        '--mysql-password',
        help='MySQL 密码'
    )
    parser.add_argument(
        '--mysql-database',
        help='元数据库名称'
    )

    args = parser.parse_args()

    # 加载配置
    config = load_env_config()
    mysql_config = config.get('metastore_mysql', {}).copy()

    # 输出路径
    if not args.output_csv:
        csv_dir = config.get('csv_dir', 'output')
        args.output_csv = os.path.join(csv_dir, f'{datetime.date.today().strftime("%Y%m%d")}/{args.data_dt}_{args.cluster}_table_stats.csv')

    # 命令行参数覆盖配置
    if args.mysql_host:
        mysql_config['host'] = args.mysql_host
    if args.mysql_port:
        mysql_config['port'] = args.mysql_port
    if args.mysql_user:
        mysql_config['user'] = args.mysql_user
    if args.mysql_password:
        mysql_config['password'] = args.mysql_password
    if args.mysql_database:
        mysql_config['database'] = args.mysql_database
    # 支持配置中的 db 字段
    if not mysql_config.get('database') and mysql_config.get('db'):
        mysql_config['database'] = mysql_config['db']

    if not mysql_config.get('host') or not mysql_config.get('user') or not mysql_config.get('port'):
        print("错误: 请通过配置文件或命令行提供完整的 MySQL 连接信息")
        print("配置示例:")
        print('  {"metastore_mysql": {"host": "localhost", "port": 3306, "user": "root", "password": "xxx", "db": "metastore"}}')
        return

    print(f"连接 MySQL: {mysql_config['user']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config.get('database') or mysql_config.get('db')}")

    # 获取数据
    results = get_table_stats(mysql_config, args.data_dt)

    if not results:
        print("未获取到数据")
        return

    print(f"共获取 {len(results)} 条记录")

    # 导出 CSV
    output_dir = os.path.dirname(args.output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    headers = ['database_name', 'table_name', 'table_type', 'partition_name',
               'num_files', 'total_size', 'raw_data_size', 'num_rows', 'last_access_time', 'data_dt']

    with open(args.output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write header row first
        writer.writerow(['database_name', 'table_name', 'table_type', 'partition_name',
                        'num_files', 'total_size', 'raw_data_size', 'num_rows', 'last_access_time', 'data_dt'])
        # Then write the data rows
        writer.writerows(results)

    print(f"\n结果已导出: {args.output_csv}")


if __name__ == '__main__':
    main()
