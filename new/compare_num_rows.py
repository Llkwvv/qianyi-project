#!/usr/bin/env python3
"""
简化版：仅比较两个集群的表行数差异
"""

import argparse
import csv
import json
import os
import sys
import pymysql
from typing import Dict, List
import datetime


def load_env_config(config_path: str = 'env_config.json') -> dict:
    """加载环境配置文件"""
    config_file = os.path.join(os.path.dirname(__file__), config_path)
    if not os.path.exists(config_file):
        return {}
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def read_csv_data(file_path: str) -> List[Dict]:
    """读取CSV数据"""
    if not os.path.exists(file_path):
        print(f"错误: 文件不存在 {file_path}")
        return []

    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data


def compare_num_rows(old_data: List[Dict], new_data: List[Dict]) -> List[Dict]:
    """仅比较行数差异"""
    # 构建以 (database_name, table_name) 为键的字典（忽略分区信息）
    # 这样可以比较有分区和无分区的表
    old_dict = {}
    new_dict = {}

    for row in old_data:
        # 使用 (database_name, table_name) 作为键，忽略分区
        key = (row['database_name'], row['table_name'])
        old_dict[key] = row

    for row in new_data:
        # 使用 (database_name, table_name) 作为键，忽略分区
        key = (row['database_name'], row['table_name'])
        new_dict[key] = row

    # 找出差异
    differences = []

    # 检查哪些表在旧集群存在但在新集群不存在
    for key, old_row in old_dict.items():
        if key not in new_dict:
            differences.append({
                'database_name': old_row['database_name'],
                'table_name': old_row['table_name'],
                'partition_name': old_row['partition_name'],
                'metric_name': 'num_rows',
                'old_value': old_row['num_rows'],
                'new_value': '',
                'data_dt': old_row.get('data_dt', '')
            })

    # 检查哪些表在新集群存在但在旧集群不存在
    for key, new_row in new_dict.items():
        if key not in old_dict:
            differences.append({
                'database_name': new_row['database_name'],
                'table_name': new_row['table_name'],
                'partition_name': new_row['partition_name'],
                'metric_name': 'num_rows',
                'old_value': '',
                'new_value': new_row['num_rows'],
                'data_dt': new_row.get('data_dt', '')
            })

    # 检查共同存在的表的行数差异
    for key in set(old_dict.keys()) & set(new_dict.keys()):
        old_row = old_dict[key]
        new_row = new_dict[key]

        # 检查行数是否有差异
        if old_row['num_rows'] != new_row['num_rows']:
            # 计算差异值
            def safe_int(value):
                try:
                    return int(value) if value else 0
                except:
                    return 0

            diff_num_rows = safe_int(new_row['num_rows']) - safe_int(old_row['num_rows'])

            differences.append({
                'database_name': old_row['database_name'],
                'table_name': old_row['table_name'],
                'partition_name': old_row['partition_name'],
                'metric_name': 'num_rows',
                'old_value': old_row['num_rows'],
                'new_value': new_row['num_rows'],
                'data_dt': old_row.get('data_dt', new_row.get('data_dt', ''))
            })

    return differences


def export_to_csv(differences: List[Dict], output_file: str):
    """导出差异到CSV文件"""
    if not differences:
        print("没有发现差异")
        return

    # 确保输出目录存在
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # 写入CSV文件
    fieldnames = ['database_name', 'table_name', 'partition_name',
                  'metric_name', 'old_value', 'new_value', 'data_dt']

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(differences)

    print(f"行数差异报告已导出到: {output_file}")


def get_mysql_config(config: dict, cluster_name: str) -> dict:
    """获取指定集群的MySQL配置"""
    # 优先使用 import_mysql 配置，如果没有则回退到 metastore_mysql
    mysql_config = config.get('import_mysql', config.get('metastore_mysql', {})).copy()
    return mysql_config


def create_comparison_table(mysql_config: dict):
    """在MySQL中创建对比结果表"""
    try:
        conn = pymysql.connect(
            host=mysql_config['host'],
            port=mysql_config['port'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config.get('database') or mysql_config.get('db'),
            charset='utf8mb4'
        )

        cursor = conn.cursor()

        # 创建对比结果表（仅包含行数相关字段，移除了status列，使用英文列名）
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS table_num_rows_comparison (
            id INT AUTO_INCREMENT PRIMARY KEY,
            database_name VARCHAR(255),
            table_name VARCHAR(255),
            partition_name VARCHAR(255),
            metric_name VARCHAR(50),
            old_value VARCHAR(100),
            new_value VARCHAR(100),
            data_dt VARCHAR(50),
            comparison_date DATE DEFAULT (CURRENT_DATE),
            INDEX idx_database_table (database_name, table_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()

        print("行数对比结果表创建成功")

    except Exception as e:
        print(f"创建对比表时出错: {e}")


def insert_comparison_results(mysql_config: dict, differences: List[Dict]):
    """将对比结果插入MySQL表"""
    try:
        conn = pymysql.connect(
            host=mysql_config['host'],
            port=mysql_config['port'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config.get('database') or mysql_config.get('db'),
            charset='utf8mb4'
        )

        cursor = conn.cursor()

        # 插入数据（移除了status字段，使用英文列名）
        insert_sql = """
        INSERT INTO table_num_rows_comparison (
            database_name, table_name, partition_name,
            metric_name, old_value, new_value, data_dt
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for diff in differences:
            # 处理空值情况
            cursor.execute(insert_sql, (
                diff['database_name'],
                diff['table_name'],
                diff['partition_name'],
                diff['status'],
                diff.get('指标名', '') or '',
                diff.get('指标old值', '') or '',
                diff.get('指标new值', '') or '',
                diff.get('data_dt', '') or ''
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"成功插入 {len(differences)} 条行数对比记录到数据库")

    except Exception as e:
        print(f"插入行数对比结果时出错: {e}")


def main():
    parser = argparse.ArgumentParser(description='仅比较两个集群的表行数差异')
    parser.add_argument('--old-csv', required=True, help='旧集群CSV文件路径')
    parser.add_argument('--new-csv', required=True, help='新集群CSV文件路径')
    parser.add_argument('--output-csv', help='输出对比报告CSV文件路径')
    parser.add_argument('--mysql-host', help='MySQL主机地址')
    parser.add_argument('--mysql-port', type=int, help='MySQL端口')
    parser.add_argument('--mysql-user', help='MySQL用户名')
    parser.add_argument('--mysql-password', help='MySQL密码')
    parser.add_argument('--mysql-database', help='元数据库名称')
    parser.add_argument('--no-db-insert', action='store_true', help='不插入数据库，只生成CSV报告')

    args = parser.parse_args()

    # 加载配置
    config = load_env_config()

    # 只有在需要插入数据库时才检查MySQL配置
    mysql_config = None
    if not args.no_db_insert:
        # 优先使用 insert_mysql 配置，如果没有则回退到 metastore_mysql
        mysql_config = config.get('insert_mysql', config.get('metastore_mysql', {})).copy()

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

        if not mysql_config.get('host') or not mysql_config.get('user') or not mysql_config.get('port'):
            print("错误: 请通过配置文件或命令行提供完整的 MySQL 连接信息")
            print("配置示例:")
            print('  {"insert_mysql": {"host": "localhost", "port": 3306, "user": "root", "password": "xxx", "db": "validation_db"}}')
            return

    import time
    import os

    # 检查 old-csv 文件是否存在，如果不存在则等待5分钟重试，最多48次（240分钟）
    max_retries = 48
    retry_interval = 300  # 5 minutes in seconds

    for attempt in range(max_retries):
        if os.path.exists(args.old_csv):
            break
        else:
            if attempt < max_retries - 1:
                print(f"等待 {retry_interval//60} 分钟，第 {attempt+1}/{max_retries} 次尝试...")
                time.sleep(retry_interval)
            else:
                print(f"错误: {args.old_csv} 文件在 {max_retries*retry_interval//60} 分钟内仍未出现")
                return

    # 读取数据
    print("正在读取旧集群数据...")
    old_data = read_csv_data(args.old_csv)

    print("正在读取新集群数据...")
    new_data = read_csv_data(args.new_csv)

    if not old_data:
        print("错误: 无法读取旧集群数据")
        return

    if not new_data:
        print("错误: 无法读取新集群数据")
        return

    print(f"旧集群数据量: {len(old_data)}")
    print(f"新集群数据量: {len(new_data)}")

    # 比较行数
    print("正在比较行数...")
    differences = compare_num_rows(old_data, new_data)

    print(f"发现 {len(differences)} 条行数差异")

    # 输出报告
    if args.output_csv:
        export_to_csv(differences, args.output_csv)
    else:
        # 默认输出路径
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        default_output = f"output/num_rows_comparison_{timestamp}.csv"
        export_to_csv(differences, default_output)

    # 插入数据库
    if not args.no_db_insert and mysql_config:
        print("正在创建行数对比表...")
        create_comparison_table(mysql_config)

        print("正在插入行数对比结果到数据库...")
        insert_comparison_results(mysql_config, differences)

    print("行数对比完成!")


if __name__ == '__main__':
    main()