import argparse
import csv
import json
import os
import sys
import time
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
    """将两个集群的数据拼接，计算差值，返回所有数据（不区分是否有差异）"""
    # 构建以 (database_name, table_name) 为键的字典
    old_dict = {}
    new_dict = {}

    for row in old_data:
        key = (row['database_name'], row['table_name'])
        old_dict[key] = row

    for row in new_data:
        key = (row['database_name'], row['table_name'])
        new_dict[key] = row

    results = []
    all_keys = set(old_dict.keys()) | set(new_dict.keys())

    for key in all_keys:
        old_row = old_dict.get(key)
        new_row = new_dict.get(key)

        # 获取两边的值（表不存在时用 None 表示，否则转换为 int）
        def to_int_or_none(value):
            if value is None or value == '':
                return None
            try:
                return int(value)
            except:
                return None

        old_num_rows = to_int_or_none(old_row['num_rows']) if old_row else None
        new_num_rows = to_int_or_none(new_row['num_rows']) if new_row else None

        # 计算差值（None 视为 0）
        def safe_int(value):
            try:
                return int(value) if value else 0
            except:
                return 0

        diff_value = safe_int(new_num_rows) - safe_int(old_num_rows)

        # 获取分区名（优先用旧的）
        partition_name = None
        if old_row:
            partition_name = old_row.get('partition_name') or None
        elif new_row:
            partition_name = new_row.get('partition_name') or None

        # 获取 data_dt
        data_dt = None
        if old_row:
            data_dt = old_row.get('data_dt') or None
        elif new_row:
            data_dt = new_row.get('data_dt') or None

        results.append({
            'database_name': key[0],
            'table_name': key[1],
            'partition_name': partition_name,
            'metric_name': 'num_rows',
            'old_value': old_num_rows,
            'new_value': new_num_rows,
            'diff_value': diff_value,
            'data_dt': data_dt
        })

    return results

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
                  'metric_name', 'old_value', 'new_value', 'diff_value', 'data_dt']

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(differences)

    print(f"行数差异报告已导出到: {output_file}")

def get_mysql_config(config: dict, cluster_name: str) -> dict:
    """获取指定集群的MySQL配置"""
    # 严格使用 metastore_mysql 配置
    if 'metastore_mysql' not in config:
        raise ValueError("配置文件缺少 metastore_mysql 配置")
    return config['metastore_mysql'].copy()

def insert_comparison_results(mysql_config: dict, differences: List[Dict], data_dt: str):
    """将对比结果插入MySQL表（先删除相同data_dt的数据）"""
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

        # 先删除相同 data_dt 的数据
        delete_sql = "DELETE FROM table_comparison WHERE data_dt = %s"
        cursor.execute(delete_sql, (data_dt,))
        deleted_count = cursor.rowcount
        print(f"已删除 {deleted_count} 条相同 data_dt 的旧数据")

        # 插入数据（使用英文列名）
        insert_sql = """
        INSERT INTO table_comparison (
            database_name, table_name, partition_name,
            metric_name, old_value, new_value, diff_value, data_dt
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for diff in differences:
            # None 会被 pymysql 正确转换为 MySQL 的 NULL
            cursor.execute(insert_sql, (
                diff['database_name'],
                diff['table_name'],
                diff['partition_name'],
                diff['metric_name'],
                diff['old_value'],
                diff['new_value'],
                diff.get('diff_value', 0),
                diff.get('data_dt')
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"成功插入 {len(differences)} 条行数对比记录到数据库")

    except Exception as e:
        print(f"插入行数对比结果时出错: {e}")

def main():
    parser = argparse.ArgumentParser(description='仅比较两个集群的表行数差异')
    parser.add_argument('--data-dt', required=True, help='分区日期，如 20250324')
    parser.add_argument('--old-csv', help='旧集群CSV文件路径（可选，默认从csv_dir读取）')
    parser.add_argument('--new-csv', help='新集群CSV文件路径（可选，默认从csv_dir读取）')
    parser.add_argument('--mysql-host', help='MySQL主机地址')
    parser.add_argument('--mysql-port', type=int, help='MySQL端口')
    parser.add_argument('--mysql-user', help='MySQL用户名')
    parser.add_argument('--mysql-password', help='MySQL密码')
    parser.add_argument('--mysql-database', help='元数据库名称')

    args = parser.parse_args()

    # 加载配置
    config = load_env_config()
    csv_dir = config.get('csv_dir', 'output')

    # 自动拼接文件路径
    today = datetime.date.today().strftime('%Y%m%d')
    if not args.old_csv:
        args.old_csv = os.path.join(csv_dir, f'{today}/{args.data_dt}_old_table_stats.csv')
    if not args.new_csv:
        args.new_csv = os.path.join(csv_dir, f'{today}/{args.data_dt}_new_table_stats.csv')

    print(f"旧集群CSV: {args.old_csv}")
    print(f"新集群CSV: {args.new_csv}")

    # 严格使用 insert_mysql 配置（不进行回退）
    if 'insert_mysql' not in config:
        print("错误: 配置文件缺少 insert_mysql 配置，无法导出数据")
        print("配置示例:")
        print('  {"insert_mysql": {"host": "localhost", "port": 3306, "user": "root", "password": "xxx", "db": "validation_db"}}')
        return
    mysql_config = config['insert_mysql'].copy()

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

    print(f"共 {len(differences)} 条数据")

    # 输出前10条数据到控制台
    print(f"\n前10条数据:")
    for diff in differences[:10]:
        old_val = diff['old_value'] or 'N/A'
        new_val = diff['new_value'] or 'N/A'
        diff_val = diff.get('diff_value', 0)
        print(f"{diff['database_name']}.{diff['table_name']} - 行数: {old_val} → {new_val} (差值: {diff_val})")

    # 将差异结果写入数据库（使用英文列名）
    print("正在将结果写入数据库...")
    insert_comparison_results(mysql_config, differences, args.data_dt)

    print("行数对比完成!")

if __name__ == '__main__':
    main()