import argparse
import csv
import json
import os
import sys
import time
import subprocess
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

    results = []

    # 创建查找表，使用(database_name, table_name, partition_name)作为键
    old_lookup = {}
    new_lookup = {}

    # 为旧数据建立查找表
    for row in old_data:
        key = (row['database_name'], row['table_name'], row.get('partition_name') or '')
        if key not in old_lookup:
            old_lookup[key] = []
        old_lookup[key].append(row)

    # 为新数据建立查找表
    for row in new_data:
        key = (row['database_name'], row['table_name'], row.get('partition_name') or '')
        if key not in new_lookup:
            new_lookup[key] = []
        new_lookup[key].append(row)

    # 处理所有唯一的(database_name, table_name, partition_name)组合
    all_keys = set(old_lookup.keys()) | set(new_lookup.keys())

    for key in all_keys:
        old_rows = old_lookup.get(key, [])
        new_rows = new_lookup.get(key, [])

        # 如果两边都有数据，则进行交叉对比
        if old_rows and new_rows:
            # 为每对记录创建对比结果
            for old_row in old_rows:
                for new_row in new_rows:
                    # 获取数值
                    def to_int_or_none(value):
                        if value is None or value == '':
                            return None
                        try:
                            return int(value)
                        except:
                            return None

                    old_num_rows = to_int_or_none(old_row['num_rows'])
                    new_num_rows = to_int_or_none(new_row['num_rows'])

                    # 计算差值
                    def safe_int(value):
                        try:
                            return int(value) if value else 0
                        except:
                            return 0

                    diff_value = safe_int(new_num_rows) - safe_int(old_num_rows)

                    # 获取分区名（从key中获取）
                    partition_name = key[2] if key[2] else None

                    # 获取 data_dt（优先用旧的）
                    data_dt = old_row.get('data_dt') or new_row.get('data_dt') or None

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
        elif old_rows:
            # 只存在于旧集群中的记录
            for old_row in old_rows:
                def to_int_or_none(value):
                    if value is None or value == '':
                        return None
                    try:
                        return int(value)
                    except:
                        return None

                old_num_rows = to_int_or_none(old_row['num_rows'])

                # 获取分区名（从key中获取）
                partition_name = key[2] if key[2] else None

                results.append({
                    'database_name': key[0],
                    'table_name': key[1],
                    'partition_name': partition_name,
                    'metric_name': 'num_rows',
                    'old_value': old_num_rows,
                    'new_value': None,
                    'diff_value': 0 - (old_num_rows or 0),
                    'data_dt': old_row.get('data_dt') or None
                })
        else:
            # 只存在于新集群中的记录
            for new_row in new_rows:
                def to_int_or_none(value):
                    if value is None or value == '':
                        return None
                    try:
                        return int(value)
                    except:
                        return None

                new_num_rows = to_int_or_none(new_row['num_rows'])

                # 获取分区名（从key中获取）
                partition_name = key[2] if key[2] else None

                results.append({
                    'database_name': key[0],
                    'table_name': key[1],
                    'partition_name': partition_name,
                    'metric_name': 'num_rows',
                    'old_value': None,
                    'new_value': new_num_rows,
                    'diff_value': (new_num_rows or 0) - 0,
                    'data_dt': new_row.get('data_dt') or None
                })

    return results

def export_to_hive_csv(differences: List[Dict], output_file: str):
    """导出对比结果到CSV文件（Hive格式）"""
    if not differences:
        print("没有发现差异")
        return

    # 确保输出目录存在
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Hive表字段定义（注意：data_dt是分区字段，不出现在CSV中）
    fieldnames = [
        'database_name', 'table_name', 'partition_name',
        'metric_name', 'old_value', 'new_value', 'diff_value',
        'compare_date'
    ]

    # 添加额外的字段
    today = datetime.date.today().strftime('%Y%m%d')
    enriched_differences = []
    for diff in differences:
        enriched_diff = {
            'database_name': diff['database_name'],
            'table_name': diff['table_name'],
            'partition_name': diff['partition_name'],
            'metric_name': diff['metric_name'],
            'old_value': diff['old_value'],
            'new_value': diff['new_value'],
            'diff_value': diff['diff_value'],
            'compare_date': today  # 对比日期
        }
        enriched_differences.append(enriched_diff)

    # 写入CSV文件（Hive格式，需要指定NULL值的表示方式）
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # 不写入表头
        for diff in enriched_differences:
            # 将None转换为空字符串，这样Hive可以解析为NULL
            row = {k: ('' if v is None else str(v)) for k, v in diff.items()}
            writer.writerow(row)

    print(f"行数对比结果已导出到CSV: {output_file}")
    return output_file

def load_to_hive_simple(csv_file: str, hive_config: dict, data_dt: str, output_dir: str):
    """将CSV文件加载到Hive表"""
    if not os.path.exists(csv_file):
        print(f"错误: CSV文件不存在 {csv_file}")
        return False

    hive_database = hive_config.get('database', 'default')
    hive_table = hive_config.get('table', 'table_comparison')
    beeline_url = hive_config.get('beeline_url', 'jdbc:hive2://localhost:10000')

    try:
        print(f"准备将CSV文件加载到Hive...")
        print(f"CSV文件: {csv_file}")
        print(f"Hive表: {hive_database}.{hive_table}")

        # 使用与output_file相同的路径
        hdfs_dir = f"{output_dir}/{data_dt}"
        print(f"正在上传CSV文件到HDFS: {hdfs_dir}")
        hdfs_put_cmd = ["hdfs", "dfs", "-put", "-f", csv_file, hdfs_dir + "/"]
        print(f"执行命令: {' '.join(hdfs_put_cmd)}")
        result = subprocess.run(hdfs_put_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            print(f"上传到HDFS失败")
            return False
        print(f"CSV文件已成功上传到HDFS")

        # 使用HDFS路径加载数据到分区
        hdfs_file_path = f"{hdfs_dir}/{os.path.basename(csv_file)}"
        load_data_sql = f"""
        LOAD DATA INPATH '{hdfs_file_path}'
        OVERWRITE INTO TABLE {hive_database}.{hive_table}
        PARTITION (data_dt='{data_dt}')
        """

        print(f"执行Beeline命令...")
        cmd = ["beeline", "-u", beeline_url, "-e", load_data_sql]
        print(f"执行命令: {' '.join(cmd)}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode != 0:
            print(f"Hive命令执行失败")
            return False

        print(f"数据已成功加载到Hive表: {hive_database}.{hive_table}")
        print(f"分区: data_dt={data_dt}")

        # 验证数据加载
        count_sql = f"SELECT COUNT(*) FROM {hive_database}.{hive_table} WHERE data_dt='{data_dt}'"
        count_cmd = ["beeline", "-u", beeline_url, "-e", count_sql]
        print(f"执行命令: {' '.join(count_cmd)}")
        count_result = subprocess.run(count_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if count_result.returncode == 0:
            print(f"加载到Hive的数据行数: {count_result.stdout.decode().strip()}")

        return True

    except Exception as e:
        print(f"加载到Hive时出错: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='比较两个集群的表行数差异并导出到CSV/Hive')
    parser.add_argument('--data-dt', required=True, help='分区日期，如 20250324')
    parser.add_argument('--old-csv', help='旧集群CSV文件路径（可选，默认从csv_dir读取）')
    parser.add_argument('--new-csv', help='新集群CSV文件路径（可选，默认从csv_dir读取）')
    parser.add_argument('--output-dir', help='输出目录（可选，默认从csv_dir读取）')
    parser.add_argument('--hive-table', help='Hive表名（可选，默认从配置读取）')
    parser.add_argument('--skip-hive', action='store_true', help='跳过Hive加载步骤')

    args = parser.parse_args()

    # 加载配置
    config = load_env_config()
    csv_dir = config.get('csv_dir')

    # 输出目录使用csv_dir
    output_dir = csv_dir

    # 自动拼接文件路径
    today = args.data_dt
    if not args.old_csv:
        args.old_csv = os.path.join(csv_dir, f'{args.data_dt}/{args.data_dt}_old_table_stats.csv')
    if not args.new_csv:
        args.new_csv = os.path.join(csv_dir, f'{args.data_dt}/{args.data_dt}_new_table_stats.csv')

    print(f"旧集群CSV: {args.old_csv}")
    print(f"新集群CSV: {args.new_csv}")
    print(f"输出目录: {output_dir}")

    # 检查 old-csv 和 new-csv 文件是否存在，如果不存在则等待5分钟重试，最多48次（240分钟）
    max_retries = 48
    retry_interval = 300  # 5 minutes in seconds

    for attempt in range(max_retries):
        old_exists = os.path.exists(args.old_csv)
        new_exists = os.path.exists(args.new_csv)
        if old_exists and new_exists:
            break
        else:
            if attempt < max_retries - 1:
                missing = []
                if not old_exists:
                    missing.append(f"旧集群CSV: {args.old_csv}")
                if not new_exists:
                    missing.append(f"新集群CSV: {args.new_csv}")
                print(f"等待 {retry_interval//60} 分钟，第 {attempt+1}/{max_retries} 次尝试...")
                print(f"缺失文件: {', '.join(missing)}")
                time.sleep(retry_interval)
            else:
                missing = []
                if not old_exists:
                    missing.append(args.old_csv)
                if not new_exists:
                    missing.append(args.new_csv)
                print(f"错误: 以下文件在 {max_retries*retry_interval//60} 分钟内仍未出现: {missing}")
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

    # 导出到CSV文件
    print(f"\n正在导出结果到CSV...")
    output_csv = os.path.join(output_dir, f'{args.data_dt}/{args.data_dt}_table_comparison.csv')
    csv_file = export_to_hive_csv(differences, output_csv)

    if csv_file:
        print(f"CSV文件已生成: {csv_file}")

        # 加载到Hive（可选）
        if not args.skip_hive:
            print(f"\n正在加载数据到Hive...")

            # 从配置获取Hive配置
            hive_config = config.get('hive', {})
            if args.hive_table:
                hive_config['table'] = args.hive_table

            # 如果没有Hive配置，使用默认值
            if not hive_config:
                hive_config = {
                    'database': 'default',
                    'table': 'table_comparison'
                }
                print(f"使用默认Hive配置: {hive_config}")

            success = load_to_hive_simple(csv_file, hive_config, args.data_dt, output_dir)
            if success:
                print("数据已成功加载到Hive!")
            else:
                print("警告: 加载到Hive失败，但CSV文件已生成")
        else:
            print("跳过Hive加载步骤")
    else:
        print("警告: CSV文件生成失败")

    print("行数对比完成!")

if __name__ == '__main__':
    main()