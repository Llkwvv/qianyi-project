#!/usr/bin/env python3
"""
Hive 查询执行脚本
读取单个 .sql 文件（多条语句以分号分隔），执行并导出 CSV
"""

import argparse
import csv
import json
import os
import subprocess
from datetime import datetime


def parse_sql_file(sql_file: str) -> list:
    """读取 SQL 文件，按分号拆分出每条语句"""
    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 按分号拆分，去除空语句
    statements = []
    for stmt in content.split(';'):
        stmt = stmt.strip()
        if stmt:
            statements.append(stmt)

    return statements


def load_env_config(config_path: str = 'env_config.json') -> dict:
    """加载环境配置文件"""
    config_file = os.path.join(os.path.dirname(__file__), config_path)
    if not os.path.exists(config_file):
        return {}
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def replace_placeholder(sql: str, data_dt: str) -> str:
    """替换模板中的 {{data_dt}} 占位符"""
    return sql.replace('{{data_dt}}', data_dt)


def execute_hive_query_local(sql: str):
    """本地执行 Hive 查询，返回 (headers, data) 元组"""
    # 通过 --hiveconf 设置打印表头
    cmd = [
        'hive',
        '--hiveconf', 'hive.cli.print.header=true',
        '-e', sql
    ]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

        if result.returncode != 0:
            print(f"执行失败: {result.stderr}")
            return None, None

        output = result.stdout.strip()
        if not output:
            return None, None

        # 过滤提示符行
        lines = []
        for line in output.split('\n'):
            line = line.strip()
            if not line or line.startswith('>') or line.startswith('0:') or line.startswith('.'):
                continue
            lines.append(line)

        if len(lines) < 2:
            # 调试：打印原始输出帮助排查问题
            print(f"  警告: 解析失败，原始输出 = {output[:1000]}")
            return None, None

        headers = lines[0].split('\t')
        data = []
        for line in lines[1:]:
            cols = line.split('\t')
            if len(cols) == len(headers):
                data.append(cols)

        if not data:
            return None, None

        return headers, data

    except subprocess.TimeoutExpired:
        print("查询超时")
        return None, None
    except Exception as e:
        print(f"执行错误: {e}")
        return None, None


def execute_hive_query_ssh(sql: str, ssh_config: dict = None):
    """SSH 远程执行 Hive 查询，返回 (headers, data) 元组"""
    if not ssh_config:
        print("缺少 SSH 配置")
        return None, None

    ssh_host = ssh_config.get('ssh_host')
    ssh_port = ssh_config.get('ssh_port', 22)
    ssh_user = ssh_config.get('ssh_user')
    hive_host = ssh_config.get('hive_host', 'localhost')
    hive_port = ssh_config.get('hive_port', 10000)
    hive_username = ssh_config.get('username', '')

    try:
        # 先将 SQL 写入远程服务器的临时文件
        remote_sql_file = f"/tmp/hive_query_{os.getpid()}.sql"

        # 使用 scp 或 echo 写入远程文件
        write_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "cat > {remote_sql_file}"'

        # 用 stdin 写入 SQL 内容
        write_process = subprocess.Popen(
            write_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        # 通过 hiveconf 参数设置打印表头
        sql_with_engine = sql
        stdout, stderr = write_process.communicate(input=sql_with_engine, timeout=30)

        if write_process.returncode != 0:
            print(f"写入远程文件失败: {stderr}")
            return None, None

        # 远程执行 beeline
        # 使用 hive -e 替代 beeline，输出更干净
        # 通过 --hiveconf 设置打印表头
        ssh_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "hive --hiveconf hive.cli.print.header=true -e \\"source {remote_sql_file}\\""'

        result = subprocess.run(
            ssh_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

        # 清理远程临时文件
        subprocess.run(
            f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "rm -f {remote_sql_file}"',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if result.returncode != 0:
            print(f"  Hive 执行失败 (返回码: {result.returncode})")
            print(f"  stderr: {result.stderr[:500]}")
            return None, None

        # 解析 tab-separated 输出
        output = result.stdout.strip()

        if not output:
            print(f"  警告: Hive 返回空输出")
            return None, None

        # 打印前500字符帮助调试
        print(f"  [调试] 原始输出前500字符:\n{output[:500]}")

        # 过滤 beeline 提示符和续行符
        lines = []
        for line in output.split('\n'):
            # 跳过提示符行和续行符（以 > 或 . 开头）
            line = line.strip()
            if not line or line.startswith('>') or line.startswith('0:') or line.startswith('.'):
                continue
            lines.append(line)

        print(f"  [调试] 过滤后行数: {len(lines)}")
        if lines:
            print(f"  [调试] 首行: {lines[0]}")

        if len(lines) < 2:
            print(f"  警告: 输出行数不足2行，无法解析")
            return None, None

        # 首行为 header
        headers = lines[0].split('\t')
        # 数据行（只保留列数与 header 一致的行）
        data = []
        for line in lines[1:]:
            cols = line.split('\t')
            if len(cols) == len(headers):
                data.append(cols)

        if not data:
            print(f"  警告: 无有效数据行，过滤后所有行: {lines}")
            return None, None

        return headers, data

    except subprocess.TimeoutExpired:
        print("查询超时")
        return None, None
    except Exception as e:
        print(f"执行错误: {e}")
        return None, None


def expand_metrics(headers: list, data: list, cluster_name: str = 'old'):
    """
    动态展开指标列
    保留基础列：table_name, partition_col, computed_at, data_dt
    其余列均视为 metric 列，每个 metric 列展开为一行
    返回: (headers, expanded_data) 元组
    """
    if not headers or not data:
        return None, None

    # 基础列（可能不存在）
    base_columns = ['table_name', 'partition_col', 'computed_at', 'data_dt']

    # 找出 metric 列（不在基础列中的）
    metric_columns = [col for col in headers if col not in base_columns]

    if not metric_columns:
        return None, None

    # 构建列名到索引的映射
    col_idx = {col: i for i, col in enumerate(headers)}

    # 展开每条记录
    expanded_rows = []

    for row in data:
        for metric_col in metric_columns:
            metric_idx = col_idx.get(metric_col)
            if metric_idx is None:
                continue

            metric_value = row[metric_idx] if metric_idx < len(row) else ''

            new_row = [
                cluster_name,
                row[col_idx.get('table_name', 0)] if col_idx.get('table_name', 0) < len(row) else '',
                row[col_idx.get('partition_col', 0)] if col_idx.get('partition_col', 0) < len(row) else '',
                metric_col,
                metric_value,
                row[col_idx.get('computed_at', 0)] if col_idx.get('computed_at', 0) < len(row) else '',
                row[col_idx.get('data_dt', 0)] if col_idx.get('data_dt', 0) < len(row) else ''
            ]
            expanded_rows.append(new_row)

    expanded_headers = ['cluster', 'table_name', 'partition_col', 'metric_name', 'value', 'computed_at', 'data_dt']
    return expanded_headers, expanded_rows


def main():
    parser = argparse.ArgumentParser(
        description='执行 Hive 查询并导出 CSV'
    )
    parser.add_argument(
        '--sql-file',
        required=True,
        help='SQL 文件路径'
    )
    parser.add_argument(
        '--data-dt',
        required=True,
        help='分区日期，替换模板中的 {{data_dt}}'
    )
    parser.add_argument(
        '--output-csv',
        help='输出 CSV 文件路径（默认: /data/transfer_agent/data/upload/yyyymmdd）'
    )
    parser.add_argument(
        '--cluster',
        default='old',
        help='集群名称，对应 env_config.json 中的 clusters 配置'
    )
    parser.add_argument(
        '--hive-server',
        help='Hive 服务器连接字符串（会覆盖 --cluster 配置）'
    )

    args = parser.parse_args()

    # 如果未指定输出路径，使用默认路径 /data/transfer_agent/data/upload/yyyymmdd
    if not args.output_csv:
        today = datetime.now().strftime('%Y%m%d')
        args.output_csv = f'/data/transfer_agent/data/upload/{today}/old_summary.csv'
        print(f"使用默认输出路径: {args.output_csv}")

    # 读取配置文件获取集群信息
    config = load_env_config()
    cluster_config = config.get('clusters', {}).get(args.cluster, {})

    # 读取配置
    use_ssh = cluster_config.get('use_ssh', True) if cluster_config else True

    if use_ssh:
        if cluster_config:
            ssh_config = {
                'ssh_host': cluster_config.get('ssh_host'),
                'ssh_port': cluster_config.get('ssh_port', 22),
                'ssh_user': cluster_config.get('ssh_user'),
                'hive_host': cluster_config.get('hive_host', 'localhost'),
                'hive_port': cluster_config.get('port', 10000),
                'username': cluster_config.get('username', '')
            }
            print(f"使用 SSH: {ssh_config['ssh_user']}@{ssh_config['ssh_host']}")
        else:
            ssh_config = None
            print("警告: 未找到集群配置")
    else:
        ssh_config = None
        print("使用本地 Hive")

    # 读取 SQL 文件
    print(f"读取 SQL 文件: {args.sql_file}")
    statements = parse_sql_file(args.sql_file)
    print(f"共 {len(statements)} 条语句")

    # 执行每条语句并收集结果
    all_results = []

    for i, stmt in enumerate(statements):
        print(f"\n执行第 {i+1} 条语句...")

        # 替换占位符
        actual_sql = replace_placeholder(stmt, args.data_dt)

        # 打印具体SQL内容
        print(f"SQL: {actual_sql}")

        # 执行查询
        # 根据配置选择本地或 SSH 执行
        if use_ssh:
            headers, rows = execute_hive_query_ssh(actual_sql, ssh_config)
        else:
            headers, rows = execute_hive_query_local(actual_sql)

        if not headers or not rows:
            print(f"  第 {i+1} 条语句返回空结果")
            continue

        print(f"  返回 {len(rows)} 行，列: {headers}")

        # 动态展开指标
        expanded_headers, expanded_rows = expand_metrics(headers, rows, args.cluster)
        if expanded_headers and expanded_rows:
            all_results.append((expanded_headers, expanded_rows))
            print(f"  展开为 {len(expanded_rows)} 行")

    if not all_results:
        print("没有结果可导出")
        return

    # 合并所有结果（使用第一个结果的 headers）
    final_headers = all_results[0][0]
    final_rows = []
    for _, rows in all_results:
        final_rows.extend(rows)

    # 导出 CSV（tab 分隔，不带表头）
    # 确保输出目录存在
    output_dir = os.path.dirname(args.output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(args.output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerows(final_rows)

    print(f"\n结果已导出: {args.output_csv}")
    print(f"共 {len(final_rows)} 行")


if __name__ == '__main__':
    main()
