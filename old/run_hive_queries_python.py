#!/usr/bin/env python3
"""
Hive 查询执行脚本
读取单个 .sql 文件（多条语句以分号分隔），执行并导出 CSV
"""

import argparse
import json
import os
import subprocess
import pandas as pd


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


def execute_hive_query_local(sql: str) -> pd.DataFrame:
    """本地执行 Hive 查询"""
    # 添加 MR 引擎设置
    sql_with_engine = "set hive.execution.engine=mr;\n" + sql

    cmd = [
        'hive',
        '-e', sql_with_engine
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
            return None

        output = result.stdout.strip()
        if not output:
            return None

        # 过滤提示符行
        lines = []
        for line in output.split('\n'):
            line = line.strip()
            if not line or line.startswith('>') or line.startswith('0:') or line.startswith('.'):
                continue
            lines.append(line)

        if len(lines) < 2:
            return None

        headers = lines[0].split('\t')
        data = []
        for line in lines[1:]:
            cols = line.split('\t')
            if len(cols) == len(headers):
                data.append(cols)

        if not data:
            return None

        df = pd.DataFrame(data, columns=headers)
        return df

    except subprocess.TimeoutExpired:
        print("查询超时")
        return None
    except Exception as e:
        print(f"执行错误: {e}")
        return None


def execute_hive_query_ssh(sql: str, ssh_config: dict = None) -> pd.DataFrame:
    """SSH 远程执行 Hive 查询"""
    if not ssh_config:
        print("缺少 SSH 配置")
        return None

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
        # 在 SQL 前添加设置 MR 引擎
        sql_with_engine = "set hive.execution.engine=mr;\n" + sql
        stdout, stderr = write_process.communicate(input=sql_with_engine, timeout=30)

        if write_process.returncode != 0:
            print(f"写入远程文件失败: {stderr}")
            return None

        # 远程执行 beeline
        # 使用 hive -e 替代 beeline，输出更干净
        ssh_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "hive -e \\"source {remote_sql_file}\\""'

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
            print(f"执行失败: {result.stderr}")
            return None

        # 解析 tab-separated 输出
        output = result.stdout.strip()
        if not output:
            return None

        # 过滤 beeline 提示符和续行符
        lines = []
        for line in output.split('\n'):
            # 跳过提示符行和续行符（以 > 或 . 开头）
            line = line.strip()
            if not line or line.startswith('>') or line.startswith('0:') or line.startswith('.'):
                continue
            lines.append(line)

        if len(lines) < 2:
            return None

        # 首行为 header
        headers = lines[0].split('\t')
        # 数据行（只保留列数与 header 一致的行）
        data = []
        for line in lines[1:]:
            cols = line.split('\t')
            if len(cols) == len(headers):
                data.append(cols)

        if not data:
            return None

        df = pd.DataFrame(data, columns=headers)
        return df

    except subprocess.TimeoutExpired:
        print("查询超时")
        return None
    except Exception as e:
        print(f"执行错误: {e}")
        return None


def expand_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    动态展开指标列
    保留基础列：table_name, partition_col, computed_at, data_dt
    其余列均视为 metric 列，每个 metric 列展开为一行
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 基础列（可能不存在）
    base_columns = ['table_name', 'partition_col', 'computed_at', 'data_dt']

    # 找出 metric 列（不在基础列中的）
    metric_columns = [col for col in df.columns if col not in base_columns]

    if not metric_columns:
        return pd.DataFrame()

    # 展开每条记录
    expanded_rows = []

    for _, row in df.iterrows():
        for metric_col in metric_columns:
            metric_value = row.get(metric_col, None)
            if metric_value is None:
                continue

            new_row = {
                'table_name': row.get('table_name', ''),
                'partition_col': row.get('partition_col', ''),
                'metric_name': metric_col,
                'value': metric_value,
                'computed_at': row.get('computed_at', ''),
                'data_dt': row.get('data_dt', '')
            }
            expanded_rows.append(new_row)

    return pd.DataFrame(expanded_rows)


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
        default='old_summary_data.csv',
        help='输出 CSV 文件路径'
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

        # 执行查询
        # 根据配置选择本地或 SSH 执行
        if use_ssh:
            df = execute_hive_query_ssh(actual_sql, ssh_config)
        else:
            df = execute_hive_query_local(actual_sql)

        if df is None or df.empty:
            print(f"  第 {i+1} 条语句返回空结果")
            continue

        print(f"  返回 {len(df)} 行，列: {list(df.columns)}")

        # 动态展开指标
        expanded_df = expand_metrics(df)
        if not expanded_df.empty:
            all_results.append(expanded_df)
            print(f"  展开为 {len(expanded_df)} 行")

    if not all_results:
        print("没有结果可导出")
        return

    # 合并所有结果
    final_df = pd.concat(all_results, ignore_index=True)

    # 导出 CSV（tab 分隔）
    final_df.to_csv(args.output_csv, sep='\t', index=False)
    print(f"\n结果已导出: {args.output_csv}")
    print(f"共 {len(final_df)} 行")


if __name__ == '__main__':
    main()
