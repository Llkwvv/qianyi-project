#!/usr/bin/env python3
"""
Hive 集群指标校验工具
功能:
  1. ingest-old: 从旧集群 CSV 读取并通过 LOAD DATA 写入 Hive 统一结果表（带 cluster=old 字段）
  2. run-new: 新集群执行相同语句并通过 LOAD DATA 写入同一张表（带 cluster=new 字段）
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime


# ============== 通用函数 ==============

def load_env_config(config_path: str) -> dict:
    """加载环境配置文件"""
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_cluster_config(config: dict, cluster: str) -> dict:
    """获取集群配置"""
    clusters = config.get('clusters', {})
    if cluster not in clusters:
        raise ValueError(f"集群配置中未找到: {cluster}")
    return clusters[cluster]


def get_mysql_config(config: dict) -> dict:
    """获取 MySQL 配置"""
    return config.get('mysql', {})


def replace_placeholder(sql: str, data_dt: str) -> str:
    """替换模板中的 {{data_dt}} 占位符"""
    return sql.replace('{{data_dt}}', data_dt)


def upload_file_via_scp(local_file: str, ssh_config: dict, remote_dir: str = "/tmp") -> str:
    """通过 SCP 上传文件到远程服务器"""
    ssh_host = ssh_config.get('ssh_host')
    ssh_port = ssh_config.get('ssh_port', 22)
    ssh_user = ssh_config.get('ssh_user')

    filename = os.path.basename(local_file)
    remote_file = f"{remote_dir}/{filename}_{os.getpid()}"

    try:
        scp_cmd = f'scp -P {ssh_port} "{local_file}" {ssh_user}@{ssh_host}:"{remote_file}"'
        result = subprocess.run(
            scp_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

        if result.returncode != 0:
            print(f"SCP 上传失败: {result.stderr}")
            return None

        return remote_file
    except Exception as e:
        print(f"SCP 上传错误: {e}")
        return None


def execute_hive_query(sql: str, cluster_config: dict):
    """执行 Hive 查询，根据配置选择 SSH 或本地 beeline 方式"""
    use_ssh = cluster_config.get('use_ssh', True)

    if use_ssh:
        return execute_hive_query_ssh(sql, cluster_config)
    else:
        return execute_hive_query_beeline(sql, cluster_config)


def execute_hive_query_beeline(sql: str, cluster_config: dict):
    """本地直接执行 beeline 查询，返回 (headers, data) 元组"""
    beeline_url = cluster_config.get('beeline_url', 'jdbc:hive2://localhost:10000/default')

    try:
        # 设置 beeline 命令，开启 header 输出
        full_sql = f"set hive.cli.print.header=true;{sql}"

        # 使用 beeline -e 参数直接执行 SQL，--silent=true 减少输出
        cmd = [
            'beeline',
            '-u', beeline_url,
            '--silent=true --showHeader=false --outputformat=tsv2',
            '-e', full_sql
        ]

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

        # 过滤 beeline 输出中的装饰行（边框、连接信息等）
        lines = []
        for line in output.split('\n'):
            line = line.strip()
            # 跳过空行、提示符、边框符号
            if not line:
                continue
            if line.startswith('>') or line.startswith('0:') or line.startswith('.'):
                continue
            # 只过滤纯边框行（整行只有 + | = 符号），保留实际数据行
            if line.startswith('+') or line.startswith('='):
                continue
            if line.startswith('|') and all(c in '| +-=' or c.isspace() for c in line):
                continue
            if line.startswith('Connecting') or line.startswith('Connected') or line.startswith('Hive on'):
                continue
            if line.startswith('Hadoop job') or line.startswith('Query'):
                continue
            if 'No rows affected' in line:
                continue
            if 'row selected' in line or 'rows selected' in line:
                continue
            if line.startswith('jdbc:'):
                continue
            lines.append(line)

        if len(lines) < 2:
            return None, None

        # 解析表头和数据行（beeline 使用 | 分隔）
        def parse_beeline_line(line):
            # 去掉首尾的 |，然后按 | 分割
            line = line.strip()
            if line.startswith('|'):
                line = line[1:]
            if line.endswith('|'):
                line = line[:-1]
            return [col.strip() for col in line.split('|')]

        headers = parse_beeline_line(lines[0])
        data = []
        for line in lines[1:]:
            cols = parse_beeline_line(line)
            if len(cols) == len(headers):
                data.append(cols)

        if not data:
            return None, None

        return headers, data

    except Exception as e:
        print(f"执行错误: {e}")
        return None, None


def execute_hive_query_ssh(sql: str, ssh_config: dict):
    """SSH 远程执行 Hive 查询，返回 (headers, data) 元组"""
    ssh_host = ssh_config.get('ssh_host')
    ssh_port = ssh_config.get('ssh_port', 22)
    ssh_user = ssh_config.get('ssh_user')

    try:
        remote_sql_file = f"/tmp/hive_query_{os.getpid()}.sql"
        write_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "cat > {remote_sql_file}"'

        write_process = subprocess.Popen(
            write_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        sql_with_header = "set hive.cli.print.header=true;\n" + sql
        stdout, stderr = write_process.communicate(input=sql_with_header, timeout=30)

        if write_process.returncode != 0:
            print(f"写入远程文件失败: {stderr}")
            return None, None

        # 使用 beeline -f 参数执行 SQL 文件
        beeline_url = ssh_config.get('beeline_url', 'jdbc:hive2://localhost:10000/default')
        ssh_cmd = f"ssh -p {ssh_port} {ssh_user}@{ssh_host} 'beeline -u \"{beeline_url}\" -n {ssh_user} -f {remote_sql_file}'"
        result = subprocess.run(
            ssh_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

        # 清理远程 SQL 文件
        subprocess.run(
            f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "rm -f {remote_sql_file}"',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if result.returncode != 0:
            print(f"执行失败: {result.stderr}")
            return None, None

        output = result.stdout.strip()
        if not output:
            return None, None

        # 过滤 beeline 输出中的装饰行（边框、连接信息等）
        lines = []
        for line in output.split('\n'):
            line = line.strip()
            # 跳过空行、提示符、边框符号
            if not line:
                continue
            if line.startswith('>') or line.startswith('0:') or line.startswith('.'):
                continue
            # 只过滤纯边框行（整行只有 + | = 符号），保留实际数据行
            if line.startswith('+') or line.startswith('='):
                continue
            if line.startswith('|') and all(c in '| +-=' or c.isspace() for c in line):
                continue
            if line.startswith('Connecting') or line.startswith('Connected') or line.startswith('Hive on'):
                continue
            if line.startswith('Hadoop job') or line.startswith('Query'):
                continue
            if 'No rows affected' in line:
                continue
            if 'row selected' in line or 'rows selected' in line:
                continue
            if line.startswith('jdbc:'):
                continue
            lines.append(line)

        if len(lines) < 2:
            return None, None

        # 解析表头和数据行（beeline 使用 | 分隔）
        def parse_beeline_line(line):
            # 去掉首尾的 |，然后按 | 分割
            line = line.strip()
            if line.startswith('|'):
                line = line[1:]
            if line.endswith('|'):
                line = line[:-1]
            return [col.strip() for col in line.split('|')]

        headers = parse_beeline_line(lines[0])
        data = []
        for line in lines[1:]:
            cols = parse_beeline_line(line)
            if len(cols) == len(headers):
                data.append(cols)

        if not data:
            return None, None

        return headers, data

    except Exception as e:
        print(f"执行错误: {e}")
        return None, None


def execute_hive_query_no_result(sql: str, cluster_config: dict) -> bool:
    """执行 Hive 查询（不返回结果），根据配置选择 SSH 或本地 beeline 方式"""
    use_ssh = cluster_config.get('use_ssh', True)

    if use_ssh:
        return execute_hive_query_ssh_no_result(sql, cluster_config)
    else:
        return execute_hive_query_beeline_no_result(sql, cluster_config)


def execute_hive_query_beeline_no_result(sql: str, cluster_config: dict) -> bool:
    """本地直接执行 beeline 查询（不返回结果）"""
    beeline_url = cluster_config.get('beeline_url', 'jdbc:hive2://localhost:10000/default')

    try:
        # 使用 beeline -e 参数直接执行 SQL，--silent=true 减少输出
        cmd = [
            'beeline',
            '-u', beeline_url,
            '--silent=true --showHeader=false --outputformat=tsv2',
            '-e', sql
        ]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

        if result.returncode != 0:
            print(f"执行失败: {result.stderr}")
            return False

        return True

    except Exception as e:
        print(f"执行错误: {e}")
        return False


def execute_hive_query_ssh_no_result(sql: str, ssh_config: dict) -> bool:
    """SSH 远程执行 Hive 查询（不返回结果）"""
    ssh_host = ssh_config.get('ssh_host')
    ssh_port = ssh_config.get('ssh_port', 22)
    ssh_user = ssh_config.get('ssh_user')

    try:
        remote_sql_file = f"/tmp/hive_query_{os.getpid()}.sql"
        write_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "cat > {remote_sql_file}"'

        write_process = subprocess.Popen(
            write_cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        stdout, stderr = write_process.communicate(input=sql, timeout=30)

        if write_process.returncode != 0:
            print(f"写入远程文件失败: {stderr}")
            return False

        # 使用 beeline -f 参数执行 SQL 文件
        beeline_url = ssh_config.get('beeline_url', 'jdbc:hive2://localhost:10000/default')
        ssh_cmd = f"ssh -p {ssh_port} {ssh_user}@{ssh_host} 'beeline -u \"{beeline_url}\" -n {ssh_user} -f {remote_sql_file}'"
        result = subprocess.run(
            ssh_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

        # 清理远程 SQL 文件
        subprocess.run(
            f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "rm -f {remote_sql_file}"',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if result.returncode != 0:
            print(f"执行失败: {result.stderr}")
            return False

        return True

    except Exception as e:
        print(f"执行错误: {e}")
        return False


def execute_hive_insert(cluster_config: dict, headers: list, rows: list, validation_db: str, table_name: str, overwrite: bool = False) -> bool:
    """执行 Hive INSERT 批量写入"""
    if not rows:
        return True

    full_table_name = f"{validation_db}.{table_name}"
    overwrite_clause = "OVERWRITE" if overwrite else ""

    # 批量生成 VALUES 子句，每行数据用括号包裹
    values_list = []
    for row in rows:
        # 对每个值进行处理，NULL 保持原样，字符串加引号
        formatted_values = []
        for val in row:
            if val is None or val == '' or val.upper() == 'NULL':
                formatted_values.append('NULL')
            else:
                # 转义单引号
                val_str = str(val).replace("'", "''")
                formatted_values.append(f"'{val_str}'")
        values_list.append(f"({','.join(formatted_values)})")

    # 拼接完整的 INSERT 语句
    # Hive: INSERT INTO 或 INSERT OVERWRITE（不需要 TABLE 关键字）
    values_clause = ',\n'.join(values_list)
    if overwrite_clause:
        insert_sql = f"INSERT {overwrite_clause} TABLE {full_table_name} VALUES\n{values_clause};"
    else:
        insert_sql = f"INSERT INTO {full_table_name} VALUES\n{values_clause};"

    print(f"执行 INSERT: {len(rows)} 行")

    return execute_hive_query_no_result(insert_sql, cluster_config)


def execute_hive_load_data(cluster_config: dict, file_path: str, validation_db: str, table_name: str, overwrite: bool = False) -> bool:
    """执行 Hive LOAD DATA 命令"""
    full_table_name = f"{validation_db}.{table_name}"
    overwrite_clause = "OVERWRITE" if overwrite else ""

    load_sql = f"LOAD DATA LOCAL INPATH '{file_path}' {overwrite_clause} INTO TABLE {full_table_name};"
    print(f"执行 LOAD DATA: {load_sql}")

    return execute_hive_query_no_result(load_sql, cluster_config)


def expand_metrics(headers: list, data: list, cluster: str):
    """动态展开指标列并添加 cluster 字段，返回 (headers, expanded_data) 元组"""
    if not headers or not data:
        return None, None

    base_columns = ['table_name', 'partition_col', 'computed_at', 'data_dt']
    metric_columns = [col for col in headers if col not in base_columns]

    if not metric_columns:
        return None, None

    # 构建列名到索引的映射
    col_idx = {col: i for i, col in enumerate(headers)}

    expanded_rows = []
    for row in data:
        for metric_col in metric_columns:
            metric_idx = col_idx.get(metric_col)
            if metric_idx is None:
                continue

            metric_value = row[metric_idx] if metric_idx < len(row) else ''

            new_row = [
                cluster,
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


def create_database(validation_db: str, cluster_config: dict) -> bool:
    """创建数据库（如果不存在）"""
    create_sql = f"CREATE DATABASE IF NOT EXISTS {validation_db};"
    print(f"创建数据库: {validation_db}")
    return execute_hive_query_no_result(create_sql, cluster_config)


def create_summary_table(validation_db: str, table_name: str, cluster_config: dict) -> bool:
    """创建统一结果表（如果不存在）"""
    # 先确保数据库存在
    if not create_database(validation_db, cluster_config):
        print(f"创建数据库失败: {validation_db}")
        return False

    full_table_name = f"{validation_db}.{table_name}"

    create_sql = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
    cluster STRING,
    table_name STRING,
    partition_col STRING,
    metric_name STRING,
    value STRING,
    computed_at TIMESTAMP,
    data_dt STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
"""
    print(f"创建表: {full_table_name}")
    return execute_hive_query_no_result(create_sql, cluster_config)


def cleanup_remote_file(remote_file: str, ssh_config: dict) -> bool:
    """清理远程服务器上的临时文件"""
    if not remote_file:
        return True

    ssh_host = ssh_config.get('ssh_host')
    ssh_port = ssh_config.get('ssh_port', 22)
    ssh_user = ssh_config.get('ssh_user')

    try:
        ssh_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "rm -f {remote_file}"'
        result = subprocess.run(
            ssh_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=30
        )
        return result.returncode == 0
    except Exception as e:
        print(f"清理远程文件失败: {e}")
        return False


# ============== 子命令: ingest-old ==============

def cmd_ingest_old(args):
    """从旧集群 CSV 读取并通过 LOAD DATA 写入 Hive 统一结果表（带 cluster=old 字段）"""
    config = load_env_config(args.config)
    insert_mysql = config.get('insert_mysql', {})
    validation_db = insert_mysql.get('db', 'validation_db')
    metrics_summary_table = 'metrics_summary'

    cluster_config = get_cluster_config(config, args.cluster)
    use_ssh = cluster_config.get('use_ssh', True)

    print(f"使用集群: {args.cluster}")
    if use_ssh:
        print(f"SSH: {cluster_config['ssh_user']}@{cluster_config['ssh_host']}")
    else:
        print(f"Beeline: {cluster_config.get('beeline_url')}")

    # 读取 CSV
    csv_path = args.csv
    if not csv_path:
        # 从配置读取默认路径
        csv_dir = config.get('csv_dir', 'input')
        csv_path = os.path.join(csv_dir, 'old_summary.csv')
        print(f"从配置读取默认 CSV 路径: {csv_path}")

    # 转换为绝对路径
    if not os.path.exists(csv_path):
        print(f"CSV 文件不存在: {csv_path}")
        return 1

    # 创建表
    create_summary_table(validation_db, metrics_summary_table, cluster_config)

    # 直接使用原始 CSV 文件
    csv_to_use = csv_path
    print(f"CSV 文件: {csv_to_use}")

    if use_ssh:
        # SSH 模式：上传到远程服务器
        print(f"上传 CSV 文件到远程服务器")
        remote_csv_path = upload_file_via_scp(csv_to_use, cluster_config)
        if not remote_csv_path:
            print("上传 CSV 文件失败")
            return 1

        try:
            success = execute_hive_load_data(cluster_config, remote_csv_path, validation_db, metrics_summary_table, args.overwrite)
            if success:
                print("CSV 数据已通过 LOAD DATA 成功载入 Hive 表")
            else:
                print("LOAD DATA 执行失败")
                return 1
        finally:
            cleanup_remote_file(remote_csv_path, cluster_config)
    else:
        # Beeline 本地模式
        success = execute_hive_load_data(cluster_config, csv_to_use, validation_db, metrics_summary_table, args.overwrite)
        if success:
            print("CSV 数据已通过 LOAD DATA 成功载入 Hive 表")
        else:
            print("LOAD DATA 执行失败")
            return 1

    print("\n完成!")
    return 0


# ============== 子命令: run-new ==============

def cmd_run_new(args):
    """新集群执行相同语句并通过 LOAD DATA 写入同一张表（带 cluster=new 字段）"""
    config = load_env_config(args.config)
    insert_mysql = config.get('insert_mysql', {})
    validation_db = insert_mysql.get('db', 'validation_db')
    metrics_summary_table = 'metrics_summary'

    cluster_config = get_cluster_config(config, args.cluster)
    use_ssh = cluster_config.get('use_ssh', True)

    print(f"使用集群: {args.cluster}")
    if use_ssh:
        print(f"SSH: {cluster_config['ssh_user']}@{cluster_config['ssh_host']}")
    else:
        print(f"Beeline: {cluster_config.get('beeline_url')}")

    # 读取 SQL 文件
    sql_file = args.sql_file
    if not sql_file:
        print("错误: 请通过 --sql-file 参数指定 SQL 文件路径")
        return 1

    # 转换为绝对路径
    if not os.path.exists(sql_file):
        print(f"SQL 文件不存在: {sql_file}")
        return 1

    # 分区日期
    data_dt = args.data_dt
    if not data_dt:
        print("错误: 请通过 --data-dt 参数指定分区日期")
        return 1

    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
    print(f"读取 SQL 文件: {sql_file}, 共 {len(statements)} 条语句")

    # 创建目标表
    create_summary_table(validation_db, metrics_summary_table, cluster_config)

    # 执行每条语句
    all_results = []

    for i, stmt in enumerate(statements):
        print(f"\n执行第 {i+1} 条语句...")

        actual_sql = replace_placeholder(stmt, data_dt)

        # 打印具体SQL内容
        print(f"SQL: {actual_sql}")

        headers, rows = execute_hive_query(actual_sql, cluster_config)

        if not headers or not rows:
            print(f"  第 {i+1} 条语句返回空结果")
            continue

        print(f"  返回 {len(rows)} 行，列: {headers}")

        expanded_headers, expanded_rows = expand_metrics(headers, rows, 'new')
        if expanded_headers and expanded_rows:
            all_results.append((expanded_headers, expanded_rows))
            print(f"  展开为 {len(expanded_rows)} 行")

    if not all_results:
        print("没有结果可导出")
        return 1

    # 合并结果
    final_headers = all_results[0][0]
    final_rows = []
    for _, rows in all_results:
        final_rows.extend(rows)

    print(f"合并结果: {len(final_rows)} 行")

    # 通过 INSERT 写入 Hive 表
    success = execute_hive_insert(cluster_config, final_headers, final_rows, validation_db, metrics_summary_table, args.overwrite)
    if success:
        print("新集群数据已通过 INSERT 成功写入 Hive 表")
    else:
        print("INSERT 执行失败")
        return 1

    # 导出 CSV
    if args.output_csv:
        output_csv = args.output_csv
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(final_rows)
        print(f"\nCSV 已导出: {output_csv}")

    print(f"\n完成! 共处理 {len(final_rows)} 行")
    return 0


# ============== 子命令: run-all ==============

def cmd_run_all(args):
    """依次执行 ingest-old 和 run-new"""
    config = load_env_config(args.config)
    insert_mysql = config.get('insert_mysql', {})
    validation_db = insert_mysql.get('db', 'validation_db')
    metrics_summary_table = 'metrics_summary'

    cluster_config = get_cluster_config(config, args.cluster)
    use_ssh = cluster_config.get('use_ssh', True)

    print(f"========== 步骤 1: ingest-old ==========")
    print(f"使用集群: {args.cluster}")
    if use_ssh:
        print(f"SSH: {cluster_config['ssh_user']}@{cluster_config['ssh_host']}")
    else:
        print(f"Beeline: {cluster_config.get('beeline_url')}")

    # 读取 CSV
    csv_path = args.csv
    if not csv_path:
        # 从配置读取默认路径
        csv_dir = config.get('csv_dir', 'input')
        csv_path = os.path.join(csv_dir, 'old_summary.csv')
        print(f"从配置读取默认 CSV 路径: {csv_path}")

    # 转换为绝对路径
    if not os.path.exists(csv_path):
        print(f"CSV 文件不存在: {csv_path}")
        return 1

    # 创建表
    create_summary_table(validation_db, metrics_summary_table, cluster_config)

    # 直接使用原始 CSV 文件
    csv_to_use = csv_path
    print(f"CSV 文件: {csv_to_use}")

    if use_ssh:
        # SSH 模式：上传到远程服务器
        print(f"上传 CSV 文件到远程服务器")
        remote_csv_path = upload_file_via_scp(csv_to_use, cluster_config)
        if not remote_csv_path:
            print("上传 CSV 文件失败")
            return 1

        try:
            success = execute_hive_load_data(cluster_config, remote_csv_path, validation_db, metrics_summary_table, args.overwrite)
            if success:
                print("CSV 数据已通过 LOAD DATA 成功载入 Hive 表")
            else:
                print("LOAD DATA 执行失败")
                return 1
        finally:
            cleanup_remote_file(remote_csv_path, cluster_config)
    else:
        # Beeline 本地模式
        success = execute_hive_load_data(cluster_config, csv_to_use, validation_db, metrics_summary_table, args.overwrite)
        if success:
            print("CSV 数据已通过 LOAD DATA 成功载入 Hive 表")
        else:
            print("LOAD DATA 执行失败")
            return 1

    print(f"========== 步骤 1 完成 ==========\n")

    print(f"========== 步骤 2: run-new ==========")
    # 读取 SQL 文件
    sql_file = args.sql_file
    if not sql_file:
        print("错误: 请通过 --sql-file 参数指定 SQL 文件路径")
        return 1

    # 转换为绝对路径
    if not os.path.exists(sql_file):
        print(f"SQL 文件不存在: {sql_file}")
        return 1

    # 分区日期
    data_dt = args.data_dt
    if not data_dt:
        print("错误: 请通过 --data-dt 参数指定分区日期")
        return 1

    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
    print(f"读取 SQL 文件: {sql_file}, 共 {len(statements)} 条语句")

    # 执行每条语句
    all_results = []

    for i, stmt in enumerate(statements):
        print(f"\n执行第 {i+1} 条语句...")

        actual_sql = replace_placeholder(stmt, data_dt)
        print(f"SQL: {actual_sql}")

        headers, rows = execute_hive_query(actual_sql, cluster_config)

        if not headers or not rows:
            print(f"  第 {i+1} 条语句返回空结果")
            continue

        print(f"  返回 {len(rows)} 行，列: {headers}")

        expanded_headers, expanded_rows = expand_metrics(headers, rows, 'new')
        if expanded_headers and expanded_rows:
            all_results.append((expanded_headers, expanded_rows))
            print(f"  展开为 {len(expanded_rows)} 行")

    if not all_results:
        print("没有结果可导出")
        return 1

    # 合并结果
    final_headers = all_results[0][0]
    final_rows = []
    for _, rows in all_results:
        final_rows.extend(rows)

    print(f"合并结果: {len(final_rows)} 行")

    # 通过 INSERT 写入 Hive 表
    success = execute_hive_insert(cluster_config, final_headers, final_rows, validation_db, metrics_summary_table, args.overwrite)
    if success:
        print("新集群数据已通过 INSERT 成功写入 Hive 表")
    else:
        print("INSERT 执行失败")
        return 1

    # 导出 CSV
    if args.output_csv:
        output_csv = args.output_csv
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(final_rows)
        print(f"\nCSV 已导出: {output_csv}")

    print(f"\n========== 步骤 2 完成 ==========")
    print(f"\n完成! 共处理 {len(final_rows)} 行")
    return 0


# ============== 主程序 ==============

def main():
    parser = argparse.ArgumentParser(
        description='Hive 集群指标校验工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s ingest-old --csv ../old/output/old_summary.csv --data-dt 2024-01-01
  %(prog)s run-new --sql-file output/metrics_queries.sql --data-dt 2024-01-01
  %(prog)s run-all --csv input/old_summary.csv --sql-file sql/metrics_queries.sql --data-dt 2024-01-01
        """
    )
    parser.add_argument(
        '--config',
        default='env_config.json',
        help='配置文件路径'
    )

    subparsers = parser.add_subparsers(dest='command', help='子命令')

    # ingest-old 子命令
    parser_ingest = subparsers.add_parser('ingest-old', help='从旧集群 CSV 读取并通过 LOAD DATA 写入 Hive 统一结果表（带 cluster=old 字段）')
    parser_ingest.add_argument('--csv', help='旧集群导出的 CSV 文件路径（默认从配置读取）')
    parser_ingest.add_argument('--cluster', default='new', help='目标集群')
    parser_ingest.add_argument('--overwrite', action='store_true', help='覆盖已有数据')

    # run-new 子命令
    parser_run = subparsers.add_parser('run-new', help='新集群执行相同语句并通过 LOAD DATA 写入同一张表（带 cluster=new 字段）')
    parser_run.add_argument('--sql-file', help='SQL 文件路径（默认从配置读取）')
    parser_run.add_argument('--data-dt', help='分区日期（默认从配置读取）')
    parser_run.add_argument('--cluster', default='new', help='集群名称')
    parser_run.add_argument('--overwrite', action='store_true', help='覆盖已有数据')
    parser_run.add_argument('--output-csv', help='输出 CSV 文件路径（可选）')

    # run-all 子命令
    parser_all = subparsers.add_parser('run-all', help='依次执行 ingest-old 和 run-new')
    parser_all.add_argument('--csv', help='旧集群导出的 CSV 文件路径（默认从配置读取）')
    parser_all.add_argument('--sql-file', help='SQL 文件路径（默认从配置读取）')
    parser_all.add_argument('--data-dt', help='分区日期（默认从配置读取）')
    parser_all.add_argument('--cluster', default='new', help='集群名称')
    parser_all.add_argument('--overwrite', action='store_true', help='覆盖已有数据')
    parser_all.add_argument('--output-csv', help='输出 CSV 文件路径（可选）')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == 'ingest-old':
        return cmd_ingest_old(args)
    elif args.command == 'run-new':
        return cmd_run_new(args)
    elif args.command == 'run-all':
        return cmd_run_all(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
