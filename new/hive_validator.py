#!/usr/bin/env python3
"""
Hive 集群指标校验工具
功能:
  1. ingest-old: 从旧集群 CSV 读取并写入 Hive 目标表
  2. run-new: 新集群执行相同语句，直接写入目标表
  3. compare: 新旧集群指标对比并写入对比结果表
"""

import argparse
import json
import os
import subprocess
import sys
import pandas as pd


# ============== 通用函数 ==============

def load_env_config(config_path: str = 'env_config.json') -> dict:
    """加载环境配置文件"""
    config_file = os.path.join(os.path.dirname(__file__), config_path)
    if not os.path.exists(config_file):
        return {}
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_cluster_config(config: dict, cluster: str) -> dict:
    """获取集群配置"""
    clusters = config.get('clusters', {})
    if cluster not in clusters:
        raise ValueError(f"集群配置中未找到: {cluster}")
    return clusters[cluster]


def replace_placeholder(sql: str, data_dt: str) -> str:
    """替换模板中的 {{data_dt}} 占位符"""
    return sql.replace('{{data_dt}}', data_dt)


def execute_hive_query_ssh(sql: str, ssh_config: dict) -> pd.DataFrame:
    """SSH 远程执行 Hive 查询"""
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
        sql_with_engine = "set hive.execution.engine=mr;\n" + sql
        stdout, stderr = write_process.communicate(input=sql_with_engine, timeout=30)

        if write_process.returncode != 0:
            print(f"写入远程文件失败: {stderr}")
            return None

        ssh_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "hive -e \\"source {remote_sql_file}\\""'
        result = subprocess.run(
            ssh_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

        subprocess.run(
            f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "rm -f {remote_sql_file}"',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        if result.returncode != 0:
            print(f"执行失败: {result.stderr}")
            return None

        output = result.stdout.strip()
        if not output:
            return None

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

    except Exception as e:
        print(f"执行错误: {e}")
        return None


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
        sql_with_engine = "set hive.execution.engine=mr;\n" + sql
        stdout, stderr = write_process.communicate(input=sql_with_engine, timeout=30)

        if write_process.returncode != 0:
            print(f"写入远程文件失败: {stderr}")
            return False

        ssh_cmd = f'ssh -p {ssh_port} {ssh_user}@{ssh_host} "hive -e \\"source {remote_sql_file}\\""'
        result = subprocess.run(
            ssh_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=300
        )

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


def expand_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """动态展开指标列"""
    if df is None or df.empty:
        return pd.DataFrame()

    base_columns = ['table_name', 'partition_col', 'computed_at', 'data_dt']
    metric_columns = [col for col in df.columns if col not in base_columns]

    if not metric_columns:
        return pd.DataFrame()

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


def create_summary_table(validation_db: str, table_name: str, cluster_config: dict) -> bool:
    """创建摘要表（如果不存在）"""
    full_table_name = f"{validation_db}.{table_name}"

    create_sql = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
    table_name STRING,
    partition_col STRING,
    metric_name STRING,
    value STRING,
    computed_at TIMESTAMP,
    data_dt STRING
)
STORED AS PARQUET;
"""
    print(f"创建表: {full_table_name}")
    return execute_hive_query_ssh_no_result(create_sql, cluster_config)


def insert_data_to_hive(df: pd.DataFrame, validation_db: str, table_name: str, cluster_config: dict) -> bool:
    """将 DataFrame 写入 Hive 表"""
    full_table_name = f"{validation_db}.{table_name}"

    for _, row in df.iterrows():
        table_name_val = row.get('table_name', '').replace("'", "\\'")
        partition_col_val = row.get('partition_col', '').replace("'", "\\'")
        metric_name_val = row.get('metric_name', '').replace("'", "\\'")
        value_val = row.get('value', '').replace("'", "\\'")
        computed_at_val = row.get('computed_at', '')
        data_dt_val = row.get('data_dt', '').replace("'", "\\'")

        insert_sql = f"""
INSERT INTO {full_table_name} VALUES (
    '{table_name_val}',
    '{partition_col_val}',
    '{metric_name_val}',
    '{value_val}',
    '{computed_at_val}',
    '{data_dt_val}'
);
"""
        execute_hive_query_ssh_no_result(insert_sql, cluster_config)

    print(f"成功写入 {len(df)} 行数据到 {full_table_name}")
    return True


# ============== 子命令: ingest-old ==============

def cmd_ingest_old(args):
    """从旧集群 CSV 读取并写入 Hive 目标表"""
    config = load_env_config(args.config)
    validation_db = config.get('validation_db', 'validation_db')

    cluster_config = get_cluster_config(config, args.cluster)
    print(f"使用集群: {args.cluster}")
    print(f"SSH: {cluster_config['ssh_user']}@{cluster_config['ssh_host']}")

    # 读取 CSV
    csv_path = args.csv
    if not os.path.isabs(csv_path):
        csv_path = os.path.join(os.path.dirname(__file__), csv_path)

    if not os.path.exists(csv_path):
        print(f"CSV 文件不存在: {csv_path}")
        return 1

    df = pd.read_csv(csv_path, sep='\t', dtype=str)
    print(f"读取 CSV: {csv_path}, 共 {len(df)} 行")
    print(f"列: {list(df.columns)}")

    # 验证列名
    required_columns = ['table_name', 'partition_col', 'metric_name', 'value', 'computed_at', 'data_dt']
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"警告: CSV 缺少列: {missing_cols}")

    # 创建表
    create_summary_table(validation_db, args.table, cluster_config)

    # 插入数据
    insert_data_to_hive(df, validation_db, args.table, cluster_config)

    print("\n完成!")
    return 0


# ============== 子命令: run-new ==============

def cmd_run_new(args):
    """新集群执行相同语句，直接写入目标表"""
    config = load_env_config(args.config)
    validation_db = config.get('validation_db', 'validation_db')

    cluster_config = get_cluster_config(config, args.cluster)
    print(f"使用集群: {args.cluster}")
    print(f"SSH: {cluster_config['ssh_user']}@{cluster_config['ssh_host']}")

    # 读取 SQL 文件
    sql_file = args.sql_file
    if not os.path.isabs(sql_file):
        sql_file = os.path.join(os.path.dirname(__file__), sql_file)

    if not os.path.exists(sql_file):
        print(f"SQL 文件不存在: {sql_file}")
        return 1

    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    statements = [stmt.strip() for stmt in content.split(';') if stmt.strip()]
    print(f"读取 SQL 文件: {sql_file}, 共 {len(statements)} 条语句")

    # 创建目标表
    create_summary_table(validation_db, args.table, cluster_config)

    # 执行每条语句
    all_results = []

    for i, stmt in enumerate(statements):
        print(f"\n执行第 {i+1} 条语句...")

        actual_sql = replace_placeholder(stmt, args.data_dt)
        df = execute_hive_query_ssh(actual_sql, cluster_config)

        if df is None or df.empty:
            print(f"  第 {i+1} 条语句返回空结果")
            continue

        print(f"  返回 {len(df)} 行，列: {list(df.columns)}")

        expanded_df = expand_metrics(df)
        if not expanded_df.empty:
            all_results.append(expanded_df)
            print(f"  展开为 {len(expanded_df)} 行")

    if not all_results:
        print("没有结果可导出")
        return 1

    # 合并结果
    final_df = pd.concat(all_results, ignore_index=True)

    # 写入 Hive 表
    insert_data_to_hive(final_df, validation_db, args.table, cluster_config)

    # 导出 CSV
    if args.output_csv:
        output_csv = args.output_csv
        if not os.path.isabs(output_csv):
            output_csv = os.path.join(os.path.dirname(__file__), output_csv)

        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        final_df.to_csv(output_csv, sep='\t', index=False)
        print(f"\nCSV 已导出: {output_csv}")

    print(f"\n完成! 共处理 {len(final_df)} 行")
    return 0


# ============== 子命令: compare ==============

def query_table_data(validation_db: str, table_name: str, data_dt: str, cluster_config: dict) -> pd.DataFrame:
    """查询表数据"""
    full_table_name = f"{validation_db}.{table_name}"
    query_sql = f"SELECT * FROM {full_table_name} WHERE data_dt = '{data_dt}'"

    print(f"查询表: {full_table_name}, data_dt = {data_dt}")
    df = execute_hive_query_ssh(query_sql, cluster_config)

    if df is not None and not df.empty:
        print(f"  查询到 {len(df)} 行")

    return df if df is not None else pd.DataFrame()


def compare_metrics(old_df: pd.DataFrame, new_df: pd.DataFrame, threshold: float = 0.0) -> pd.DataFrame:
    """对比新旧集群指标"""
    if old_df.empty:
        print("旧表数据为空")
        return pd.DataFrame()
    if new_df.empty:
        print("新表数据为空")
        return pd.DataFrame()

    # 标准化 value 列
    def normalize_value(val):
        if pd.isna(val) or val == '' or val == 'NULL':
            return None
        return str(val)

    old_df['value'] = old_df['value'].apply(normalize_value)
    new_df['value'] = new_df['value'].apply(normalize_value)

    # 合并两个表
    merged = pd.merge(
        old_df[['table_name', 'partition_col', 'metric_name', 'value', 'data_dt']],
        new_df[['table_name', 'partition_col', 'metric_name', 'value', 'data_dt']],
        on=['table_name', 'partition_col', 'metric_name', 'data_dt'],
        how='outer',
        suffixes=('_old', '_new')
    )

    results = []
    for _, row in merged.iterrows():
        old_value = row.get('value_old')
        new_value = row.get('value_new')

        # 判断状态
        if old_value is None and new_value is None:
            status = 'MISSING_BOTH'
            diff = None
            reason = '双方值都为空'
        elif old_value is None:
            status = 'MISSING_OLD'
            diff = None
            reason = '旧集群缺失'
        elif new_value is None:
            status = 'MISSING_NEW'
            diff = None
            reason = '新集群缺失'
        else:
            try:
                old_num = float(old_value) if old_value else 0
                new_num = float(new_value) if new_value else 0
                diff = new_num - old_num

                if abs(diff) <= threshold:
                    status = 'PASS'
                    reason = None
                else:
                    status = 'FAIL'
                    reason = f'差值 {diff} 超过阈值 {threshold}'
            except ValueError:
                if old_value == new_value:
                    status = 'PASS'
                    diff = None
                    reason = None
                else:
                    status = 'FAIL'
                    diff = None
                    reason = f'值不一致: {old_value} vs {new_value}'

        result_row = {
            'table_name': row.get('table_name', ''),
            'partition_col': row.get('partition_col', ''),
            'metric_name': row.get('metric_name', ''),
            'data_dt': row.get('data_dt', ''),
            'old_value': old_value if old_value else '',
            'new_value': new_value if new_value else '',
            'diff': str(diff) if diff is not None else '',
            'status': status,
            'reason': reason if reason else '',
            'compared_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        results.append(result_row)

    return pd.DataFrame(results)


def create_compare_table(validation_db: str, table_name: str, cluster_config: dict) -> bool:
    """创建对比结果表"""
    full_table_name = f"{validation_db}.{table_name}"

    create_sql = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
    table_name STRING,
    partition_col STRING,
    metric_name STRING,
    data_dt STRING,
    old_value STRING,
    new_value STRING,
    diff STRING,
    status STRING,
    reason STRING,
    compared_at TIMESTAMP
)
STORED AS PARQUET;
"""
    print(f"创建对比结果表: {full_table_name}")
    return execute_hive_query_ssh_no_result(create_sql, cluster_config)


def insert_compare_results(df: pd.DataFrame, validation_db: str, table_name: str, cluster_config: dict) -> bool:
    """写入对比结果到 Hive 表"""
    full_table_name = f"{validation_db}.{table_name}"

    for _, row in df.iterrows():
        table_name_val = row.get('table_name', '').replace("'", "\\'")
        partition_col_val = row.get('partition_col', '').replace("'", "\\'")
        metric_name_val = row.get('metric_name', '').replace("'", "\\'")
        data_dt_val = row.get('data_dt', '').replace("'", "\\'")
        old_value_val = row.get('old_value', '').replace("'", "\\'")
        new_value_val = row.get('new_value', '').replace("'", "\\'")
        diff_val = row.get('diff', '').replace("'", "\\'")
        status_val = row.get('status', '').replace("'", "\\'")
        reason_val = row.get('reason', '').replace("'", "\\'")
        compared_at_val = row.get('compared_at', '')

        insert_sql = f"""
INSERT INTO {full_table_name} VALUES (
    '{table_name_val}',
    '{partition_col_val}',
    '{metric_name_val}',
    '{data_dt_val}',
    '{old_value_val}',
    '{new_value_val}',
    '{diff_val}',
    '{status_val}',
    '{reason_val}',
    '{compared_at_val}'
);
"""
        execute_hive_query_ssh_no_result(insert_sql, cluster_config)

    print(f"成功写入 {len(df)} 行对比结果到 {full_table_name}")
    return True


def cmd_compare(args):
    """新旧集群指标对比并写入对比结果表"""
    config = load_env_config(args.config)
    validation_db = config.get('validation_db', 'validation_db')

    cluster_config = get_cluster_config(config, args.cluster)
    print(f"使用集群: {args.cluster}")
    print(f"SSH: {cluster_config['ssh_user']}@{cluster_config['ssh_host']}")
    print(f"对比阈值: {args.threshold}")

    # 查询旧表数据
    print("\n=== 查询旧集群数据 ===")
    old_df = query_table_data(validation_db, args.old_table, args.data_dt, cluster_config)

    # 查询新表数据
    print("\n=== 查询新集群数据 ===")
    new_df = query_table_data(validation_db, args.new_table, args.data_dt, cluster_config)

    # 对比
    print("\n=== 对比指标 ===")
    compare_df = compare_metrics(old_df, new_df, args.threshold)

    if compare_df.empty:
        print("没有可对比的数据")
        return 1

    # 统计结果
    status_counts = compare_df['status'].value_counts()
    print("\n对比结果统计:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")

    # 创建对比结果表
    create_compare_table(validation_db, args.compare_table, cluster_config)

    # 写入对比结果
    insert_compare_results(compare_df, validation_db, args.compare_table, cluster_config)

    # 导出 CSV
    if args.output_csv:
        output_csv = args.output_csv
        if not os.path.isabs(output_csv):
            output_csv = os.path.join(os.path.dirname(__file__), output_csv)

        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        compare_df.to_csv(output_csv, sep='\t', index=False)
        print(f"\nCSV 已导出: {output_csv}")

    print("\n完成!")
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
  %(prog)s compare --data-dt 2024-01-01
        """
    )
    parser.add_argument(
        '--config',
        default='env_config.json',
        help='配置文件路径'
    )

    subparsers = parser.add_subparsers(dest='command', help='子命令')

    # ingest-old 子命令
    parser_ingest = subparsers.add_parser('ingest-old', help='从旧集群 CSV 读取并写入 Hive 目标表')
    parser_ingest.add_argument('--csv', required=True, help='旧集群导出的 CSV 文件路径')
    parser_ingest.add_argument('--table', default='old_summary', help='目标表名（不含库名）')
    parser_ingest.add_argument('--cluster', default='new', help='目标集群')

    # run-new 子命令
    parser_run = subparsers.add_parser('run-new', help='新集群执行相同语句，直接写入目标表')
    parser_run.add_argument('--sql-file', default='output/metrics_queries.sql', help='SQL 文件路径')
    parser_run.add_argument('--data-dt', required=True, help='分区日期')
    parser_run.add_argument('--table', default='new_summary', help='目标表名（不含库名）')
    parser_run.add_argument('--cluster', default='new', help='集群名称')
    parser_run.add_argument('--output-csv', help='输出 CSV 文件路径（可选）')

    # compare 子命令
    parser_cmp = subparsers.add_parser('compare', help='新旧集群指标对比并写入对比结果表')
    parser_cmp.add_argument('--data-dt', required=True, help='分区日期')
    parser_cmp.add_argument('--old-table', default='old_summary', help='旧集群结果表名')
    parser_cmp.add_argument('--new-table', default='new_summary', help='新集群结果表名')
    parser_cmp.add_argument('--compare-table', default='compare_result', help='对比结果表名')
    parser_cmp.add_argument('--cluster', default='new', help='集群名称')
    parser_cmp.add_argument('--threshold', type=float, default=0.0, help='数值对比阈值')
    parser_cmp.add_argument('--output-csv', help='输出 CSV 文件路径（可选）')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == 'ingest-old':
        return cmd_ingest_old(args)
    elif args.command == 'run-new':
        return cmd_run_new(args)
    elif args.command == 'compare':
        return cmd_compare(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
