#!/usr/bin/env python3
"""
Compare old/new metric artifacts and emit a Hive-ready CSV file.
"""

import argparse
import datetime
import os
import subprocess
import time

from metrics_artifact_common import (
    build_beeline_command,
    build_default_artifact_path,
    build_default_compare_path,
    decimal_to_string,
    get_artifact_dir,
    get_runtime_config,
    load_env_config,
    normalize_value_string,
    parse_decimal_or_none,
    parse_beeline_tsv,
    read_artifact_tsv,
    split_table_name,
    write_compare_csv,
)


def wait_for_artifacts(file_paths, timeout_sec, poll_interval_sec):
    """Wait until all file paths exist or raise a timeout error."""
    deadline = time.time() + timeout_sec

    while True:
        missing = [path for path in file_paths if not os.path.exists(path)]
        if not missing:
            return

        if time.time() >= deadline:
            raise TimeoutError('等待中间结果文件超时，缺失: {0}'.format(', '.join(missing)))

        print('等待文件: {0}'.format(', '.join(missing)))
        time.sleep(poll_interval_sec)


def build_lookup(rows, label):
    """Build a unique-key lookup for one cluster artifact."""
    lookup = {}
    for row in rows:
        key = (
            row['table_name'],
            row['partition_col'],
            row['metric_name'],
            row['data_dt'],
        )
        if key in lookup:
            raise ValueError('{0} 中存在重复主键: {1}'.format(label, key))
        lookup[key] = row
    return lookup


def compare_artifacts(old_rows, new_rows):
    """Compare two metric artifacts with strict key matching."""
    old_lookup = build_lookup(old_rows, 'old_artifact')
    new_lookup = build_lookup(new_rows, 'new_artifact')

    old_keys = set(old_lookup.keys())
    new_keys = set(new_lookup.keys())
    if old_keys != new_keys:
        missing_in_new = sorted(old_keys - new_keys)
        missing_in_old = sorted(new_keys - old_keys)
        raise ValueError(
            '新旧结果主键不一致，missing_in_new={0}, missing_in_old={1}'.format(
                missing_in_new, missing_in_old
            )
        )

    etl_tm = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    compare_rows = []

    for key in sorted(old_keys):
        old_row = old_lookup[key]
        new_row = new_lookup[key]
        database_name, table_name = split_table_name(old_row['table_name'])
        old_value = normalize_value_string(old_row['value'])
        new_value = normalize_value_string(new_row['value'])
        old_decimal = parse_decimal_or_none(old_row['value'])
        new_decimal = parse_decimal_or_none(new_row['value'])

        if old_decimal is None and new_decimal is None:
            diff_value = None
        else:
            diff_value = decimal_to_string((new_decimal or 0) - (old_decimal or 0))

        compare_rows.append({
            'database_name': database_name,
            'table_name': table_name,
            'partition_name': old_row['partition_col'],
            'metric_name': old_row['metric_name'],
            'old_value': old_value,
            'new_value': new_value,
            'diff_value': diff_value,
            'etl_tm': etl_tm,
        })

    return compare_rows


def load_table_sql_template(template_path, database_name, table_name):
    """Load a CREATE TABLE template and render database/table placeholders."""
    with open(template_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    return (
        sql.replace('{{database}}', database_name)
        .replace('{{table}}', table_name)
    )


def run_beeline_sql(hive_config, sql):
    """Run one beeline SQL and return the completed subprocess result."""
    cmd = build_beeline_command(hive_config, sql)
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=hive_config.get('timeout_sec', 1800),
    )


def ensure_hive_table(hive_config):
    """Create the Hive table when it does not already exist."""
    template_path = hive_config.get('create_table_sql') or os.path.join(
        os.path.dirname(__file__), 'hive_table.sql'
    )
    create_sql = load_table_sql_template(
        template_path,
        hive_config['database'],
        hive_config['table'],
    )
    result = run_beeline_sql(hive_config, create_sql)
    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8', errors='replace').strip()
        raise RuntimeError('建 Hive 表失败: {0}'.format(stderr or '无错误输出'))


def build_hdfs_target_dir(base_dir, data_dt):
    """Build the HDFS directory used for the comparison CSV."""
    return '{0}/{1}'.format(base_dir.rstrip('/'), data_dt)


def run_hdfs_command(cmd):
    """Run one HDFS CLI command and raise on failure."""
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=1800,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8', errors='replace').strip()
        raise RuntimeError(
            'HDFS 命令失败: {0}: {1}'.format(' '.join(cmd), stderr or '无错误输出')
        )
    return result


def load_compare_csv_to_hive(csv_file, hive_config, data_dt):
    """通过INSERT方式将对比CSV写入Hive表"""
    if not os.path.exists(csv_file):
        raise ValueError('对比结果文件不存在: {0}'.format(csv_file))

    ensure_hive_table(hive_config)

    hive_database = hive_config['database']
    hive_table = hive_config['table']

    # 读取CSV文件
    print('读取CSV文件: {0}'.format(csv_file))
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print('CSV文件没有数据')
        return 0

    print('共 {0} 条数据需要写入Hive'.format(len(rows)))

    # 批量插入数据
    batch_size = 100
    total_rows = len(rows)

    for i in range(0, total_rows, batch_size):
        batch = rows[i:i+batch_size]
        values_list = []

        for row in batch:
            # 处理NULL值
            def to_value(val):
                if val is None or val == '':
                    return 'NULL'
                return "'{0}'".format(val.replace("'", "''"))

            value = "({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})".format(
                to_value(row.get('database_name')),
                to_value(row.get('table_name')),
                to_value(row.get('partition_name')),
                to_value(row.get('metric_name')),
                to_value(row.get('old_value')),
                to_value(row.get('new_value')),
                to_value(row.get('diff_value')),
                to_value(data_dt)
            )
            values_list.append(value)

        insert_sql = "INSERT INTO TABLE {0}.{1} VALUES {2}".format(
            hive_database,
            hive_table,
            ','.join(values_list)
        )

        print('正在写入第 {0} 批数据 ({1} 条)...'.format(i//batch_size + 1, len(batch)))
        load_result = run_beeline_sql(hive_config, insert_sql)
        if load_result.returncode != 0:
            stderr = load_result.stderr.decode('utf-8', errors='replace').strip()
            raise RuntimeError('INSERT 失败: {0}'.format(stderr or '无错误输出'))

    print('数据已写入 Hive')

    # Verify
    count_sql = """
SELECT count(1) as row_count
FROM {database_name}.{table_name}
WHERE data_dt = '{data_dt}'
""".format(
        database_name=hive_database,
        table_name=hive_table,
        data_dt=data_dt,
    )
    count_result = run_beeline_sql(hive_config, count_sql)
    if count_result.returncode != 0:
        stderr = count_result.stderr.decode('utf-8', errors='replace').strip()
        raise RuntimeError('校验 Hive 表失败: {0}'.format(stderr or '无错误输出'))

    header, rows = parse_beeline_tsv(count_result.stdout.decode('utf-8', errors='replace'))
    if header != ['row_count'] or len(rows) != 1:
        raise RuntimeError('校验 Hive 表返回异常: header={0}, rows={1}'.format(header, rows))

    return rows[0][0]


def main():
    parser = argparse.ArgumentParser(description='对比新旧集群的指标中间结果文件')
    parser.add_argument('--data-dt', required=True, help='分区日期，如 2024-01-01')
    parser.add_argument('--old-artifact', help='旧集群 TSV 文件路径')
    parser.add_argument('--new-artifact', help='新集群 TSV 文件路径')
    parser.add_argument('--output-file', help='输出结果 CSV 路径')
    parser.add_argument('--hive-table', help='Hive 表名，格式 database.table 或 table')
    parser.add_argument('--hdfs-dir', help='HDFS 落地目录，默认读配置')
    args = parser.parse_args()

    config = load_env_config()
    runtime = get_runtime_config(config)
    artifact_dir = get_artifact_dir(config)

    if not args.old_artifact:
        args.old_artifact = build_default_artifact_path(artifact_dir, args.data_dt, 'old')
    if not args.new_artifact:
        args.new_artifact = build_default_artifact_path(artifact_dir, args.data_dt, 'new')
    if not args.output_file:
        args.output_file = build_default_compare_path(artifact_dir, args.data_dt)

    print('old 文件: {0}'.format(args.old_artifact))
    print('new 文件: {0}'.format(args.new_artifact))
    print('输出文件: {0}'.format(args.output_file))

    wait_for_artifacts(
        [args.old_artifact, args.new_artifact],
        runtime['wait_timeout_sec'],
        runtime['poll_interval_sec'],
    )

    old_rows = read_artifact_tsv(args.old_artifact)
    new_rows = read_artifact_tsv(args.new_artifact)
    if not old_rows:
        raise ValueError('old 中间结果为空: {0}'.format(args.old_artifact))
    if not new_rows:
        raise ValueError('new 中间结果为空: {0}'.format(args.new_artifact))

    compare_rows = compare_artifacts(old_rows, new_rows)
    write_compare_csv(args.output_file, compare_rows)

    print('对比完成: {0}'.format(args.output_file))
    print('共输出 {0} 条结果'.format(len(compare_rows)))

    hive_config = config.get('hive', {}).copy()
    hive_config.setdefault('database', 'default')
    hive_config.setdefault('table', 'metric_comparison')
    hive_config.setdefault('beeline_cmd', '/home/lkw/apache-hive-3.1.3-bin/bin/beeline')
    hive_config.setdefault('beeline_url', 'jdbc:hive2://172.20.10.6:10000/')
    hive_config.setdefault('username', 'atguigu')
    hive_config.setdefault('use_ssh', False)
    hive_config.setdefault('hdfs_dir', '/home/lkw/qianyi-project/old/sumamount/hdfs_tmp')

    if args.hive_table:
        if '.' in args.hive_table:
            hive_config['database'], hive_config['table'] = args.hive_table.split('.', 1)
        else:
            hive_config['table'] = args.hive_table
    if args.hdfs_dir:
        hive_config['hdfs_dir'] = args.hdfs_dir

    print(
        '准备写入 Hive: {0}.{1}, 分区 data_dt={2}'.format(
            hive_config['database'],
            hive_config['table'],
            args.data_dt,
        )
    )
    loaded_rows = load_compare_csv_to_hive(args.output_file, hive_config, args.data_dt)
    print(
        'Hive 写入完成: {0}.{1}, 分区 data_dt={2}, 行数 {3}'.format(
            hive_config['database'],
            hive_config['table'],
            args.data_dt,
            loaded_rows,
        )
    )


if __name__ == '__main__':
    main()
