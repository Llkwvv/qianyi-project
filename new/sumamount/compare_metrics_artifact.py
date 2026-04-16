#!/usr/bin/env python3
"""
Compare old/new metric artifacts and emit a Hive-ready CSV file.
"""

import argparse
import datetime
import os
import time
from decimal import Decimal, InvalidOperation

from metrics_artifact_common import (
    build_default_artifact_path,
    build_default_compare_path,
    get_artifact_dir,
    get_runtime_config,
    load_env_config,
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


def parse_int_or_none(value):
    """Parse an integer-like value, allowing NULL/empty values."""
    if value is None:
        return None

    stripped = str(value).strip()
    if not stripped or stripped.upper() == 'NULL':
        return None

    try:
        decimal_value = Decimal(stripped)
    except InvalidOperation:
        raise ValueError('值不是合法数字: {0}'.format(value))

    if decimal_value != decimal_value.to_integral_value():
        raise ValueError('值无法安全写入 BIGINT: {0}'.format(value))

    return int(decimal_value)


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
        old_value = parse_int_or_none(old_row['value'])
        new_value = parse_int_or_none(new_row['value'])
        diff_value = (new_value or 0) - (old_value or 0)

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


def main():
    parser = argparse.ArgumentParser(description='对比新旧集群的指标中间结果文件')
    parser.add_argument('--data-dt', required=True, help='分区日期，如 2024-01-01')
    parser.add_argument('--old-artifact', help='旧集群 TSV 文件路径')
    parser.add_argument('--new-artifact', help='新集群 TSV 文件路径')
    parser.add_argument('--output-file', help='输出结果 CSV 路径')
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


if __name__ == '__main__':
    main()
