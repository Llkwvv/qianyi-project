#!/usr/bin/env python3
"""
Generate per-cluster metric artifacts by executing Hive SQL in parallel.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from metrics_artifact_common import (
    HIVE_DRIVER_AVAILABLE,
    build_default_artifact_path,
    close_hive_connection_pool,
    execute_hive_sql,
    get_artifact_dir,
    get_cluster_config,
    get_runtime_config,
    load_env_config,
    load_sql_tasks,
    normalize_metric_rows,
    write_artifact_tsv,
)


def run_task(task_index, cluster_name, cluster_config, sql, timeout_sec, max_retries):
    """Run one SQL task with retries and return expanded rows."""
    attempt = 0
    last_error = None
    max_attempts = max_retries + 1

    while attempt < max_attempts:
        try:
            result_row = execute_hive_sql(cluster_name, cluster_config, sql, timeout_sec)
            return task_index, normalize_metric_rows(cluster_name, result_row)
        except Exception as exc:
            last_error = exc
            attempt += 1
            if attempt >= max_attempts:
                break

    raise RuntimeError(
        '任务 {0} 在重试 {1} 次后仍失败: {2}'.format(
            task_index, max_retries, last_error
        )
    )


def main():
    parser = argparse.ArgumentParser(description='执行 Hive SQL 并生成指标中间结果文件')
    parser.add_argument('--sql-file', required=True, help='SQL 文件路径')
    parser.add_argument('--data-dt', required=True, help='分区日期，如 2024-01-01')
    parser.add_argument('--cluster', required=True, help='集群名称，如 old 或 new')
    parser.add_argument('--output-file', help='输出 TSV 文件路径')
    parser.add_argument('--use-beeline', action='store_true',
                        help='强制使用 beeline 模式，不使用连接池')
    args = parser.parse_args()

    config = load_env_config()
    runtime = get_runtime_config(config)
    cluster_config = get_cluster_config(config, args.cluster)

    # 强制使用 beeline 模式
    if args.use_beeline:
        cluster_config['use_tez'] = False

    if not args.output_file:
        artifact_dir = get_artifact_dir(config)
        args.output_file = build_default_artifact_path(artifact_dir, args.data_dt, args.cluster)

    tasks = load_sql_tasks(args.sql_file, args.data_dt)
    if not tasks:
        raise ValueError('SQL 文件中未解析到可执行语句: {0}'.format(args.sql_file))

    print('集群: {0}'.format(args.cluster))
    print('SQL 数量: {0}'.format(len(tasks)))
    print('输出文件: {0}'.format(args.output_file))
    print('执行引擎: {0}'.format(
        'beeline' if args.use_beeline or not HIVE_DRIVER_AVAILABLE else 'Tez连接池'))

    try:
        results_by_index = {}
        with ThreadPoolExecutor(max_workers=runtime['max_workers']) as executor:
            future_map = {}
            for index, sql in enumerate(tasks):
                future = executor.submit(
                    run_task,
                    index,
                    args.cluster,
                    cluster_config,
                    sql,
                    runtime['query_timeout_sec'],
                    runtime['max_retries'],
                )
                future_map[future] = index

            for future in as_completed(future_map):
                task_index, rows = future.result()
                results_by_index[task_index] = rows
                print('任务完成: {0}'.format(task_index + 1))

        all_rows = []
        for index in range(len(tasks)):
            all_rows.extend(results_by_index[index])

        write_artifact_tsv(args.output_file, all_rows)
        print('中间结果已生成: {0}'.format(args.output_file))
        print('共输出 {0} 条指标记录'.format(len(all_rows)))
    finally:
        # 确保连接池总是被关闭
        close_hive_connection_pool()
        print('连接池已关闭')


if __name__ == '__main__':
    main()
