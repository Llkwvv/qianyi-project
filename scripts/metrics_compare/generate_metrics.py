#!/usr/bin/env python3
"""
Generate per-cluster metric artifacts by executing Hive SQL in parallel.
"""

import argparse
import csv
import json
import os
import re
import shlex
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

# 尝试导入 pyhive，如果失败则使用 beeline 模式
try:
    from pyhive import hive
    HIVE_DRIVER_AVAILABLE = True
except ImportError:
    HIVE_DRIVER_AVAILABLE = False


class HiveConnectionPool:
    """Hive 连接池，支持连接复用和 Tez 引擎"""

    _conn_counter = 0  # 连接计数器，用于生成唯一ID

    def __init__(self, host, port, username, database='default',
                 max_connections=5, use_tez=True):
        self.host = host
        self.port = port
        self.username = username
        self.database = database
        self.max_connections = max_connections
        self.use_tez = use_tez
        self.pool = []
        self.lock = threading.Lock()
        self._init_pool()

    def _init_pool(self):
        """预创建指定数量的连接"""
        for _ in range(self.max_connections):
            conn = self._create_connection()
            self.pool.append(conn)

    def _create_connection(self):
        """创建一个新的 Hive 连接"""
        import socket
        HiveConnectionPool._conn_counter += 1
        conn_id = HiveConnectionPool._conn_counter

        print('[连接池] 正在创建连接 #{0} (host={1}:{2})...'.format(
            conn_id, self.host, self.port))

        conn = hive.Connection(
            host=self.host,
            port=self.port,
            username=self.username,
            database=self.database,
            auth='NONE'
        )

        # 设置执行引擎
        cursor = conn.cursor()
        if self.use_tez:
            cursor.execute("SET hive.execution.engine=tez")
        else:
            cursor.execute("SET hive.execution.engine=mr")
        cursor.close()
        conn._conn_id = conn_id
        print('[连接池] 创建新连接 #{0} 成功'.format(conn_id))
        return conn

    def get_connection(self):
        """从连接池获取一个连接"""
        import threading
        task_name = threading.current_thread().name
        with self.lock:
            if self.pool:
                conn = self.pool.pop()
                # 检查连接是否仍然有效
                try:
                    conn.ping()
                except Exception:
                    # 连接失效，重新创建
                    conn = self._create_connection()
                print('[连接池] 任务 {0} 复用连接 #{1} (池中剩余: {2}, 总连接数: {3})'.format(
                    task_name, conn._conn_id, len(self.pool), self.max_connections))
                return conn
            else:
                # 池已空，创建新连接
                conn = self._create_connection()
                print('[连接池] 任务 {0} 使用新连接 #{1} (池为空, 总连接数: {2})'.format(
                    task_name, conn._conn_id, self.max_connections))
                return conn

    def return_connection(self, conn):
        """归还连接到池中"""
        import threading
        task_name = threading.current_thread().name
        with self.lock:
            if len(self.pool) < self.max_connections:
                self.pool.append(conn)
                print('[连接池] 任务 {0} 归还连接 #{1} (池中现有: {2})'.format(
                    task_name, conn._conn_id, len(self.pool)))
            else:
                conn.close()
                print('[连接池] 任务 {0} 关闭连接 #{1}'.format(
                    task_name, conn._conn_id))

    def close_all(self):
        """关闭所有连接"""
        with self.lock:
            for conn in self.pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self.pool.clear()


# 全局连接池实例
_global_pool = None
_pool_lock = threading.Lock()


def get_hive_connection_pool(cluster_config, max_connections=4, use_tez=True):
    """获取或创建全局 Hive 连接池"""
    global _global_pool

    # 解析 beeline_url 获取 host 和 port
    beeline_url = cluster_config.get('beeline_url', '')
    # jdbc:hive2://192.168.10.102:10000/
    host = cluster_config.get('hive_host') or 'hadoop102'
    port = cluster_config.get('hive_port') or 10000

    # 从 URL 中解析 host 和 port
    if 'hive2://' in beeline_url:
        parts = beeline_url.split('hive2://')[1].split('/')[0].split(':')
        if len(parts) >= 1:
            host = parts[0]
        if len(parts) >= 2:
            port = int(parts[1])

    username = cluster_config.get('username', 'atguigu')
    database = cluster_config.get('database', 'default')

    with _pool_lock:
        if _global_pool is None:
            _global_pool = HiveConnectionPool(
                host=host,
                port=port,
                username=username,
                database=database,
                max_connections=max_connections,
                use_tez=use_tez
            )
    return _global_pool


def close_hive_connection_pool():
    """关闭全局连接池"""
    global _global_pool
    with _pool_lock:
        if _global_pool:
            _global_pool.close_all()
            _global_pool = None


ARTIFACT_FIELDNAMES = [
    'cluster',
    'table_name',
    'partition_col',
    'metric_name',
    'value',
    'etl_tm',
    'data_dt',
]

BASE_RESULT_COLUMNS = ['table_name', 'data_dt', 'partition_col']
LEGACY_TIMESTAMP_COLUMN = 'computed_at'
RESULT_TIMESTAMP_COLUMN = 'etl_tm'

BEELINE_PROMPT_PREFIX_RE = re.compile(
    r"^(?:(?:\.\s*)+>|(?:\d+:\s*jdbc:[^>]+>)|(?:jdbc:[^>]+>))\s*"
)


def load_env_config(config_path='env_config.json'):
    """Load the config file from the current script directory."""
    config_file = os.path.join(os.path.dirname(__file__), config_path)
    if not os.path.exists(config_file):
        return {}

    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_artifact_dir(config):
    """Return the base directory used for artifacts."""
    return config.get('artifact_dir') or config.get('csv_dir') or 'output'


def get_runtime_config(config):
    """Return runtime settings with safe defaults."""
    runtime = config.get('runtime', {}).copy()
    runtime.setdefault('max_workers', 4)
    runtime.setdefault('max_retries', 0)
    runtime.setdefault('query_timeout_sec', 1800)
    runtime.setdefault('poll_interval_sec', 300)
    runtime.setdefault('wait_timeout_sec', 14400)
    return runtime


def get_cluster_config(config, cluster_name):
    """Return a named cluster config or raise a helpful error."""
    clusters = config.get('clusters', {})
    cluster_config = clusters.get(cluster_name)
    if not cluster_config:
        raise ValueError('未在 env_config.json 中找到集群配置: {0}'.format(cluster_name))
    return cluster_config


def build_default_artifact_path(base_dir, data_dt, cluster_name):
    """Build the default per-cluster artifact path."""
    return os.path.join(base_dir, data_dt, '{0}_metrics.tsv'.format(cluster_name))


def ensure_parent_dir(file_path):
    """Create the parent directory when needed."""
    parent_dir = os.path.dirname(file_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)


def split_sql_statements(sql_text):
    """Split multi-statement SQL text on semicolons outside single quotes."""
    statements = []
    current = []
    in_single_quote = False
    prev_char = ''

    for char in sql_text:
        if char == "'" and prev_char != '\\':
            in_single_quote = not in_single_quote

        if char == ';' and not in_single_quote:
            statement = ''.join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)

        prev_char = char

    trailing = ''.join(current).strip()
    if trailing:
        statements.append(trailing)

    return statements


def load_sql_tasks(sql_file, data_dt):
    """Read SQL tasks from file and render the date placeholder."""
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_text = f.read()

    statements = split_sql_statements(sql_text)
    rendered = []
    for statement in statements:
        rendered.append(statement.replace('{{data_dt}}', data_dt))
    return rendered


def build_beeline_command(cluster_config, sql):
    """Build a beeline command, optionally wrapped by ssh."""
    beeline_cmd = cluster_config.get('beeline_cmd', 'beeline')
    beeline_url = cluster_config.get('beeline_url')
    if not beeline_url:
        raise ValueError('集群配置缺少 beeline_url')

    use_tez = cluster_config.get('use_tez', True)

    base_cmd = [
        beeline_cmd,
        '-u', beeline_url,
        '--outputformat=tsv',
        '--showHeader=true',
        '--silent=true',
    ]

    # 如果不使用tez，先设置mr引擎
    if not use_tez:
        base_cmd.extend(['-e', 'set hive.execution.engine=mr;'])

    base_cmd.extend(['-e', sql])

    username = cluster_config.get('username')
    if username:
        base_cmd.extend(['-n', username])

    password = cluster_config.get('password')
    if password:
        base_cmd.extend(['-p', password])

    extra_args = cluster_config.get('extra_args', [])
    if extra_args:
        base_cmd.extend(extra_args)

    if not cluster_config.get('use_ssh'):
        return base_cmd

    ssh_host = cluster_config.get('ssh_host')
    ssh_user = cluster_config.get('ssh_user')
    ssh_port = cluster_config.get('ssh_port', 22)
    if not ssh_host or not ssh_user:
        raise ValueError('SSH 模式缺少 ssh_host 或 ssh_user 配置')

    remote_cmd = ' '.join(shlex.quote(part) for part in base_cmd)
    return [
        'ssh',
        '-p', str(ssh_port),
        '{0}@{1}'.format(ssh_user, ssh_host),
        remote_cmd,
    ]


def parse_beeline_tsv(stdout_text):
    """Parse beeline tsv2 output into one header row and data rows."""
    tsv_lines = []

    for raw_line in stdout_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith('WARN') or line.startswith('INFO') or line.startswith('SLF4J'):
            continue
        # Skip lines that look like table borders
        if line.startswith('+') or line.startswith('|'):
            continue
        normalized_line = BEELINE_PROMPT_PREFIX_RE.sub('', raw_line, count=1).strip()
        if normalized_line:
            tsv_lines.append(normalized_line)

    if not tsv_lines:
        raise ValueError('未能从 beeline 输出中解析到 TSV 数据')

    parsed_lines = []
    for line in tsv_lines:
        if '\t' in line:
            # Tab-separated format (multi-column)
            row = next(csv.reader([line], delimiter='\t'))
            parsed_lines.append([normalize_beeline_field(value) for value in row])
        else:
            # Single column format (no tabs)
            parsed_lines.append([normalize_beeline_field(line)])

    # Handle case where only data rows exist (no header)
    if len(parsed_lines) == 1:
        # Single row, treat as data with single column name
        return ['col1'], [parsed_lines[0]]

    header = parsed_lines[0]
    data_rows = []
    for row in parsed_lines[1:]:
        if len(row) == len(header):
            data_rows.append(row)
        elif len(row) == 1 and len(header) > 1:
            # Single value, pad to match header length
            data_rows.append(row * len(header))

    return header, data_rows


def normalize_beeline_field(value):
    """Normalize one beeline TSV field for easier downstream handling."""
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == "'" and stripped[-1] == "'":
        return stripped[1:-1].replace("\\'", "'")
    return stripped


def execute_hive_sql(cluster_name, cluster_config, sql, timeout_sec):
    """Execute one Hive SQL and return exactly one row as a dict.

    仅使用 Tez 连接池模式，不支持回退。
    """
    if not HIVE_DRIVER_AVAILABLE:
        raise RuntimeError('pyhive 未安装，无法使用 Tez 连接池模式')

    return _execute_hive_sql_with_pool(cluster_name, cluster_config, sql, timeout_sec)


def _execute_hive_sql_with_pool(cluster_name, cluster_config, sql, timeout_sec):
    """使用连接池执行 Hive SQL（带超时）"""
    import threading
    import socket
    # 从配置中获取最大并发数
    max_workers = cluster_config.get('max_workers', 4)
    use_tez = cluster_config.get('use_tez', True)

    # 获取或创建连接池
    pool = get_hive_connection_pool(cluster_config, max_workers, use_tez)

    # 从连接池获取连接
    conn = pool.get_connection()
    conn_id = conn._conn_id

    # 设置 socket 超时（如果连接有 socket 属性）
    socket_attr = getattr(conn, '_socket', None)
    if socket_attr is not None:
        socket_attr.settimeout(timeout_sec)

    cursor = conn.cursor()

    task_name = threading.current_thread().name
    # 截取SQL前100个字符作为显示
    sql_preview = sql.strip()[:100].replace('\n', ' ')
    print('[任务 {0}] 开始执行 (连接 #{1}, Tez引擎: {2}, 超时: {3}秒): {4}...'.format(
        task_name, conn_id, use_tez, timeout_sec, sql_preview))

    try:
        # 设置执行引擎为 Tez（如果需要）
        if use_tez:
            cursor.execute("SET hive.execution.engine=tez")

        # 打印将要执行的 SQL
        print('[任务 {0}] 提交 SQL 到 Hive...'.format(task_name))

        # 执行 SQL（带超时）
        cursor.execute(sql)

        # 获取执行状态
        print('[任务 {0}] SQL 执行完成，正在获取结果...'.format(task_name))

        # 获取结果
        if cursor.description:
            # 有返回结果的查询
            result = cursor.fetchall()
            if len(result) != 1:
                raise ValueError(
                    '集群 {0} 的 SQL 结果必须是 1 行，实际为 {1} 行'.format(
                        cluster_name, len(result)
                    )
                )
            # 获取列名
            columns = [desc[0] for desc in cursor.description]
            print('[任务 {0}] 成功获取结果: {1}'.format(task_name, dict(zip(columns, result[0]))))
            return dict(zip(columns, result[0]))
        else:
            # DDL/DML 语句
            raise ValueError(
                '集群 {0} 的 SQL 查询没有返回结果'.format(cluster_name)
            )

    except socket.timeout:
        print('[任务 {0}] 执行超时 ({1}秒)'.format(task_name, timeout_sec))
        raise RuntimeError(
            '集群 {0} 执行超时 (超过 {1} 秒)'.format(cluster_name, timeout_sec)
        )
    except Exception as e:
        print('[任务 {0}] 执行失败: {1}'.format(task_name, str(e)))
        raise RuntimeError(
            '集群 {0} 执行失败: {1}'.format(cluster_name, str(e))
        )
    finally:
        cursor.close()
        pool.return_connection(conn)
        print('[任务 {0}] 连接已归还池 (连接 #{1})'.format(task_name, conn_id))


def _execute_hive_sql_with_beeline(cluster_name, cluster_config, sql, timeout_sec):
    """使用 beeline 命令执行 Hive SQL（回退模式）"""
    cmd = build_beeline_command(cluster_config, sql)
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_sec,
    )

    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8', errors='replace').strip()
        raise RuntimeError(
            '集群 {0} 执行失败，返回码 {1}: {2}'.format(
                cluster_name, result.returncode, stderr or '无错误输出'
            )
        )

    stdout_text = result.stdout.decode('utf-8', errors='replace')
    header, data_rows = parse_beeline_tsv(stdout_text)
    if len(data_rows) != 1:
        raise ValueError(
            '集群 {0} 的 SQL 结果必须是 1 行，实际为 {1} 行'.format(
                cluster_name, len(data_rows)
            )
        )

    return dict(zip(header, data_rows[0]))


def normalize_metric_rows(cluster_name, result_row):
    """Expand one wide result row into metric artifact rows."""
    missing_columns = [col for col in BASE_RESULT_COLUMNS if col not in result_row]
    if missing_columns:
        raise ValueError('查询结果缺少基础列: {0}'.format(', '.join(missing_columns)))

    timestamp_value = result_row.get(RESULT_TIMESTAMP_COLUMN)
    if timestamp_value is None:
        timestamp_value = result_row.get(LEGACY_TIMESTAMP_COLUMN)
    if timestamp_value is None:
        raise ValueError(
            '查询结果缺少时间列: {0} 或 {1}'.format(
                RESULT_TIMESTAMP_COLUMN, LEGACY_TIMESTAMP_COLUMN
            )
        )

    rows = []
    for column_name, value in result_row.items():
        if column_name in BASE_RESULT_COLUMNS:
            continue
        if column_name in (RESULT_TIMESTAMP_COLUMN, LEGACY_TIMESTAMP_COLUMN):
            continue
        rows.append({
            'cluster': cluster_name,
            'table_name': result_row['table_name'],
            'partition_col': result_row['partition_col'],
            'metric_name': column_name,
            'value': value,
            'etl_tm': timestamp_value,
            'data_dt': result_row['data_dt'],
        })

    return rows


def atomic_write_delimited(file_path, fieldnames, rows, delimiter, write_header):
    """Atomically write a delimited file."""
    ensure_parent_dir(file_path)
    parent_dir = os.path.dirname(file_path) or '.'
    fd, temp_path = tempfile.mkstemp(prefix='.tmp_', dir=parent_dir)

    try:
        with os.fdopen(fd, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                delimiter=delimiter,
                lineterminator='\n',
            )
            if write_header:
                writer.writeheader()
            for row in rows:
                writer.writerow(row)
        os.replace(temp_path, file_path)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise


def write_artifact_tsv(file_path, rows):
    """Write the metric artifact with a header row."""
    atomic_write_delimited(file_path, ARTIFACT_FIELDNAMES, rows, '\t', True)


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
