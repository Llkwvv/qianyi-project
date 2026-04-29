#!/usr/bin/env python3
"""
Generate per-cluster metric artifacts by executing Hive SQL.

Default PyHive mode uses the async scheduler:
- submit with ``async_=True``
- return immediately after submit
- keep filling in-flight slots until the configured limit
- fetch and write results only after a task finishes
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
from hive_async_scheduler import (
    DelimitedRowWriter,
    HiveAsyncScheduler,
    build_scheduler_runtime,
    resolve_cluster_connection,
)

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
                 max_connections=5):
        self.host = host
        self.port = port
        self.username = username
        self.database = database
        self.max_connections = max_connections
        self.pool = []
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        # 预创建连接池
        for _ in range(max_connections):
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

        # 设置执行引擎为 tez
        cursor = conn.cursor()
        cursor.execute("SET hive.execution.engine=tez")
        cursor.close()
        conn._conn_id = conn_id
        print('[连接池] 创建新连接 #{0} 成功'.format(conn_id))
        return conn

    def get_connection(self, wait_timeout=60):
        """从连接池获取一个连接，如果池为空则等待"""
        import threading
        import time
        task_name = threading.current_thread().name

        with self.condition:
            while True:
                if self.pool:
                    conn = self.pool.pop()
                    # 检查连接是否仍然有效
                    try:
                        conn.ping()
                        print('[连接池] 任务 {0} 复用连接 #{1} (池中剩余: {2})'.format(
                            task_name, conn._conn_id, len(self.pool)))
                        return conn
                    except Exception:
                        # 连接失效，关闭并继续获取下一个
                        try:
                            conn.close()
                        except Exception:
                            pass
                        continue
                else:
                    # 池已空，等待其他线程归还
                    print('[连接池] 任务 {0} 等待连接归还 (池为空)...'.format(task_name))
                    # 使用 condition.wait 等待，避免死锁
                    self.condition.wait(timeout=1)
                    # 继续循环尝试获取连接

    def return_connection(self, conn):
        """归还连接到池中"""
        import threading
        task_name = threading.current_thread().name
        with self.condition:
            if len(self.pool) < self.max_connections:
                self.pool.append(conn)
                print('[连接池] 任务 {0} 归还连接 #{1} (池中现有: {2})'.format(
                    task_name, conn._conn_id, len(self.pool)))
            else:
                conn.close()
                print('[连接池] 任务 {0} 关闭连接 #{1}'.format(
                    task_name, conn._conn_id))
            # 通知等待的线程
            self.condition.notify_all()

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


def get_hive_connection_pool(cluster_config, max_connections=5):
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
                max_connections=max_connections
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


def get_file_dir(config):
    """Return the base directory used for output files."""
    return config.get('file_dir') or 'output'


def get_runtime_config(config):
    """Return runtime settings with safe defaults."""
    runtime = config.get('runtime', {}).copy()
    runtime.setdefault('max_connections', 4)
    runtime.setdefault('max_retries', 0)
    runtime.setdefault('query_timeout_sec', 1800)
    runtime.setdefault('poll_interval_sec', 300)
    runtime.setdefault('wait_timeout_sec', 14400)
    return runtime


def normalize_run_dt(value):
    """Normalize CLI date input to yyyymmdd."""
    stripped = (value or '').strip()
    if re.fullmatch(r'\d{8}', stripped):
        return stripped
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', stripped):
        return stripped.replace('-', '')
    raise ValueError(
        '--data-dt 格式错误: {0}，期望 yyyymmdd 或 yyyy-mm-dd'.format(value)
    )


def to_partition_dt(run_dt):
    """Convert yyyymmdd to yyyy-mm-dd for Hive partition filters."""
    if not re.fullmatch(r'\d{8}', run_dt):
        raise ValueError('run_dt 格式错误: {0}，期望 yyyymmdd'.format(run_dt))
    return '{0}-{1}-{2}'.format(run_dt[0:4], run_dt[4:6], run_dt[6:8])


def get_cluster_config(config, cluster_name):
    """Return the shared cluster config while keeping cluster_name as a logical label."""
    # 使用 hive 配置
    cluster_config = config.get('hive')
    if cluster_config:
        return cluster_config.copy()

    raise ValueError(
        '未在 env_config.json 中找到 hive 配置'
    )


def build_default_artifact_path(base_dir, data_dt, cluster_name):
    """Build the default per-cluster artifact path."""
    return os.path.join(base_dir, data_dt, '{0}_{1}_table_metrics.tsv'.format(data_dt, cluster_name))


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


def load_sql_tasks(sql_file, partition_dt, run_dt):
    """Read SQL tasks from file and render date placeholders."""
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_text = f.read()

    statements = split_sql_statements(sql_text)
    rendered = []
    for statement in statements:
        rendered.append(
            statement
            .replace('{{data_dt}}', partition_dt)
            .replace('{{partition_dt}}', partition_dt)
            .replace('{{run_dt}}', run_dt)
        )
    return rendered


def build_beeline_command(cluster_config, sql):
    """Build a beeline command, optionally wrapped by ssh."""
    beeline_cmd = cluster_config.get('beeline_cmd', 'beeline')
    beeline_url = cluster_config.get('beeline_url')
    if not beeline_url:
        raise ValueError('集群配置缺少 beeline_url')

    base_cmd = [
        beeline_cmd,
        '-u', beeline_url,
        '--outputformat=tsv',
        '--showHeader=true',
        '--silent=true',
    ]

    # 设置 tez 引擎
    base_cmd.extend(['-e', 'set hive.execution.engine=tez;'])

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


def format_shell_command(cmd_parts):
    """Format a command list for printing/copy-paste."""
    return ' '.join(str(part) for part in cmd_parts)


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


def execute_hive_sql(cluster_name, cluster_config, sql, timeout_sec, config):
    """Execute one Hive SQL and return exactly one row as a dict.

    仅使用 Tez 连接池模式，不支持回退。
    """
    if not HIVE_DRIVER_AVAILABLE:
        raise RuntimeError('pyhive 未安装，无法使用 Tez 连接池模式')

    return _execute_hive_sql_with_pool(cluster_name, cluster_config, sql, timeout_sec, config)


def _execute_hive_sql_with_pool(cluster_name, cluster_config, sql, timeout_sec, config):
    """使用连接池执行 Hive SQL（带超时）"""
    import threading
    import socket
    # 从配置中获取最大并发数（从 runtime 配置读取，不是 cluster 配置）
    runtime_config = config.get('runtime', {})
    max_connections = runtime_config.get('max_connections', 4)

    # 获取或创建连接池
    pool = get_hive_connection_pool(cluster_config, max_connections)

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
    print('[任务 {0}] 开始执行 (连接 #{1}, Tez引擎: 是, 超时: {2}秒): {3}...'.format(
        task_name, conn_id, timeout_sec, sql_preview))

    try:
        # 设置执行引擎为 Tez
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


def run_beeline_loop(cluster_name, cluster_config, tasks, timeout_sec, output_file):
    """单线程循环执行：每条 SQL 启动一次 beeline，解析结果并写入 TSV。"""
    writer = DelimitedRowWriter(output_file, ARTIFACT_FIELDNAMES, '\t')
    row_count = 0

    for index, sql in enumerate(tasks):
        print('\n[beeline-loop] 开始执行 {0}/{1}'.format(index + 1, len(tasks)))
        cmd = build_beeline_command(cluster_config, sql)
        print('[beeline-loop] command: {0}'.format(format_shell_command(cmd)))
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_sec,
        )
        if result.returncode != 0:
            stderr_text = result.stderr.decode('utf-8', errors='replace').strip()
            raise RuntimeError(
                'beeline 执行失败 (cluster={0}, index={1}, rc={2}): {3}'.format(
                    cluster_name, index + 1, result.returncode, stderr_text or '无错误输出'
                )
            )

        stdout_text = result.stdout.decode('utf-8', errors='replace')
        header, data_rows = parse_beeline_tsv(stdout_text)
        if len(data_rows) != 1:
            raise ValueError(
                '集群 {0} 的 SQL 结果必须是 1 行，实际为 {1} 行 (index={2})'.format(
                    cluster_name, len(data_rows), index + 1
                )
            )

        result_row = dict(zip(header, data_rows[0]))
        rows = normalize_metric_rows(cluster_name, result_row)
        writer.write_rows(rows)
        row_count += len(rows)
        print('[beeline-loop] 完成 {0}/{1}，新增 {2} 行指标'.format(
            index + 1, len(tasks), len(rows)))

    return row_count


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


def write_ok_file(target_file):
    """Create a same-name .ok marker file after successful processing."""
    base, _ = os.path.splitext(target_file)
    ok_file = '{0}.ok'.format(base)
    ensure_parent_dir(ok_file)
    with open(ok_file, 'w', encoding='utf-8') as f:
        f.write('ok\n')
    return ok_file


class ArtifactResultProcessor(object):
    """Write normalized metric rows as soon as each task finishes."""

    def __init__(self, cluster_name, output_file):
        self.cluster_name = cluster_name
        self.writer = DelimitedRowWriter(output_file, ARTIFACT_FIELDNAMES, '\t')
        self.row_count = 0

    def __call__(self, task, result_row):
        rows = normalize_metric_rows(self.cluster_name, result_row)
        self.writer.write_rows(rows)
        self.row_count += len(rows)


def run_async_scheduler(cluster_name, cluster_config, runtime, tasks, output_file):
    """Run the default PyHive async submitter for metric generation."""
    processor = ArtifactResultProcessor(cluster_name, output_file)
    scheduler = HiveAsyncScheduler(
        resolve_cluster_connection(cluster_config),
        build_scheduler_runtime(runtime),
        result_processor=processor,
    )
    summary = scheduler.run(tasks)
    return summary, processor


def run_task(task_index, cluster_name, cluster_config, sql, timeout_sec, max_retries, config):
    """Run one SQL task with retries and return expanded rows."""
    attempt = 0
    last_error = None
    max_attempts = max_retries + 1

    while attempt < max_attempts:
        try:
            result_row = execute_hive_sql(cluster_name, cluster_config, sql, timeout_sec, config)
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


def execute_hive_sql_fast_beeline(cluster_name, cluster_config, sql, timeout_sec):
    """使用 beeline 命令执行 Hive SQL，快速提交模式（不等待结果）"""
    import subprocess
    import threading

    cmd = build_beeline_command(cluster_config, sql)

    # 使用 Popen 启动进程，不等待完成
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # 返回进程对象，稍后获取结果
    return process


def wait_beeline_result(process, task_index, cluster_name, timeout_sec):
    """等待 beeline 进程完成并获取结果"""
    try:
        stdout, stderr = process.communicate(timeout=timeout_sec)

        if process.returncode != 0:
            stderr_text = stderr.decode('utf-8', errors='replace').strip()
            raise RuntimeError(
                '集群 {0} 执行失败，返回码 {1}: {2}'.format(
                    cluster_name, process.returncode, stderr_text or '无错误输出'
                )
            )

        stdout_text = stdout.decode('utf-8', errors='replace')
        header, data_rows = parse_beeline_tsv(stdout_text)

        if len(data_rows) != 1:
            raise ValueError(
                '集群 {0} 的 SQL 结果必须是 1 行，实际为 {1} 行'.format(
                    cluster_name, len(data_rows)
                )
            )

        return dict(zip(header, data_rows[0]))

    except subprocess.TimeoutExpired:
        process.kill()
        raise RuntimeError(
            '集群 {0} 执行超时 (超过 {1} 秒)'.format(cluster_name, timeout_sec)
        )


def run_beeline_persistent(cluster_name, cluster_config, tasks, timeout_sec, max_processes=5, max_pending=30):
    """使用持久 beeline 进程执行任务，每个进程持续运行，不断提交新任务"""
    import queue
    import time
    import threading
    import subprocess

    # 任务队列（所有 worker 共享）
    task_queue = queue.Queue()
    for i, sql in enumerate(tasks):
        task_queue.put((i, sql))

    total_tasks = len(tasks)
    completed_results = {}  # {task_index: result}
    lock = threading.Lock()

    def worker_thread(worker_id):
        """工作线程：维护一个持久的 beeline 进程，不断处理任务"""
        beeline_cmd = cluster_config.get('beeline_cmd', 'beeline')
        beeline_url = cluster_config.get('beeline_url')
        username = cluster_config.get('username', 'atguigu')

        # 启动持久 beeline 进程
        cmd = [
            beeline_cmd,
            '-u', beeline_url,
            '--outputformat=tsv',
            '--showHeader=true',
            '--silent=false',
            '-n', username,
        ]

        print('[Worker {0}] 启动 beeline 进程'.format(worker_id))

        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
        )

        stdin = process.stdin
        stdout = process.stdout

        # 设置非阻塞读取
        import fcntl
        import os
        flags = fcntl.fcntl(stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(stdout.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)

        current_task_index = None
        current_sql = None
        output_buffer = []
        waiting_for_result = False

        while True:
            # 1. 如果没有正在执行的任务，从队列获取新任务
            if current_task_index is None:
                try:
                    task_index, sql = task_queue.get_nowait()
                    current_task_index = task_index
                    current_sql = sql
                    output_buffer = []
                    waiting_for_result = True

                    # 提交 SQL
                    print('\n[Worker {0}] 即将执行任务 {1}/{2}'.format(
                        worker_id, task_index + 1, total_tasks))
                    print(sql.strip())
                    stdin.write(sql + ';\n')
                    stdin.flush()
                    print('[Worker {0}] 任务 {1} 已提交'.format(worker_id, task_index + 1))

                except queue.Empty:
                    pass

            # 2. 尝试读取输出
            if waiting_for_result:
                try:
                    while True:
                        line = stdout.readline()
                        if not line:
                            break
                        output_buffer.append(line.rstrip())

                        # 检测任务完成（看到新的 jdbc 提示符）
                        if 'jdbc:' in line.lower() and '>' in line:
                            # 检查是否是命令执行完成的提示符
                            if len(output_buffer) > 2:
                                waiting_for_result = False
                                break

                except:
                    pass

                # 如果收集到完整结果，解析它
                if not waiting_for_result and output_buffer:
                    try:
                        # 解析 TSV 输出
                        filtered = [l for l in output_buffer if l and '\t' in l]
                        if filtered:
                            header = filtered[0].split('\t')
                            data = filtered[1].split('\t')
                            result = dict(zip(header, data))

                            with lock:
                                completed_results[current_task_index] = result

                            print('[Worker {0}] 任务 {1} 完成!'.format(worker_id, current_task_index + 1))

                        current_task_index = None
                        current_sql = None
                        output_buffer = []

                    except Exception as e:
                        print('[Worker {0}] 解析错误: {1}'.format(worker_id, e))
                        current_task_index = None
                        output_buffer = []

            # 3. 检查任务队列是否全部完成
            with lock:
                if len(completed_results) >= total_tasks:
                    break

            # 4. 短暂等待
            time.sleep(0.1)

        # 退出 beeline
        try:
            stdin.write('!quit\n')
            stdin.flush()
            process.wait(timeout=5)
        except:
            process.kill()

        print('[Worker {0}] 退出'.format(worker_id))

    # 启动工作线程
    print('\n[开始] 启动 {0} 个持久 beeline 进程...'.format(max_processes))
    threads = []
    for i in range(max_processes):
        t = threading.Thread(target=worker_thread, args=(i,))
        t.start()
        threads.append(t)

    # 等待所有任务完成
    start_time = time.time()
    while True:
        with lock:
            completed = len(completed_results)
        if completed >= total_tasks:
            break
        if time.time() - start_time > timeout_sec:
            print('[超时] 任务执行超时')
            break
        time.sleep(0.5)

    elapsed = time.time() - start_time
    print('\n[完成] 全部任务完成! 总耗时: {0:.2f}s'.format(elapsed))

    # 等待线程结束
    for t in threads:
        t.join(timeout=5)

    # 按顺序返回结果
    results = []
    for i in range(total_tasks):
        if i in completed_results:
            results.append((i, normalize_metric_rows(cluster_name, completed_results[i])))
        else:
            print('警告: 任务 {0} 未完成'.format(i))

    return results


def main():
    parser = argparse.ArgumentParser(description='执行 Hive SQL 并生成指标中间结果文件')
    parser.add_argument('--sql-file', required=True, help='SQL 文件路径')
    parser.add_argument('--data-dt', required=True, help='分区日期，如 2024-01-01')
    parser.add_argument('--cluster', required=True, help='逻辑集群标识，用于区分生成文件名称')
    parser.add_argument('--output-file', help='输出 TSV 文件路径')
    parser.add_argument('--beeline-loop', action='store_true',
                        help='单线程循环：每条 SQL 启动一次 beeline 并等待结果')
    parser.add_argument('--use-beeline', action='store_true',
                        help='强制使用 beeline 模式，不使用连接池')
    parser.add_argument('--max-processes', type=int, default=5,
                        help='最大 beeline 进程数 (默认 5)')
    parser.add_argument('--max-pending', type=int, default=30,
                        help='最大等待提交的任务数 (默认 30)')
    args = parser.parse_args()

    config = load_env_config()
    runtime = get_runtime_config(config)
    run_dt = normalize_run_dt(args.data_dt)
    partition_dt = to_partition_dt(run_dt)
    cluster_config = get_cluster_config(config, args.cluster)

    # 强制使用 beeline 模式（仍使用 Tez 引擎）
    if args.use_beeline or args.beeline_loop:
        pass

    if not args.output_file:
        artifact_dir = get_file_dir(config)
        args.output_file = build_default_artifact_path(artifact_dir, run_dt, args.cluster)

    # 确保输出目录存在
    ensure_parent_dir(args.output_file)

    tasks = load_sql_tasks(args.sql_file, partition_dt, run_dt)
    if not tasks:
        raise ValueError('SQL 文件中未解析到可执行语句: {0}'.format(args.sql_file))

    print('集群: {0}'.format(args.cluster))
    print('运行日期: {0}'.format(run_dt))
    print('分区日期: {0}'.format(partition_dt))
    print('SQL 数量: {0}'.format(len(tasks)))
    for index, sql in enumerate(tasks):
        print('\n[SQL {0}/{1}]'.format(index + 1, len(tasks)))
        print(sql.strip())
    print('输出文件: {0}'.format(args.output_file))
    print('执行引擎: {0}'.format(
        'beeline-loop (单线程)' if args.beeline_loop else
        ('beeline (有限并发模式)' if args.use_beeline or not HIVE_DRIVER_AVAILABLE else 'PyHive 异步调度器')
    ))
    print('最大进程数: {0}, 最大等待任务数: {1}'.format(args.max_processes, args.max_pending))

    if args.beeline_loop:
        row_count = run_beeline_loop(
            args.cluster,
            cluster_config,
            tasks,
            runtime['query_timeout_sec'],
            args.output_file,
        )
        print('\n中间结果已生成: {0}'.format(args.output_file))
        print('共输出 {0} 条指标记录'.format(row_count))
        ok_file = write_ok_file(args.output_file)
        print('完成标记文件: {0}'.format(ok_file))
        return

    # 使用 beeline 持久进程模式
    if args.use_beeline or not HIVE_DRIVER_AVAILABLE:
        results_list = run_beeline_persistent(
            args.cluster,
            cluster_config,
            tasks,
            runtime['query_timeout_sec'],
            max_processes=args.max_processes,
            max_pending=args.max_pending
        )
        results_by_index = {index: rows for index, rows in results_list}

    else:
        scheduler_runtime = build_scheduler_runtime(runtime)
        print('异步提交配置: max_connections={0}, max_inflight_tasks={1}'.format(
            scheduler_runtime['max_connections'],
            scheduler_runtime['max_inflight_tasks'],
        ))
        print('提交策略: execute(async_=True) 返回后立即继续提交下一个任务')

        summary, processor = run_async_scheduler(
            args.cluster,
            cluster_config,
            runtime,
            tasks,
            args.output_file,
        )
        print('\n异步调度完成')
        print('成功任务: {0}'.format(len(summary['finished'])))
        print('失败任务: {0}'.format(len(summary['failed'])))
        print('最大 in-flight: {0}'.format(summary['max_observed_inflight']))
        print('总耗时: {0:.2f}s'.format(summary['elapsed_sec']))
        print('\n中间结果已生成: {0}'.format(args.output_file))
        print('共输出 {0} 条指标记录'.format(processor.row_count))

        if summary['failed']:
            failed_preview = []
            for task in summary['failed'][:5]:
                failed_preview.append(
                    'task {0}: {1}'.format(task.task_id, task.error)
                )
            raise RuntimeError(
                '存在失败任务，共 {0} 个: {1}'.format(
                    len(summary['failed']),
                    '; '.join(failed_preview),
                )
            )
        ok_file = write_ok_file(args.output_file)
        print('完成标记文件: {0}'.format(ok_file))
        return

    # 汇总结果
    all_rows = []
    for index in range(len(tasks)):
        if index in results_by_index:
            all_rows.extend(results_by_index[index])

    write_artifact_tsv(args.output_file, all_rows)
    print('\n中间结果已生成: {0}'.format(args.output_file))
    print('共输出 {0} 条指标记录'.format(len(all_rows)))
    ok_file = write_ok_file(args.output_file)
    print('完成标记文件: {0}'.format(ok_file))


if __name__ == '__main__':
    main()
