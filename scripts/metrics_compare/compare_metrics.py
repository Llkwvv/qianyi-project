#!/usr/bin/env python3
"""
Compare old/new metric artifacts and emit a Hive-ready CSV file.
"""

import argparse
import csv
import datetime
import json
import os
import re
import shlex
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
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
                 max_connections=5):
        self.host = host
        self.port = port
        self.username = username
        self.database = database
        self.max_connections = max_connections
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

        # 设置执行引擎为 tez
        cursor = conn.cursor()
        cursor.execute("SET hive.execution.engine=tez")
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


def get_hive_connection_pool(cluster_config, max_connections=4):
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

COMPARE_FIELDNAMES = [
    'database_name',
    'table_name',
    'partition_name',
    'metric_name',
    'old_value',
    'new_value',
    'diff_value',
    'etl_tm',
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


def build_default_artifact_path(base_dir, data_dt, cluster_name):
    """Build the default per-cluster artifact path."""
    return os.path.join(base_dir, data_dt, '{0}_metrics.tsv'.format(cluster_name))


def build_default_compare_path(base_dir, data_dt):
    """Build the default comparison result path."""
    return os.path.join(base_dir, data_dt, '{0}_metric_comparison.csv'.format(data_dt))


def ensure_parent_dir(file_path):
    """Create the parent directory when needed."""
    parent_dir = os.path.dirname(file_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)


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


def write_compare_csv(file_path, rows):
    """Write the comparison file in Hive-ready CSV format without a header."""
    serializable_rows = []
    for row in rows:
        serializable_rows.append({
            key: '' if value is None else str(value)
            for key, value in row.items()
        })
    atomic_write_delimited(file_path, COMPARE_FIELDNAMES, serializable_rows, ',', False)


def read_artifact_tsv(file_path):
    """Read a TSV artifact and validate its header."""
    if not os.path.exists(file_path):
        raise ValueError('文件不存在: {0}'.format(file_path))

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        fieldnames = reader.fieldnames or []
        legacy_fieldnames = [
            'cluster',
            'table_name',
            'partition_col',
            'metric_name',
            'value',
            'computed_at',
            'data_dt',
        ]
        if fieldnames not in (ARTIFACT_FIELDNAMES, legacy_fieldnames):
            raise ValueError(
                '中间结果文件列不匹配，期望 {0}，实际 {1}'.format(
                    ARTIFACT_FIELDNAMES, fieldnames
                )
            )

        rows = []
        for row in reader:
            if fieldnames == legacy_fieldnames:
                row['etl_tm'] = row.pop('computed_at')
            rows.append(row)

    return rows


def split_table_name(full_table_name):
    """Split db.table into database_name and table_name."""
    if '.' not in full_table_name:
        return '', full_table_name
    return full_table_name.split('.', 1)


def normalize_value_string(value):
    """Normalize empty and NULL-like values while keeping numeric precision text."""
    if value is None:
        return None

    stripped = str(value).strip()
    if not stripped or stripped.upper() == 'NULL':
        return None
    return stripped


def parse_decimal_or_none(value):
    """Parse a decimal value or return None for empty/NULL."""
    normalized = normalize_value_string(value)
    if normalized is None:
        return None
    return Decimal(normalized)


def decimal_to_string(value):
    """Render Decimal as a plain string without scientific notation."""
    if value is None:
        return None

    plain = format(value, 'f')
    if '.' in plain:
        plain = plain.rstrip('0').rstrip('.')
    if plain in ('', '-0'):
        return '0'
    return plain


def wait_for_artifacts(file_paths, timeout_sec, poll_interval_sec):
    """Wait until all file paths exist or raise a timeout error."""
    import time
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
    artifact_dir = get_file_dir(config)
    run_dt = normalize_run_dt(args.data_dt)
    partition_dt = to_partition_dt(run_dt)

    if not args.old_artifact:
        args.old_artifact = build_default_artifact_path(artifact_dir, run_dt, 'old')
    if not args.new_artifact:
        args.new_artifact = build_default_artifact_path(artifact_dir, run_dt, 'new')
    if not args.output_file:
        args.output_file = build_default_compare_path(artifact_dir, run_dt)

    print('运行日期: {0}'.format(run_dt))
    print('分区日期: {0}'.format(partition_dt))
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
            partition_dt,
        )
    )
    loaded_rows = load_compare_csv_to_hive(args.output_file, hive_config, partition_dt)
    print(
        'Hive 写入完成: {0}.{1}, 分区 data_dt={2}, 行数 {3}'.format(
            hive_config['database'],
            hive_config['table'],
            partition_dt,
            loaded_rows,
        )
    )


if __name__ == '__main__':
    main()
