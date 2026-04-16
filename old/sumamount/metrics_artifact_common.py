#!/usr/bin/env python3
"""
Shared helpers for Hive metric artifact generation and comparison.
"""

import csv
import json
import os
import re
import shlex
import subprocess
import tempfile
from decimal import Decimal


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


def build_default_compare_path(base_dir, data_dt):
    """Build the default comparison result path."""
    return os.path.join(base_dir, data_dt, '{0}_metric_comparison.csv'.format(data_dt))


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

    base_cmd = [
        beeline_cmd,
        '-u', beeline_url,
        '--outputformat=tsv',
        '--showHeader=true',
        '--silent=true',
        '-e', sql,
    ]

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
        if not line or '\t' not in line:
            continue
        if line.startswith('WARN') or line.startswith('INFO') or line.startswith('SLF4J'):
            continue
        normalized_line = BEELINE_PROMPT_PREFIX_RE.sub('', raw_line, count=1)
        tsv_lines.append(normalized_line)

    if not tsv_lines:
        raise ValueError('未能从 beeline 输出中解析到 TSV 数据')

    parsed_lines = []
    for line in tsv_lines:
        row = next(csv.reader([line], delimiter='\t'))
        parsed_lines.append([normalize_beeline_field(value) for value in row])

    header = parsed_lines[0]
    data_rows = []
    for row in parsed_lines[1:]:
        if len(row) == len(header):
            data_rows.append(row)

    return header, data_rows


def normalize_beeline_field(value):
    """Normalize one beeline TSV field for easier downstream handling."""
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == "'" and stripped[-1] == "'":
        return stripped[1:-1].replace("\\'", "'")
    return stripped


def execute_hive_sql(cluster_name, cluster_config, sql, timeout_sec):
    """Execute one Hive SQL and return exactly one row as a dict."""
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
