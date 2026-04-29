#!/usr/bin/env python3
"""
Generic asynchronous Hive scheduler backed by PyHive.
"""

import argparse
import collections
import csv
import json
import os
import time

try:
    from pyhive import hive
    HIVE_DRIVER_AVAILABLE = True
except ImportError:
    hive = None
    HIVE_DRIVER_AVAILABLE = False

try:
    from TCLIService import ttypes as hive_ttypes
except ImportError:
    hive_ttypes = None


OPERATION_STATE_FALLBACKS = {
    0: 'INITIALIZED_STATE',
    1: 'RUNNING_STATE',
    2: 'FINISHED_STATE',
    3: 'CANCELED_STATE',
    4: 'CLOSED_STATE',
    5: 'ERROR_STATE',
    6: 'UNKNOWN_STATE',
    7: 'PENDING_STATE',
    8: 'TIMEDOUT_STATE',
}


def load_env_config(config_path='env_config.json'):
    """Load a JSON config file from the current script directory."""
    config_file = os.path.join(os.path.dirname(__file__), config_path)
    if not os.path.exists(config_file):
        return {}

    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def split_sql_statements(sql_text):
    """Split SQL text on semicolons outside single quotes."""
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


def parse_beeline_url(beeline_url):
    """Parse jdbc:hive2://host:port/database style URLs."""
    if not beeline_url or 'hive2://' not in beeline_url:
        return None, None, None

    location = beeline_url.split('hive2://', 1)[1]
    if '/' in location:
        host_port, database = location.split('/', 1)
    else:
        host_port = location
        database = ''

    if ':' in host_port:
        host, port_text = host_port.split(':', 1)
        port = int(port_text)
    else:
        host = host_port
        port = 10000

    database = database.strip() or None
    return host, port, database


def build_scheduler_runtime(runtime_config):
    """Return runtime settings for the async scheduler."""
    runtime = (runtime_config or {}).copy()
    max_connections = runtime.get('max_connections', 5)
    runtime['max_connections'] = max_connections
    runtime.setdefault('max_inflight_tasks', 30)
    runtime.setdefault('max_retries', 0)
    runtime.setdefault('query_timeout_sec', 1800)
    runtime.setdefault(
        'fetch_result_timeout_sec',
        runtime.get('query_timeout_sec', 1800),
    )
    runtime.setdefault(
        'scheduler_poll_interval_sec',
        runtime.get('poll_interval_sec', 3),
    )
    return runtime


def resolve_cluster_connection(cluster_config):
    """Resolve PyHive connection info from cluster config."""
    host = cluster_config.get('hive_host')
    port = cluster_config.get('hive_port')
    database = cluster_config.get('database')

    beeline_host, beeline_port, beeline_database = parse_beeline_url(
        cluster_config.get('beeline_url', '')
    )

    if host is None:
        host = beeline_host or '127.0.0.1'
    if port is None:
        port = beeline_port or 10000
    if database is None:
        database = beeline_database or 'default'

    return {
        'host': host,
        'port': port,
        'username': cluster_config.get('username', 'hive'),
        'database': database,
        'auth': cluster_config.get('auth', 'NONE'),
    }


def normalize_operation_state(state):
    """Return a stable string state name across PyHive/Thrift variants."""
    if state is None:
        return 'UNKNOWN'

    if isinstance(state, int):
        if hive_ttypes is not None:
            state_name = hive_ttypes.TOperationState._VALUES_TO_NAMES.get(state)
            if state_name:
                state = state_name
        if isinstance(state, int):
            state = OPERATION_STATE_FALLBACKS.get(state, str(state))

    state_text = str(state).upper()
    state_text = state_text.replace('_STATE', '')
    known_states = [
        'INITIALIZED',
        'PENDING',
        'RUNNING',
        'COMPILING',
        'FINISHED',
        'CANCELED',
        'CLOSED',
        'ERROR',
        'TIMEDOUT',
    ]
    for token in known_states:
        if token in state_text:
            return token
    return state_text


def is_connection_error(exc):
    """Best-effort classifier for session-level connection failures."""
    error_text = str(exc).lower()
    connection_markers = [
        'socket',
        'transport',
        'broken pipe',
        'connection reset',
        'eof',
        'invalid session',
        'sessionhandle',
        'not open',
        'closed',
        'timed out',
    ]
    for marker in connection_markers:
        if marker in error_text:
            return True
    return False


class DelimitedRowWriter(object):
    """Append rows to a delimited file and write the header once."""

    def __init__(self, file_path, fieldnames, delimiter='\t'):
        self.file_path = file_path
        self.fieldnames = list(fieldnames)
        self.delimiter = delimiter
        self._initialize_file()

    def _initialize_file(self):
        parent_dir = os.path.dirname(self.file_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        with open(self.file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=self.fieldnames,
                delimiter=self.delimiter,
                lineterminator='\n',
            )
            writer.writeheader()

    def write_rows(self, rows):
        if not rows:
            return

        with open(self.file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=self.fieldnames,
                delimiter=self.delimiter,
                lineterminator='\n',
            )
            for row in rows:
                writer.writerow(row)


class SchedulerTask(object):
    """One scheduled SQL task."""

    def __init__(self, task_id, sql, metadata=None):
        self.task_id = task_id
        self.sql = sql
        self.metadata = metadata or {}
        self.attempts = 0
        self.status = 'pending'
        self.error = None
        self.cursor = None
        self.session = None
        self.submitted_at = None
        self.finished_at = None
        self.last_state = None
        self.result_row = None
        self.result_processor_error = None

    @property
    def elapsed_sec(self):
        if self.submitted_at is None:
            return None
        end_time = self.finished_at if self.finished_at is not None else time.time()
        return end_time - self.submitted_at


class HiveSession(object):
    """A persistent Hive session that can host multiple async cursors."""

    def __init__(self, session_id, connection_info, socket_timeout_sec=None, logger=None):
        self.session_id = session_id
        self.connection_info = connection_info
        self.socket_timeout_sec = socket_timeout_sec
        self.logger = logger or (lambda message: None)
        self.connection = None

    def ensure_connection(self):
        """Create the session connection on first use."""
        if not HIVE_DRIVER_AVAILABLE:
            raise RuntimeError('pyhive 未安装，无法使用异步调度器')

        if self.connection is not None:
            return self.connection

        self.logger(
            '[Session {0}] 创建连接 {1}:{2}/{3}'.format(
                self.session_id,
                self.connection_info['host'],
                self.connection_info['port'],
                self.connection_info['database'],
            )
        )

        self.connection = hive.Connection(
            host=self.connection_info['host'],
            port=self.connection_info['port'],
            username=self.connection_info['username'],
            database=self.connection_info['database'],
            auth=self.connection_info['auth'],
        )

        socket_obj = getattr(self.connection, '_socket', None)
        if socket_obj is not None and self.socket_timeout_sec:
            socket_obj.settimeout(self.socket_timeout_sec)

        cursor = self.connection.cursor()
        cursor.execute('SET hive.execution.engine=tez')
        cursor.close()
        return self.connection

    def create_cursor(self):
        """Create a new cursor on the underlying session."""
        conn = self.ensure_connection()
        return conn.cursor()

    def reset_connection(self):
        """Drop the current session connection."""
        if self.connection is None:
            return

        try:
            self.connection.close()
        except Exception:
            pass
        self.connection = None

    def close(self):
        self.reset_connection()


class HiveAsyncScheduler(object):
    """Submit SQL asynchronously and process completed tasks immediately."""

    def __init__(
        self,
        connection_info,
        runtime_config,
        result_processor=None,
        logger=None,
        session_factory=None,
    ):
        self.connection_info = connection_info
        self.runtime = build_scheduler_runtime(runtime_config)
        self.result_processor = result_processor
        self.logger = logger or print
        self.session_factory = session_factory or HiveSession
        self.sessions = []
        self._next_session_index = 0
        self.max_observed_inflight = 0

    def _get_next_session(self):
        if not self.sessions:
            for session_id in range(1, self.runtime['max_connections'] + 1):
                self.sessions.append(
                    self.session_factory(
                        session_id,
                        self.connection_info,
                        self.runtime['query_timeout_sec'],
                        self.logger,
                    )
                )

        session = self.sessions[self._next_session_index]
        self._next_session_index = (self._next_session_index + 1) % len(self.sessions)
        return session

    def _submit_task(self, task):
        session = self._get_next_session()
        task.attempts += 1
        task.status = 'submitting'
        task.session = session
        task.error = None
        task.result_processor_error = None
        task.finished_at = None
        task.result_row = None
        cursor = session.create_cursor()

        try:
            cursor.execute(task.sql, async_=True)
        except Exception:
            try:
                cursor.close()
            except Exception:
                pass
            raise

        task.cursor = cursor
        task.submitted_at = time.time()
        task.status = 'submitted'
        task.last_state = 'SUBMITTED'
        self.logger(
            '[提交] task={0} attempt={1} session={2}'.format(
                task.task_id,
                task.attempts,
                session.session_id,
            )
        )

    def _cleanup_task_cursor(self, task):
        if task.cursor is None:
            return
        try:
            task.cursor.close()
        except Exception:
            pass
        task.cursor = None

    def _fetch_result_row(self, task):
        rows = task.cursor.fetchall()
        if len(rows) != 1:
            raise ValueError(
                '任务 {0} 的 SQL 结果必须是 1 行，实际为 {1} 行'.format(
                    task.task_id,
                    len(rows),
                )
            )

        description = task.cursor.description or []
        if not description:
            raise ValueError('任务 {0} 的 SQL 查询没有返回列信息'.format(task.task_id))

        columns = [column[0] for column in description]
        return dict(zip(columns, rows[0]))

    def _complete_task(self, task):
        task.result_row = self._fetch_result_row(task)
        task.finished_at = time.time()
        task.status = 'finished'

        if self.result_processor is not None:
            self.result_processor(task, task.result_row)

        self.logger(
            '[完成] task={0} session={1} elapsed={2:.2f}s'.format(
                task.task_id,
                task.session.session_id if task.session else 'NA',
                task.elapsed_sec or 0.0,
            )
        )

    def _retry_or_fail(self, task, pending_tasks, exc, reset_session=False):
        task.error = str(exc)
        task.finished_at = time.time()
        if reset_session and task.session is not None:
            task.session.reset_connection()
        self._cleanup_task_cursor(task)

        if task.attempts <= self.runtime['max_retries']:
            task.status = 'pending'
            task.session = None
            pending_tasks.append(task)
            self.logger(
                '[重试] task={0} attempt={1} error={2}'.format(
                    task.task_id,
                    task.attempts,
                    task.error,
                )
            )
            return 'retried'

        task.status = 'failed'
        self.logger(
            '[失败] task={0} attempt={1} error={2}'.format(
                task.task_id,
                task.attempts,
                task.error,
            )
        )
        return 'failed'

    def _poll_once(self, task, pending_tasks):
        poll_result = task.cursor.poll()
        state = normalize_operation_state(getattr(poll_result, 'operationState', None))
        task.last_state = state

        if state in ('INITIALIZED', 'PENDING', 'RUNNING', 'COMPILING'):
            return 'running'

        if state == 'FINISHED':
            try:
                self._complete_task(task)
                return 'finished'
            except Exception as exc:
                reset_session = is_connection_error(exc)
                return self._retry_or_fail(task, pending_tasks, exc, reset_session)

        return self._retry_or_fail(
            task,
            pending_tasks,
            RuntimeError('任务 {0} 失败，状态为 {1}'.format(task.task_id, state)),
            False,
        )

    def run(self, sql_tasks):
        pending_tasks = collections.deque()
        for index, task_item in enumerate(sql_tasks):
            if isinstance(task_item, dict):
                sql_text = task_item['sql']
                metadata = task_item.get('metadata')
            else:
                sql_text = task_item
                metadata = None
            pending_tasks.append(SchedulerTask(index, sql_text, metadata))

        in_flight = []
        finished_tasks = []
        failed_tasks = []
        loop_start_time = time.time()

        while pending_tasks or in_flight:
            submitted_any = False
            while pending_tasks and len(in_flight) < self.runtime['max_inflight_tasks']:
                task = pending_tasks.popleft()
                try:
                    self._submit_task(task)
                    in_flight.append(task)
                    self.max_observed_inflight = max(
                        self.max_observed_inflight,
                        len(in_flight),
                    )
                    submitted_any = True
                except Exception as exc:
                    reset_session = is_connection_error(exc)
                    outcome = self._retry_or_fail(task, pending_tasks, exc, reset_session)
                    if outcome == 'failed':
                        failed_tasks.append(task)

            completed_any = False
            for task in list(in_flight):
                if task.status != 'submitted':
                    continue

                try:
                    outcome = self._poll_once(task, pending_tasks)
                except Exception as exc:
                    reset_session = is_connection_error(exc)
                    outcome = self._retry_or_fail(task, pending_tasks, exc, reset_session)

                if outcome == 'running':
                    continue

                completed_any = True
                in_flight.remove(task)
                self._cleanup_task_cursor(task)

                if outcome == 'finished':
                    finished_tasks.append(task)
                elif outcome == 'failed':
                    failed_tasks.append(task)

            if pending_tasks or in_flight:
                if not completed_any:
                    time.sleep(self.runtime['scheduler_poll_interval_sec'])
                elif not submitted_any and in_flight:
                    time.sleep(self.runtime['scheduler_poll_interval_sec'])

        total_elapsed = time.time() - loop_start_time
        for session in self.sessions:
            session.close()

        return {
            'finished': finished_tasks,
            'failed': failed_tasks,
            'total': len(finished_tasks) + len(failed_tasks),
            'elapsed_sec': total_elapsed,
            'max_observed_inflight': self.max_observed_inflight,
        }


SCHEDULER_RESULT_FIELDNAMES = [
    'task_id',
    'attempts',
    'status',
    'session_id',
    'elapsed_sec',
    'finished_at',
    'error',
    'result_json',
]


class SchedulerTsvProcessor(object):
    """Write one completed task per TSV row."""

    def __init__(self, output_file):
        self.writer = DelimitedRowWriter(output_file, SCHEDULER_RESULT_FIELDNAMES, '\t')

    def __call__(self, task, result_row):
        self.writer.write_rows([{
            'task_id': task.task_id,
            'attempts': task.attempts,
            'status': task.status,
            'session_id': task.session.session_id if task.session else '',
            'elapsed_sec': '{0:.6f}'.format(task.elapsed_sec or 0.0),
            'finished_at': int(task.finished_at or time.time()),
            'error': task.error or '',
            'result_json': json.dumps(result_row, ensure_ascii=False, sort_keys=True),
        }])


def main():
    parser = argparse.ArgumentParser(description='异步提交 Hive SQL 并在完成时写出结果')
    parser.add_argument('--sql-file', required=True, help='SQL 文件路径')
    parser.add_argument('--data-dt', required=True, help='分区日期，如 2024-01-01')
    parser.add_argument('--cluster', default='hive', help='集群名称（用于输出文件区分，默认 hive）')
    parser.add_argument('--config', default='env_config.json', help='配置文件名')
    parser.add_argument('--output-file', help='原始任务结果 TSV 输出路径')
    args = parser.parse_args()

    if not HIVE_DRIVER_AVAILABLE:
        raise RuntimeError('pyhive 未安装，无法运行异步调度器')

    config = load_env_config(args.config)
    runtime = build_scheduler_runtime(config.get('runtime', {}))
    cluster_config = config.get('hive')
    if not cluster_config:
        raise ValueError('未在 env_config.json 中找到 hive 配置')

    output_file = args.output_file
    if not output_file:
        base_dir = config.get('file_dir') or 'output'
        output_file = os.path.join(base_dir, args.data_dt, '{0}_scheduler.tsv'.format(args.cluster))

    processor = SchedulerTsvProcessor(output_file)
    scheduler = HiveAsyncScheduler(
        resolve_cluster_connection(cluster_config),
        runtime,
        result_processor=processor,
    )

    tasks = load_sql_tasks(args.sql_file, args.data_dt)
    if not tasks:
        raise ValueError('SQL 文件中未解析到可执行语句: {0}'.format(args.sql_file))

    summary = scheduler.run(tasks)
    print('输出文件: {0}'.format(output_file))
    print('成功任务: {0}'.format(len(summary['finished'])))
    print('失败任务: {0}'.format(len(summary['failed'])))
    print('最大 in-flight: {0}'.format(summary['max_observed_inflight']))
    print('总耗时: {0:.2f}s'.format(summary['elapsed_sec']))

    if summary['failed']:
        raise RuntimeError('存在失败任务，详情见输出文件: {0}'.format(output_file))


if __name__ == '__main__':
    main()
