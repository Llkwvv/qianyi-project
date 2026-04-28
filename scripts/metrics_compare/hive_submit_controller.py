#!/usr/bin/env python3
"""
Hive 并发任务控制器 - 生产级版本

功能：
- 客户端固定 5 个线程，每个维护一个持久 Hive 连接
- 提交任务使用 async_=True，不等待执行完成
- 通过 Semaphore 控制集群最大同时运行任务数（默认 30）
- 全部提交完成后，再统一轮询状态、获取结果
- 自动释放槽位，支持“填满 → 补充”模式
"""

import time
import threading
import queue
from pyhive import hive

# ===== 配置 =====
HIVE_HOST = '172.20.10.6'
HIVE_PORT = 10000
HIVE_USER = 'atguigu'
MAX_WORKERS = 5          # 客户端并发线程数（即连接池大小）
MAX_SLOTS = 30           # 集群最大同时运行任务数
DATA_DT = '2023-06-10'
SQL_FILE = '/home/lkw/qianyi-project/scripts/metrics_compare/metrics_queries.sql'
POLL_INTERVAL = 3        # 轮询间隔（秒）
TIMEOUT_HOURS = 2         # 单个任务超时时间（小时）

# ===== 信号量控制集群总槽位 =====
cluster_semaphore = threading.Semaphore(MAX_SLOTS)

# 线程本地连接
thread_local = threading.local()

def get_connection():
    """获取线程本地的持久连接"""
    if not hasattr(thread_local, 'conn') or thread_local.conn is None:
        print(f'[线程 {threading.current_thread().name}] 创建新连接')
        thread_local.conn = hive.Connection(
            host=HIVE_HOST,
            port=HIVE_PORT,
            username=HIVE_USER,
            auth='NONE'
        )
        cursor = thread_local.conn.cursor()
        cursor.execute('SET hive.execution.engine=tez')
        cursor.close()
    return thread_local.conn

def load_sql_tasks(sql_file: str, data_dt: str):
    """加载 SQL 文件并渲染日期变量"""
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_text = f.read()

    statements = [s.strip() for s in sql_text.split(';') if s.strip()]
    rendered = []
    for stmt in statements:
        rendered.append(stmt.replace('{{data_dt}}', data_dt))
    return rendered

class AsyncTask:
    """异步任务对象"""
    def __init__(self, task_id: int, sql: str):
        self.task_id = task_id
        self.sql = sql
        self.status = 'pending'  # pending, submitted, finished, failed
        self.cursor = None
        self.result = None
        self.error = None

    def submit(self) -> bool:
        """
        异步提交任务
        成功返回 True，失败返回 False（并设置 error）
        """
        try:
            conn = get_connection()
            cursor = conn.cursor()

            # 获取集群槽位（如果已满则阻塞）
            cluster_semaphore.acquire()
            print(f'[线程 {threading.current_thread().name}] 任务 {self.task_id} 获得槽位 → 提交中...')

            start_time = time.time()
            # ▼ 关键：异步提交，立即返回 ▼
            cursor.execute(self.sql, async_=True)
            submit_time = time.time() - start_time

            self.cursor = cursor
            self.status = 'submitted'
            print(f'[线程 {threading.current_thread().name}] 任务 {self.task_id} 提交成功 (耗时: {submit_time:.2f}s)')
            return True
        except Exception as e:
            self.status = 'failed'
            self.error = str(e)
            print(f'[线程 {threading.current_thread().name}] 任务 {self.task_id} 提交失败: {e}')
            return False

    def wait_and_fetch(self):
        """
        轮询任务状态并获取结果
        """
        if self.status != 'submitted':
            return

        timeout_sec = TIMEOUT_HOURS * 3600
        start_poll = time.time()

        print(f'开始轮询任务 {self.task_id} 状态...')
        try:
            while True:
                # 检查是否超时
                if time.time() - start_poll > timeout_sec:
                    self.status = 'failed'
                    self.error = f'Task timed out after {TIMEOUT_HOURS}h'
                    break

                poll = self.cursor.poll()
                state = poll.operationState

                if state == 'FINISHED':
                    print(f'任务 {self.task_id} 已完成，正在获取结果...')
                    self.result = self.cursor.fetchall()
                    self.status = 'finished'
                    break
                elif state in ('RUNNING', 'INITIALIZED', 'COMPILING'):
                    print(f'任务 {self.task_id} 正在运行 ({state})...')
                    time.sleep(POLL_INTERVAL)
                    continue
                else:
                    self.status = 'failed'
                    self.error = f'Operation failed: {state}'
                    break
        except Exception as e:
            self.status = 'failed'
            self.error = str(e)
        finally:
            # 无论成功失败，都必须释放集群槽位
            cluster_semaphore.release()

            if self.status == 'finished':
                print(f'✅ 任务 {self.task_id} 成功完成')
            else:
                print(f'❌ 任务 {self.task_id} 失败: {self.error}')


def worker_submit(task_queue: queue.Queue, result_list: list, stop_event: threading.Event):
    """工作线程：从队列取任务并异步提交"""
    thread_name = threading.current_thread().name
    while not stop_event.is_set():
        try:
            task = task_queue.get(timeout=1)
        except queue.Empty:
            continue

        success = task.submit()
        result_list.append((task.task_id, success))
        task_queue.task_done()


def main():
    print('=' * 70)
    print('Hive 并发任务控制器 - 生产模式')
    print('=' * 70)
    print(f'客户端线程: {MAX_WORKERS} | 集群最大槽位: {MAX_SLOTS}')
    print(f'SQL 文件: {SQL_FILE}')
    print(f'日期: {DATA_DT}')
    print()

    # 加载任务
    tasks = load_sql_tasks(SQL_FILE, DATA_DT)
    total = len(tasks)
    print(f'共加载 {total} 个任务')
    print()

    # 构建任务对象列表
    task_objects = [AsyncTask(i, sql) for i, sql in enumerate(tasks)]

    # 提交队列
    submit_queue = queue.Queue()
    for task in task_objects:
        submit_queue.put(task)

    # 收集结果
    submit_results = []
    stop_event = threading.Event()

    # 启动工作线程
    threads = []
    start_time = time.time()
    for i in range(MAX_WORKERS):
        t = threading.Thread(
            target=worker_submit,
            args=(submit_queue, submit_results, stop_event),
            name=f'HiveWorker-{i}'
        )
        t.start()
        threads.append(t)

    # 等待所有任务提交完成
    print('正在提交任务...（达到30个运行中会自动阻塞）')
    submit_queue.join()
    submit_elapsed = time.time() - start_time
    stop_event.set()  # 停止线程
    print(f'\n✅ 所有任务已提交完毕! 提交阶段耗时: {submit_elapsed:.2f}s')

    # 开始轮询并获取结果
    print('\n开始轮询任务状态并获取结果...')
    fetch_start = time.time()

    for task in task_objects:
        if task.status == 'submitted':
            task.wait_and_fetch()

    fetch_elapsed = time.time() - fetch_start
    overall_elapsed = time.time() - start_time

    # 统计
    finished = len([t for t in task_objects if t.status == 'finished'])
    failed = len([t for t in task_objects if t.status in ('failed', 'error')])

    print('\n' + '=' * 70)
    print(f'全部完成! 总耗时: {overall_elapsed:.2f}s')
    print(f'  - 提交阶段: {submit_elapsed:.2f}s')
    print(f'  - 取结果阶段: {fetch_elapsed:.2f}s')
    print(f'成功: {finished}, 失败: {failed}')
    print(f'平均速率: {total / overall_elapsed:.1f} 任务/秒')
    print('=' * 70)

if __name__ == '__main__':
    main()
