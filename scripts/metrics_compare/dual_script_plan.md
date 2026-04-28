# Hive 异步提交调度器方案

## Summary

- 主实现采用 `Python + PyHive`
- 固定维护 `5` 个客户端连接
- 全局最多保持 `30` 个 in-flight 任务
- 达到 `30` 后阻塞，有任务完成就继续补提交
- 只有任务完成后才获取结果并做后续处理
- `beeline` 仅保留为回退和排障路径

## Environment Facts

当前环境信息来自 `env_config.json` 和现有测试脚本：

- 测试 SQL 文件：`/home/lkw/qianyi-project/scripts/metrics_compare/metrics_queries.sql`
- PyHive 连接目标：`172.20.10.6:10000`
- Hive 用户：`atguigu`

`runtime`

- `max_workers = 5`
- `max_connections = 5`
- `max_inflight_tasks = 30`
- `max_retries = 0`
- `query_timeout_sec = 1800`
- `fetch_result_timeout_sec = 1800`
- `scheduler_poll_interval_sec = 3`
- `poll_interval_sec = 300`
- `wait_timeout_sec = 14400`

`clusters.old`

- `beeline_cmd = /home/lkw/apache-hive-3.1.3-bin/bin/beeline`
- `beeline_url = jdbc:hive2://172.20.10.6:10000/`
- `hive_host = 172.20.10.6`
- `hive_port = 10000`
- `username = atguigu`
- `auth = NONE`
- `database = default`
- `use_ssh = false`
- `use_tez = true`

`clusters.new`

- `beeline_cmd = /home/lkw/apache-hive-3.1.3-bin/bin/beeline`
- `beeline_url = jdbc:hive2://172.20.10.6:10000/`
- `hive_host = 172.20.10.6`
- `hive_port = 10000`
- `username = atguigu`
- `auth = NONE`
- `database = default`
- `use_ssh = false`
- `use_tez = true`

`hive`

- `database = default`
- `table = metric_comparison`
- `beeline_cmd = /home/lkw/apache-hive-3.1.3-bin/bin/beeline`
- `beeline_url = jdbc:hive2://172.20.10.6:10000/`
- `hive_host = 172.20.10.6`
- `hive_port = 10000`
- `username = atguigu`
- `auth = NONE`
- `use_ssh = false`
- `hdfs_dir = /data/transfer_agent/data/upload/hdfs_tmp`

`metastore_mysql`

- `host = 172.20.10.6`
- `port = 3306`
- `user = root`
- `password = 000000`
- `db = metastore`

其他

- `file_dir = /data/transfer_agent/data/upload`

## Key Changes

- 新增 `hive_async_scheduler.py`
  - 基于 PyHive 的事件循环式调度器
  - 固定 `5` 个连接，最多 `30` 个 in-flight 任务
  - 任务完成后立刻 `fetchall()` 并调用结果处理器
  - 提供通用 TSV writer，记录任务原始结果
- 更新 `generate_metrics.py`
  - PyHive 主路径改为复用异步调度器
  - 结果处理改为“完成即标准化并追加写 TSV”
  - 如果存在失败任务，整体结束时返回失败并保留已完成结果
- 更新 `env_config.json`
  - 补齐 PyHive 直连字段和异步调度参数

## Test Plan

- `python3 -m unittest test_hive_async_scheduler.py`
- 使用 `generate_metrics.py` 对 `metrics_queries.sql` 执行一次真实提交
- 验证 `> 30` 个任务时系统最多保持 `30` 个 in-flight
- 验证先完成的任务会先写入结果文件
- 验证提交失败或轮询失败时会按 `max_retries` 规则处理

## Assumptions

- 一个 SQL 只允许返回 `1` 行结果
- 第一版结果处理采用“回调接口 + 默认 TSV 落盘”
- `30` 的限制按 in-flight 总数处理
- `beeline` 不再作为主调度实现
