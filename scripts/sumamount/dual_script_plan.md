# 按 `rowcounts` 流程复刻的 Hive 指标对比方案

## Summary

- 保留 2 个入口：
  - `generate_metrics_artifact.py`：通用生成脚本，`--cluster old|new` 决定跑哪个集群
  - `compare_metrics_artifact.py`：等待 old/new 结果文件，读取、对比并输出结果文件
- 复用 `new/rowcounts` 的文件流转方式，只把取数来源改成 Hive SQL

## Key Changes

- `generate_metrics_artifact.py`
  - 输入：`--sql-file`、`--data-dt`、`--cluster`、`--output-file`
  - 流程：读取配置 -> 读取 SQL 文件 -> 线程池执行 Hive -> 展开结果 -> 输出明细 TSV
  - 输出字段：`cluster, table_name, partition_col, metric_name, value, computed_at, data_dt`
- `compare_metrics_artifact.py`
  - 输入：`--data-dt`、`--old-artifact`、`--new-artifact`、`--output-file`
  - 流程：等待 old/new 文件存在 -> 读取两个文件 -> 按主键对齐 -> 计算差值 -> 输出结果文件
  - 对齐主键：`table_name + partition_col + metric_name + data_dt`
  - 输出字段：`database_name, table_name, partition_name, metric_name, old_value, new_value, diff_value, etl_tm`
- `env_config.json`
  - 扩展出 old/new 集群连接、线程池并发数、重试次数、轮询间隔、超时时间

## Test Plan

- `generate` 分别跑 old 和 new，生成两份格式一致的明细文件
- `compare` 能等待文件、读取文件并输出 Hive 可装载的结果文件
- 主键不一致、重复键、SQL 返回非单行、文件缺失超时等异常能明确失败

## Assumptions

- 一个 SQL 只允许返回 1 行结果
- 结果表按 `rowcounts/hive_table.sql` 风格处理，`data_dt` 仅作为分区列
- 当前结果值按 `STRING` 处理，`diff_value` 由脚本内部用 `Decimal` 计算后再转成字符串输出
