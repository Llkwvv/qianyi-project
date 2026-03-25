# New Cluster Validation

新集群指标校验脚本，基于 [.cursor/plans/hive-new-cluster-plan_e9a3c7f2.plan.md](../.cursor/plans/hive-new-cluster-plan_e9a3c7f2.plan.md) 计划实现。

## 工具

`hive_validator.py` - 统一的 Hive 集群指标校验工具，支持两个子命令：

1. **ingest-old**: 从旧集群 CSV 读取并通过 LOAD DATA 写入 Hive **统一结果表**（`cluster=old`）
2. **run-new**: 新集群执行相同语句，并通过 LOAD DATA 写入**同一张表**（`cluster=new`）

## 配置文件

- `env_config.json`: 环境配置（包含新旧集群配置）
- `sample_tables.json`: 库表配置

## 使用方法

### 1. 旧集群 CSV 导入 Hive 目标表

```bash
python new/hive_validator.py ingest-old \
  --csv ../old/output/old_summary.csv \
  --cluster new
```

### 2. 新集群执行同 SQL 并写入目标表

```bash
python new/hive_validator.py run-new \
  --sql-file output/metrics_queries.sql \
  --data-dt 2024-01-01 \
  --cluster new
```

## 参数说明

### ingest-old

| 参数 | 默认值 | 说明 |
|------|--------|------|
| --csv | 必填 | 旧集群导出的 CSV 文件路径 |
| --cluster | new | 目标集群 |
| --overwrite | false | 覆盖已有数据 |

### run-new

| 参数 | 默认值 | 说明 |
|------|--------|------|
| --sql-file | output/metrics_queries.sql | SQL 文件路径 |
| --data-dt | 必填 | 分区日期 |
| --cluster | new | 集群名称 |
| --overwrite | false | 覆盖已有数据 |
| --output-csv | 无 | 输出 CSV 文件路径（可选） |

## 表结构

### metrics_summary（统一结果表）

| 字段 | 类型 | 说明 |
|------|------|------|
| cluster | STRING | 集群标识（old/new） |
| table_name | STRING | 表名 |
| partition_col | STRING | 分区字段 |
| metric_name | STRING | 指标名 |
| value | STRING | 指标值 |
| computed_at | TIMESTAMP | 计算时间 |
| data_dt | STRING | 数据日期 |