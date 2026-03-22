# New Cluster Validation

新集群指标校验脚本，基于 hive-new-cluster-plan.md 计划实现。

## 工具

`hive_validator.py` - 统一的 Hive 集群指标校验工具，支持三个子命令：

1. **ingest-old**: 从旧集群 CSV 读取并写入 Hive 目标表
2. **run-new**: 新集群执行相同语句，直接写入目标表
3. **compare**: 新旧集群指标对比并写入对比结果表

## 配置文件

- `env_config.json`: 环境配置（包含新旧集群配置）
- `sample_tables.json`: 库表配置

## 使用方法

### 1. 旧集群 CSV 导入 Hive 目标表

```bash
python new/hive_validator.py ingest-old \
  --csv ../old/output/old_summary.csv \
  --table old_summary \
  --cluster new
```

### 2. 新集群执行同 SQL 并写入目标表

```bash
python new/hive_validator.py run-new \
  --sql-file output/metrics_queries.sql \
  --data-dt 2024-01-01 \
  --table new_summary \
  --cluster new
```

### 3. 对比并写入对比表

```bash
python new/hive_validator.py compare \
  --data-dt 2024-01-01 \
  --old-table old_summary \
  --new-table new_summary \
  --compare-table compare_result \
  --cluster new \
  --threshold 0
```

## 参数说明

### ingest-old

| 参数 | 默认值 | 说明 |
|------|--------|------|
| --csv | 必填 | 旧集群导出的 CSV 文件路径 |
| --table | old_summary | 目标表名（不含库名） |
| --cluster | new | 目标集群 |

### run-new

| 参数 | 默认值 | 说明 |
|------|--------|------|
| --sql-file | output/metrics_queries.sql | SQL 文件路径 |
| --data-dt | 必填 | 分区日期 |
| --table | new_summary | 目标表名（不含库名） |
| --cluster | new | 集群名称 |
| --output-csv | 无 | 输出 CSV 文件路径（可选） |

### compare

| 参数 | 默认值 | 说明 |
|------|--------|------|
| --data-dt | 必填 | 分区日期 |
| --old-table | old_summary | 旧集群结果表名 |
| --new-table | new_summary | 新集群结果表名 |
| --compare-table | compare_result | 对比结果表名 |
| --cluster | new | 集群名称 |
| --threshold | 0.0 | 数值对比阈值 |
| --output-csv | 无 | 输出 CSV 文件路径（可选） |

## 表结构

### old_summary / new_summary

| 字段 | 类型 | 说明 |
|------|------|------|
| table_name | STRING | 表名 |
| partition_col | STRING | 分区字段 |
| metric_name | STRING | 指标名 |
| value | STRING | 指标值 |
| computed_at | TIMESTAMP | 计算时间 |
| data_dt | STRING | 数据日期 |

### compare_result

| 字段 | 类型 | 说明 |
|------|------|------|
| table_name | STRING | 表名 |
| partition_col | STRING | 分区字段 |
| metric_name | STRING | 指标名 |
| data_dt | STRING | 数据日期 |
| old_value | STRING | 旧集群值 |
| new_value | STRING | 新集群值 |
| diff | STRING | 差值 |
| status | STRING | 状态 (PASS/FAIL/MISSING_OLD/MISSING_NEW) |
| reason | STRING | 原因 |
| compared_at | TIMESTAMP | 对比时间 |
