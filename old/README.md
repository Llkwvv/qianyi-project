# Old 集群 Hive 指标校验脚本

## 概述

用于旧集群的 Hive 指标校验工具，支持两阶段流程：
1. **SQL 生成阶段**：读取配置文件，生成 SQL 模板
2. **执行阶段**：执行 SQL 并导出 CSV 结果

## 目录结构

```
old/
├── env_config.json              # 环境配置
├── sample_tables.json           # 库表配置
├── hive_metrics_sql_generator.py # SQL 生成脚本
├── run_hive_queries_python.py   # 执行脚本
└── output/
    ├── metrics_queries.sql      # 生成的 SQL 模板
    └── old_summary.csv          # 执行结果
```

## 配置文件

### env_config.json

```json
{
  "clusters": {
    "old": {
      "use_ssh": true,           // true: SSH 远程执行; false: 本地执行
      "ssh_host": "172.20.10.6", // SSH 服务器地址
      "ssh_port": 22,            // SSH 端口
      "ssh_user": "atguigu",     // SSH 用户名
      "hive_host": "localhost",   // Hive 服务器地址
      "port": 10000,             // Hive 端口
      "username": "atguigu"      // Hive 用户名
    }
  }
}
```

### sample_tables.json

```json
{
  "tables": [
    {
      "name": "gmall.dim_activity_full",
      "partition_cols": ["dt"],
      "fields": [
        {"name": "condition_amount", "type": "decimal(16,2)"}
      ]
    }
  ]
}
```

**字段说明**：
- `name`：库表名（必填）
- `partition_cols`：分区字段数组（可为空）
- `fields`：字段数组（仅 decimal 类型）

## 使用方法

### 阶段1：生成 SQL 模板

```bash
python old/hive_metrics_sql_generator.py \
  --table-list old/sample_tables.json \
  --output-dir old/output
```

**输出**：`old/output/metrics_queries.sql`

### 阶段2：执行查询并导出 CSV

```bash
python old/run_hive_queries_python.py \
  --sql-file old/output/metrics_queries.sql \
  --data-dt 2024-01-01 \
  --output-csv old/output/old_summary.csv
```

**参数说明**：
| 参数 | 必填 | 说明 |
|------|------|------|
| `--sql-file` | 是 | SQL 文件路径 |
| `--data-dt` | 是 | 分区日期 |
| `--output-csv` | 否 | 输出 CSV 路径，默认 `old_summary_data.csv` |
| `--cluster` | 否 | 集群名称，默认 `old` |

## 输出格式

CSV 文件（Tab 分隔），字段说明：

| 字段 | 说明 |
|------|------|
| table_name | 表名 |
| partition_col | 分区字段名 |
| metric_name | 指标名称 |
| value | 指标值 |
| computed_at | 计算时间 |
| data_dt | 分区日期 |

## 模式切换

在 `env_config.json` 中修改 `use_ssh`：
- `true`：本地测试时使用 SSH 远程执行
- `false`：上线后使用本地直接执行

## 执行流程

```
Config → SQL Generator → SQL Template → Query Executor → CSV
```
