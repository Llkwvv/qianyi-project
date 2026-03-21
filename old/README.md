# Old 集群 Hive 指标校验脚本

## 概述

用于旧集群的 Hive 指标校验工具，支持两阶段流程：
1. **SQL 生成阶段**：读取配置文件，生成 SQL 模板
2. **执行阶段**：执行 SQL 并导出 CSV 结果

## 开发规范

### 1. 设计原则

- **两阶段分离**：SQL 生成与执行分离，各司其职
- **配置驱动**：通过 JSON 配置文件定义库表信息
- **动态展开**：结果按指标列展开，而非固定格式

### 2. 流程架构

```
Config → SQL Generator → SQL Template → Query Executor → CSV
```

### 3. 输入输出规范

#### 阶段1：SQL 生成

**输入** (`sample_tables.json`)：
- `tables[].name`：库表名（必填）
- `tables[].partition_cols`：分区列数组（必填，可为空 `[]`）
- `tables[].fields[]`：字段数组（必填，仅包含 decimal 类型）

**输出** (`metrics_queries.sql`)：
- 仅 `{{data_dt}}` 占位符
- 分区字段直接输出为字符串，别名 `partition_col`
- 每表输出：`row_count` + 各 decimal 字段的 `sum`

#### 阶段2：执行查询

**输入**：
- SQL 文件（多语句以分号分隔）
- `--data-dt`：分区日期

**输出** (CSV)：
- `table_name`、`partition_col`、`metric_name`、`value`、`computed_at`、`data_dt`

### 4. SQL 模板生成规则

| 场景 | 生成内容 |
|------|----------|
| 分区字段 | `'{dt}' as partition_col`（直接输出字段名） |
| 无分区 | `'' as partition_col` |
| WHERE 条件 | `WHERE dt = '{{data_dt}}'`（仅当有分区时） |
| 行数 | `count(1) as row_count` |
| Decimal 字段 | `sum(cast(xxx as decimal(38,2))) as xxx_sum` |

### 5. 执行模式

通过 `env_config.json` 中 `use_ssh` 配置切换：
- `true`：SSH 远程执行（本地测试用）
- `false`：本地直接执行（上线后用）

## 目录结构

```
old/
├── env_config.json              # 环境配置
├── sample_tables.json           # 库表配置
├── hive_metrics_sql_generator.py # SQL 生成脚本
├── run_hive_queries_python.py   # 执行脚本
├── README.md                    # 技术文档
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
        {"name": "condition_amount", "type": "decimal(16,2)"},
        {"name": "benefit_amount", "type": "decimal(16,2)"},
        {"name": "benefit_discount", "type": "decimal(16,2)"}
      ]
    }
  ]
}
```

## 使用方法

### 阶段1：生成 SQL 模板

```bash
python old/hive_metrics_sql_generator.py \
  --table-list old/sample_tables.json \
  --output-dir old/output
```

**输出**：`old/output/metrics_queries.sql`

生成的 SQL 示例：
```sql
SELECT
    'gmall.dim_activity_full' as table_name,
    '2024-01-01' as data_dt,
    'dt' as partition_col,
    current_timestamp() as computed_at,
    count(1) as row_count,
    sum(cast(condition_amount as decimal(38,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(38,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(38,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '2024-01-01';
```

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

| 字段 | 说明 | 示例 |
|------|------|------|
| table_name | 表名 | gmall.dim_activity_full |
| partition_col | 分区字段名 | dt |
| metric_name | 指标名称 | row_count, condition_amount_sum |
| value | 指标值 | 0 |
| computed_at | 计算时间 | 2026-03-22 01:00:00 |
| data_dt | 分区日期 | 2024-01-01 |

## 模式切换

在 `env_config.json` 中修改 `use_ssh`：
- `true`：本地测试时使用 SSH 远程执行
- `false`：上线后使用本地直接执行

## 执行流程详解

### 1. SQL 生成流程

```
1. 读取 sample_tables.json
2. 遍历每个表：
   - 拼接 table_name
   - 拼接 data_dt 占位符
   - 拼接 partition_col（分区字段名或空字符串）
   - 添加 computed_at
   - 添加 row_count
   - 遍历 fields，添加 decimal 字段的 sum
3. 按分号分隔写入 metrics_queries.sql
```

### 2. 查询执行流程

```
1. 读取 env_config.json 获取集群配置
2. 根据 use_ssh 决定执行模式
3. 读取 SQL 文件，按分号拆分成多条语句
4. 对每条语句：
   - 替换 {{data_dt}} 为实际日期
   - 执行 Hive 查询
   - 解析结果，过滤提示符
   - 动态展开指标列
5. 合并所有结果，导出 CSV
```

### 3. 指标展开逻辑

原始查询结果（1 行多列）：
| table_name | data_dt | partition_col | computed_at | row_count | condition_amount_sum |
|------------|---------|---------------|-------------|-----------|---------------------|
| xxx | 2024-01-01 | dt | 2026-... | 0 | NULL |

展开后（1 列1行）：
| table_name | partition_col | metric_name | value | computed_at | data_dt |
|------------|---------------|-------------|-------|-------------|---------|
| xxx | dt | row_count | 0 | 2026-... | 2024-01-01 |
| xxx | dt | condition_amount_sum | NULL | 2026-... | 2024-01-01 |

## 常见问题

### Q1: 本地测试时报错 "beeline: command not found"
- 确保 `env_config.json` 中 `use_ssh` 为 `true`，使用 SSH 远程执行

### Q2: 上线后如何切换到本地执行
- 将 `env_config.json` 中 `use_ssh` 改为 `false`

### Q3: 分区字段为 NULL
- 检查 `sample_tables.json` 中 `partition_cols` 配置
- 确认分区字段名填写正确（如 `dt`、`pt`）

### Q4: SQL 执行报错 "Expression not in GROUP BY"
- 这是开发过程中的问题，已修复：分区字段不再参与聚合，直接输出为字符串
