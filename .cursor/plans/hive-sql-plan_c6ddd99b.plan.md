---
name: hive-sql-plan
overview: 将现有两段脚本拆成清晰的两阶段：1) 从库表配置 JSON（仅 decimal 字段）生成 Hive SQL 模板（仅 `{{data_dt}}` 占位符）并输出 metrics_manifest.json；2) 读取一个 .sql 文件（多条语句以分号分隔），执行并替换占位符，按 manifest 展开结果导出 CSV（不含 partition_spec，metric_name 为原字段名_sum）。
todos:
  - id: plan-gen-template-manifest
    content: 重构 `scripts/hive_metrics_sql_generator.py`：读取 sample_tables.json，生成 SQL 模板（仅 {{data_dt}} 占位符）；partition_cols 列从配置直接写死；WHERE 按 partition_cols 是否为空决定；输出 metrics_manifest.json。
    status: pending
  - id: plan-exec-param-replace-transform
    content: 重构 `scripts/run_hive_queries_python.py`：输入为单个 .sql 文件；--data-dt 替换占位符；每条语句独立执行得到 df，按 manifest 展开；最终 CSV 不含 partition_spec，metric_name 为 原字段_sum。
    status: pending
  - id: plan-config-and-doc
    content: 补充示例配置与脚本参数说明（在脚本帮助或注释中）。
    status: pending
isProject: false
---

## 目标

- `scripts/hive_metrics_sql_generator.py`：读取库表配置 JSON（`scripts/sample_tables.json`），生成 `metrics_queries.sql`（SQL 模板）及 `metrics_manifest.json`。
- `scripts/run_hive_queries_python.py`：读取一个 `.sql` 文件（多条语句以分号分隔），执行每条 SQL，用 `--data-dt` 替换占位符，按 manifest 展开结果导出 CSV（不含 partition_spec，metric_name 为 原字段名_sum）。

## 阶段1：SQL 生成脚本规划（生成模板 + manifest）

### 1. 明确输入/输出

- 输入：`scripts/sample_tables.json`，结构如下（**所有 key 均必填**，`partition_cols` 值可为空数组 `[]`）：
  - `tables[].name`（`db.table`，必填）
  - `tables[].partition_cols`（分区列数组，必填，可为 `[]`）
  - `tables[].fields[]`（必填，元素为 `{name, type}`，**仅包含 decimal 类型**）
- 输出到 `--output-dir`：
  - `metrics_queries.sql`：多条 `SELECT ... FROM db.table`，每表一行宽表结果。
  - `metrics_manifest.json`：按语句顺序记录 `table_name` 与 metrics 映射。

### 2. SQL 模板内容

- 占位符：**仅 `{{data_dt}}`**，用于输出列 `data_dt` 及可选 WHERE 条件。
- `partition_cols` 列：**不使用占位符**，直接从配置文件中的 `partition_cols` 生成（如 `["dt"]` 转为字符串 `"dt"` 写入 SELECT；若为空则输出空字符串）。
- WHERE 条件：
  - 若 `partition_cols` 不为空：`WHERE <第一个分区列> = '{{data_dt}}'`
  - 若 `partition_cols` 为空：无 WHERE 条件。
- 聚合：不输出分区列，metrics 为 `count(1)` 及各 decimal 的 `sum(...)`，无需 GROUP BY。

### 3. 指标列生成与 manifest

- 每表必有 `row_count`：`count(1)`。
- 每个 decimal 字段：`sum(cast(col as decimal(p,s)))`，manifest 中 `name` 为 `原字段名_sum`（如 `condition_amount_sum`）。
- manifest：`sql_index`、`table_name`、`metrics=[{name, expr}]`。

## 阶段2：SQL 执行脚本规划（执行 + 整理导出 CSV）

### 1. 明确输入/输出与 CLI

- 输入：**一个 `.sql` 文件**，内含多条语句，以分号 `;` 分隔。
- 执行参数：
  - `--sql-file`：`.sql` 文件路径。
  - `--manifest`：`metrics_manifest.json` 路径（用于按 metric 列展开结果）。
  - `--data-dt`：替换模板中的 `{{data_dt}}`。
- 输出：`--output-csv`，默认 `scripts/old_summary_data.csv`。

### 2. 读取并拆分 SQL 文件

- 读取该 `.sql` 文件，按分号 `;` 拆分出每条语句。
- 按语句顺序与 manifest 的 `sql_index` 一一对应。

### 3. 替换占位符并执行 Hive

- 对每条语句执行前，将 `{{data_dt}}` 替换成 `--data-dt` 传入的值。
- 沿用当前 `execute_hive_query()` 的 SSH 方式执行。
- 输出解析：按 tab-separated 构造 DataFrame，跳过空行，取首行 header + 数据行。

### 4. 中间 DataFrame 与展开逻辑

- 每条语句执行结果是一个独立的 DataFrame，**字段数量因表/指标不同而不一致，无法直接纵向拼接**。
- 对每条语句得到的 DataFrame，单独按 manifest 展开：遍历该条对应的 `metrics`，每列生成一行，再 append 到最终结果。

### 5. 整理为最终 CSV 并导出

- 结果行字段（**不含 partition_spec**）：
  - `table_name`：从查询输出取。
  - `check_type`：与 `metric_name` 一致。
  - `metric_name`：来自 manifest 的 `metrics[].name`。decimal 字段为 **原表字段名 + `_sum`**（如 `condition_amount_sum`）；内置 `row_count` 即 `row_count`。
  - `metric_expr`：来自 manifest。
  - `value`：来自 DataFrame 对应 metric 列。
  - `computed_at`、`data_dt`：从 DataFrame 取。
- 输出：按 `sep='\t'` 写入 `output-csv`。

## 交付物

- 修改文件：`scripts/hive_metrics_sql_generator.py`、`scripts/run_hive_queries_python.py`
- 运行时生成：`output-dir/metrics_queries.sql`、`output-dir/metrics_manifest.json`、`output-csv`（如 `scripts/old_summary_data.csv`）

## 运行示例（规划层面，待你确认后我再落代码）

- 生成：
  - `python scripts/hive_metrics_sql_generator.py --table-list scripts/sample_tables.json --output-dir output`
- 执行：
  - `python scripts/run_hive_queries_python.py --sql-file output/metrics_queries.sql --manifest output/metrics_manifest.json --data-dt 2024-01-01 --output-csv scripts/old_summary_data.csv`

## 结构关系图

```mermaid
flowchart LR
  Config[sample_tables.json
库表配置] --> Gen[SQL生成脚本
hive_metrics_sql_generator.py]
  Gen --> SQL[metrics_queries.sql
SQL模板]
  Gen --> Manifest[metrics_manifest.json
指标映射]
  SQL --> Exec[执行脚本
run_hive_queries_python.py]
  Manifest --> Exec
  Exec --> Hive[远端Hive(ssh执行)]
  Exec --> CSV[导出CSV
不含partition_spec]
```



