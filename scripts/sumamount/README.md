# SumAmount 指标对比工具集

本项目提供了一套完整的工具链，用于在新旧Hive集群之间生成、比较和验证指标数据。主要用于验证集群迁移过程中的数据一致性。

## 目录结构

```
sumamount/
├── generate_metrics_artifact.py    # 生成单个集群的指标中间结果
├── compare_metrics_artifact.py     # 对比新旧集群的指标差异
├── metrics_artifact_common.py    # 共享工具函数
├── hive_metrics_sql_generator.py # 根据配置生成SQL查询模板
├── env_config.json               # 环境配置文件
├── sample_tables.json             # 示例表配置
└── README.md                     # 本文件
```

## 主要脚本说明

### 1. `generate_metrics_artifact.py`

生成指定集群的指标中间结果文件（TSV格式）。

**功能**：
- 执行Hive SQL查询并提取指标数据
- 支持并行执行多个SQL任务
- 输出标准化的TSV格式中间结果

**参数**：
- `--sql-file`: SQL文件路径（必需）
- `--data-dt`: 分区日期，如`2024-01-01`（必需）
- `--cluster`: 集群名称（`old`或`new`）（必需）
- `--output-file`: 输出TSV文件路径（可选，默认基于配置生成）

**使用示例**：
```bash
# 为旧集群生成指标
python3 generate_metrics_artifact.py \
    --sql-file sql1.sql \
    --data-dt "2024-01-01" \
    --cluster "old"

# 为新集群生成指标
python3 generate_metrics_artifact.py \
    --sql-file sql1.sql \
    --data-dt "2024-01-01" \
    --cluster "new"
```

### 2. `compare_metrics_artifact.py`

对比新旧集群生成的指标中间结果，并输出差异报告。

**功能**：
- 加载新旧集群的TSV格式中间结果
- 严格匹配主键（表名、分区列、指标名、数据日期）
- 计算数值差异并生成CSV报告
- 支持等待机制，确保输入文件已生成

**参数**：
- `--data-dt`: 分区日期，如`2024-01-01`（必需）
- `--old-artifact`: 旧集群TSV文件路径（可选，默认自动生成）
- `--new-artifact`: 新集群TSV文件路径（可选，默认自动生成）
- `--output-file`: 输出CSV文件路径（可选，默认自动生成）
- `--skip-hive`: 跳过写入Hive步骤（仅用于测试）

**使用示例**：
```bash
# 对比指标并生成报告
python3 compare_metrics_artifact.py --data-dt 2024-01-01

# 指定具体文件路径
python3 compare_metrics_artifact.py \
    --data-dt 2024-01-01 \
    --old-artifact output/2024-01-01/old_metrics.tsv \
    --new-artifact output/2024-01-01/new_metrics.tsv \
    --output-file output/2024-01-01/2024-01-01_metric_comparison.csv
```

### 3. `hive_metrics_sql_generator.py`

根据JSON配置文件生成Hive SQL查询模板。

**功能**：
- 读取表配置JSON文件
- 为每张表生成包含占位符的SQL查询
- 支持多种指标类型（行数、decimal字段求和等）

**参数**：
- `--table-list`: 库表配置文件路径（必需）
- `--output-dir`: 输出目录（必需）

**使用示例**：
```bash
python3 hive_metrics_sql_generator.py \
    --table-list sample_tables.json \
    --output-dir .
```


## 配置文件

### 1. `env_config.json`

环境配置文件，定义了运行时参数和集群连接信息。

```json
{
  "artifact_dir": "output",  // 中间结果输出目录
  "runtime": {              // 运行时配置
    "max_workers": 4,        // 最大并行工作线程数
    "max_retries": 0,        // 查询失败重试次数
    "query_timeout_sec": 1800, // 查询超时时间（秒）
    "poll_interval_sec": 300, // 轮询间隔（秒）
    "wait_timeout_sec": 14400 // 等待超时时间（秒）
  },
  "clusters": {            // 集群配置
    "old": {                 // 旧集群
      "beeline_url": "jdbc:hive2://localhost:10000/default",
      "username": "atguigu",
      "use_ssh": true,        // 是否通过SSH连接
      "ssh_host": "172.20.10.6",
      "ssh_port": 22,
      "ssh_user": "atguigu"
    },
    "new": {                 // 新集群
      "beeline_url": "jdbc:hive2://localhost:10000/default",
      "username": "atguigu",
      "use_ssh": false        // 直接连接
    }
  },
  "metastore_mysql": {       // Metastore数据库配置
    "host": "172.20.10.6",
    "port": 3306,
    "user": "root",
    "password": "000000",
    "db": "metastore"
  },
  "csv_dir": "output"        // CSV输出目录
}
```

### 2. `sample_tables.json`

示例表配置文件，定义了需要监控的表及其字段。

```json
{
  "tables": [
    {
      "name": "gmall.dim_activity_full",  // 表名
      "partition_cols": ["dt"],          // 分区字段
      "fields": [                        // 需要监控的字段
        {"name": "condition_amount", "type": "decimal(16,2)"},
        {"name": "benefit_amount", "type": "decimal(16,2)"},
        {"name": "benefit_discount", "type": "decimal(16,2)"}
      ]
    }
  ]
}
```

## 工作流程

1. **准备阶段**：
   - 配置`env_config.json`中的集群连接信息
   - 确保SQL文件已生成并位于指定位置

2. **SQL文件位置**：
   - SQL文件路径：`/home/lkw/qianyi-project/old/sumamount/output/generated_sql/metrics_queries.sql`
   - 此SQL文件由`hive_metrics_sql_generator.py`生成，包含所有需要执行的指标查询
   - 使用时直接引用此文件，无需每次重新生成

3. **生成指标**：
   - 为旧集群生成指标
   - 为新集群生成指标

4. **对比分析**：
   ```bash
   python3 compare_metrics_artifact.py --data-dt 2024-01-01
   ```

5. **自动化执行**：
   ```bash
   ./run_full_workflow.sh
   ```

## 注意事项

- 所有脚本都假设Python 3.6+环境
- 确保Hive客户端（beeline）已正确安装并配置
- SSH连接需要配置好密钥认证
- SQL文件中可以使用`{{data_dt}}`作为日期占位符，会被自动替换
- 输出文件默认保存在`output/<data_dt>/`目录下
