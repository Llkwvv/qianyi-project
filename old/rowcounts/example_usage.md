# 修改后的表行数对比工具使用说明

## 功能变更
原来的工具将对比结果写入MySQL数据库，现在改为：
1. 将对比结果写入CSV文件
2. 可选地将CSV文件加载到Hive表

## 配置文件 (env_config.json)
```json
{
  "metastore_mysql": {
    "host": "172.20.10.6",
    "port": 3306,
    "user": "root",
    "password": "000000",
    "db": "metastore"
  },
  "hive": {
    "database": "default",
    "table": "table_comparison",
    "hdfs_path": "/user/hive/warehouse"
  },
  "csv_dir": "/home/lkw/qianyi-project/new/input"
}
```

## 命令行参数

### 基本用法
```bash
python compare_num_rows.py --data-dt 20250324
```

### 指定输入文件
```bash
python compare_num_rows.py --data-dt 20250324 \
  --old-csv /path/to/20250324_old_table_stats.csv \
  --new-csv /path/to/20250324_new_table_stats.csv
```

### 指定输出目录
```bash
python compare_num_rows.py --data-dt 20250324 \
  --output-dir /path/to/output
```

### 指定Hive表名
```bash
python compare_num_rows.py --data-dt 20250324 \
  --hive-table my_table_comparison
```

### 跳过Hive加载
```bash
python compare_num_rows.py --data-dt 20250324 --skip-hive
```

## 输出文件格式
生成的CSV文件包含以下列：
- database_name: 数据库名
- table_name: 表名
- partition_name: 分区名
- metric_name: 指标名（固定为'num_rows'）
- old_value: 旧集群行数
- new_value: 新集群行数
- diff_value: 行数差值（new - old）
- data_dt: 数据日期
- compare_date: 对比日期（当前日期）
- cluster_type: 集群类型标识（固定为'table_comparison'）

## Hive表结构
数据会加载到以下结构的Hive表中：
```sql
CREATE EXTERNAL TABLE table_comparison (
    database_name STRING,
    table_name STRING,
    partition_name STRING,
    metric_name STRING,
    old_value BIGINT,
    new_value BIGINT,
    diff_value BIGINT,
    data_dt STRING,
    compare_date STRING,
    cluster_type STRING
)
PARTITIONED BY (data_dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/default.db/table_comparison'
TBLPROPERTIES ('skip.header.line.count'='1')
```

## 执行流程
1. 检查并等待输入文件准备好（最多等待240分钟）
2. 读取旧集群和新集群的CSV数据
3. 比较表行数，计算差值
4. 将结果导出到CSV文件
5. （可选）将CSV文件加载到Hive表

## 注意事项
1. CSV文件采用逗号分隔，NULL值用空字符串表示
2. Hive加载步骤需要Hive命令行工具（hive）可用
3. 如果Hive加载失败，CSV文件仍会保留，可以手动加载
4. 默认会创建Hive表（如果不存在）并添加分区