-- 表行数对比结果表
-- 用于存储新旧集群表行数对比数据

CREATE EXTERNAL TABLE IF NOT EXISTS {{database}}.{{table}} (
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
LOCATION '{{hdfs_base_path}}/{{database}}.db/{{table}}'
TBLPROPERTIES ('skip.header.line.count'='1');