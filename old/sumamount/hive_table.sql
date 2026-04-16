CREATE TABLE IF NOT EXISTS {{database}}.{{table}} (
    database_name STRING,
    table_name STRING,
    partition_name STRING,
    metric_name STRING,
    old_value STRING,
    new_value STRING,
    diff_value STRING,
    etl_tm STRING
)
PARTITIONED BY (data_dt STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
