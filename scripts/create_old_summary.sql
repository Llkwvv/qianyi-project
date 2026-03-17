CREATE TABLE IF NOT EXISTS validation_db.old_summary (
    table_name      STRING COMMENT '被检查的表名',
    check_type      STRING COMMENT '检查类型',
    metric_name     STRING COMMENT '指标名称',
    metric_expr     STRING COMMENT '指标计算表达式',
    value           STRING COMMENT '计算结果值',
    partition_spec  STRING COMMENT '分区信息',
    computed_at     TIMESTAMP COMMENT '计算时间',
    data_dt        STRING COMMENT '数据日期'
)
COMMENT '数据质量校验结果摘要表'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
