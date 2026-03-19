-- 数据质量校验结果表

-- old_summary: 旧集群校验结果
CREATE TABLE IF NOT EXISTS validation_db.old_summary (
    run_id         STRING COMMENT '任务运行ID',
    table_name     STRING COMMENT '被检查的表名',
    check_type     STRING COMMENT '检查类型',
    metric_name    STRING COMMENT '指标名称',
    metric_expr    STRING COMMENT '指标计算表达式',
    value          STRING COMMENT '计算结果值',
    where_hash     STRING COMMENT 'WHERE条件哈希',
    partition_spec STRING COMMENT '分区信息',
    data_dt        STRING COMMENT '数据日期',
    computed_at    TIMESTAMP COMMENT '计算时间'
)
COMMENT '旧集群数据质量校验结果表'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- new_summary: 新集群校验结果
CREATE TABLE IF NOT EXISTS validation_db.new_summary (
    run_id         STRING COMMENT '任务运行ID',
    table_name     STRING COMMENT '被检查的表名',
    check_type     STRING COMMENT '检查类型',
    metric_name    STRING COMMENT '指标名称',
    metric_expr    STRING COMMENT '指标计算表达式',
    value          STRING COMMENT '计算结果值',
    where_hash     STRING COMMENT 'WHERE条件哈希',
    partition_spec STRING COMMENT '分区信息',
    data_dt        STRING COMMENT '数据日期',
    computed_at    TIMESTAMP COMMENT '计算时间'
)
COMMENT '新集群数据质量校验结果表'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- compare_result: 校验对比结果
CREATE TABLE IF NOT EXISTS validation_db.compare_result (
    run_id         STRING COMMENT '任务运行ID',
    table_name     STRING COMMENT '被检查的表名',
    check_type     STRING COMMENT '检查类型',
    metric_name    STRING COMMENT '指标名称',
    partition_spec STRING COMMENT '分区信息',
    status         STRING COMMENT '校验状态 PASS/FAIL',
    old_value      STRING COMMENT '旧集群值',
    new_value      STRING COMMENT '新集群值',
    diff           DOUBLE COMMENT '差值',
    reason         STRING COMMENT '差异原因',
    data_dt        STRING COMMENT '数据日期',
    compared_at    TIMESTAMP COMMENT '对比时间'
)
COMMENT '数据质量校验对比结果表'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- runs: 任务运行记录
CREATE TABLE IF NOT EXISTS validation_db.runs (
    run_id      STRING COMMENT '任务运行ID',
    batch_id    STRING COMMENT '迁移批次ID',
    env         STRING COMMENT '环境标签',
    start_time  TIMESTAMP COMMENT '开始时间',
    end_time    TIMESTAMP COMMENT '结束时间',
    status      STRING COMMENT '状态 SUCCESS/FAILED',
    config_hash STRING COMMENT '配置文件哈希',
    created_by  STRING COMMENT '创建人',
    note        STRING COMMENT '备注',
    data_dt     STRING COMMENT '数据日期'
)
COMMENT '数据质量校验任务运行记录表'
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');
