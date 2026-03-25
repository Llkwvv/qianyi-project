-- 表对比结果表（支持多种指标）
CREATE TABLE IF NOT EXISTS table_comparison (
    id INT AUTO_INCREMENT PRIMARY KEY,
    database_name VARCHAR(128) COMMENT '数据库名',
    table_name VARCHAR(256) COMMENT '表名',
    partition_name VARCHAR(512) COMMENT '分区名',
    metric_name VARCHAR(64) COMMENT '指标名',
    old_value BIGINT COMMENT '旧集群值',
    new_value BIGINT COMMENT '新集群值',
    diff_value BIGINT COMMENT '差值(new_value - old_value)',
    data_dt VARCHAR(16) COMMENT '数据日期',
    etl_tm TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL时间',
    INDEX idx_db_table (database_name, table_name),
    INDEX idx_data_dt (data_dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='表对比结果（支持多种指标）';