-- Hive 对比查询模板
-- 用于对比新旧集群指标并计算差值
-- 占位符: {{validation_db}}, {{table_name}}, {{data_dt}}

SELECT
    old.table_name,
    old.partition_col,
    old.metric_name,
    old.data_dt,
    old.value AS old_value,
    new.value AS new_value,
    CASE
        WHEN old.value IS NULL OR old.value = '' THEN NULL
        WHEN new.value IS NULL OR new.value = '' THEN NULL
        ELSE CAST(CAST(new.value AS DOUBLE) - CAST(old.value AS DOUBLE) AS STRING)
    END AS diff,
    CASE
        WHEN old.value IS NULL OR old.value = '' THEN 'MISSING_OLD'
        WHEN new.value IS NULL OR new.value = '' THEN 'MISSING_NEW'
        WHEN CAST(old.value AS DOUBLE) = CAST(new.value AS DOUBLE) THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    CASE
        WHEN old.value IS NULL OR old.value = '' THEN '旧集群缺失'
        WHEN new.value IS NULL OR new.value = '' THEN '新集群缺失'
        WHEN CAST(old.value AS DOUBLE) = CAST(new.value AS DOUBLE) THEN NULL
        ELSE CONCAT('值不一致: ', old.value, ' vs ', new.value)
    END AS reason
FROM (
    SELECT * FROM {{validation_db}}.{{table_name}}
    WHERE data_dt = '{{data_dt}}' AND cluster = 'old'
) old
FULL OUTER JOIN (
    SELECT * FROM {{validation_db}}.{{table_name}}
    WHERE data_dt = '{{data_dt}}' AND cluster = 'new'
) new
ON old.table_name = new.table_name
   AND old.partition_col = new.partition_col
   AND old.metric_name = new.metric_name;