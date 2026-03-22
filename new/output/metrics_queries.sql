SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as computed_at,
    count(1) as row_count,
    sum(cast(condition_amount as decimal(38,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(38,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(38,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';