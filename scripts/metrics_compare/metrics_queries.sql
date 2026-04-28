SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_sku_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(price as decimal(16,2))) as price_sum,
    sum(cast(weight as decimal(16,2))) as weight_sum
FROM gmall.dim_sku_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_province_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.dim_province_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_category1_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_category1_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_dic_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_dic_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';



SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_sku_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(price as decimal(16,2))) as price_sum,
    sum(cast(weight as decimal(16,2))) as weight_sum
FROM gmall.dim_sku_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_province_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.dim_province_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_category1_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_category1_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_dic_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_dic_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';



SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_sku_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(price as decimal(16,2))) as price_sum,
    sum(cast(weight as decimal(16,2))) as weight_sum
FROM gmall.dim_sku_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_province_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.dim_province_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_category1_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_category1_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_dic_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_dic_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';



SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_sku_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(price as decimal(16,2))) as price_sum,
    sum(cast(weight as decimal(16,2))) as weight_sum
FROM gmall.dim_sku_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_province_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.dim_province_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_category1_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_category1_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_dic_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_dic_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';



SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_sku_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(price as decimal(16,2))) as price_sum,
    sum(cast(weight as decimal(16,2))) as weight_sum
FROM gmall.dim_sku_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_province_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.dim_province_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_category1_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_category1_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_dic_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_dic_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';



SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_sku_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(price as decimal(16,2))) as price_sum,
    sum(cast(weight as decimal(16,2))) as weight_sum
FROM gmall.dim_sku_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_province_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.dim_province_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_category1_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_category1_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.ods_base_dic_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts
FROM gmall.ods_base_dic_full
WHERE dt = '{{data_dt}}';

SELECT
    'gmall.dim_activity_full' as table_name,
    '{{data_dt}}' as data_dt,
    'dt' as partition_col,
    current_timestamp() as etl_tm,
    count(1) as row_counts,
    sum(cast(condition_amount as decimal(16,2))) as condition_amount_sum,
    sum(cast(benefit_amount as decimal(16,2))) as benefit_amount_sum,
    sum(cast(benefit_discount as decimal(16,2))) as benefit_discount_sum
FROM gmall.dim_activity_full
WHERE dt = '{{data_dt}}';



