-- Data Date: 2024-01-01
-- Total SELECT statements: 2

SELECT 'gmall.dwd_trade_pv_inc' as table_name, cast(count(1) as string) as row_count, '0' as amount_sum, '2024-01-01' as partition_spec, current_timestamp() as computed_at, '2024-01-01' as data_dt FROM gmall.dwd_trade_pv_inc GROUP BY '2024-01-01';
SELECT 'gmall.dwd_traffic_page_view_inc' as table_name, cast(count(1) as string) as row_count, '2024-01-01' as partition_spec, current_timestamp() as computed_at, '2024-01-01' as data_dt FROM gmall.dwd_traffic_page_view_inc GROUP BY '2024-01-01';
