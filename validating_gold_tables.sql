SELECT 'dim_warehouse' AS table_name, count(*) AS row_count FROM meridian_t07.gold.dim_warehouse
UNION ALL
SELECT 'dim_date', count(*) FROM meridian_t07.gold.dim_date
UNION ALL
SELECT 'fct_inventory', count(*) FROM meridian_t07.gold.fct_inventory
UNION ALL
SELECT 'fct_weekly_demand', count(*) FROM meridian_t07.gold.fct_weekly_demand;