-- Step 1: Delta Compaction and Z-Ordering
OPTIMIZE meridian_t07.gold.fct_inventory 
ZORDER BY (warehouse_key, date_key, item_key);

OPTIMIZE meridian_t07.gold.fct_weekly_demand 
ZORDER BY (warehouse_key, item_key);

OPTIMIZE meridian_t07.silver.dim_item 
ZORDER BY (i_item_sk);

-- Step 2: Cost-Based Optimizer (CBO) Table Statistics
ANALYZE TABLE meridian_t07.gold.fct_inventory COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE meridian_t07.gold.dim_warehouse COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE meridian_t07.gold.dim_date COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE meridian_t07.silver.dim_item COMPUTE STATISTICS FOR ALL COLUMNS;


-- Post-Optimization Benchmark Query
-- Benchmark Query: Post-Optimization Assessment
/* benchmark_run: optimized_after */
SELECT 
    w.warehouse_name,
    w.warehouse_state,
    i.i_item_desc AS product_name,
    d.calendar_date,
    SUM(f.quantity_on_hand) AS total_inventory_units,
    COUNT(DISTINCT f.item_key) AS unique_item_count
FROM meridian_t07.gold.fct_inventory f
JOIN meridian_t07.gold.dim_warehouse w 
    ON f.warehouse_key = w.warehouse_key
JOIN meridian_t07.silver.dim_item i 
    ON f.item_key = i.i_item_sk
JOIN meridian_t07.gold.dim_date d 
    ON f.date_key = d.date_key
WHERE w.warehouse_state = 'TN'
  AND d.calendar_date >= DATE '2001-01-01'
GROUP BY 
    w.warehouse_name,
    w.warehouse_state,
    i.i_item_desc,
    d.calendar_date
ORDER BY SUM(f.quantity_on_hand) DESC
LIMIT 100;