-- Query 1: Data Quality Expectations Summary (Ops Schema)
SELECT 
    check_name,
    SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) AS passed_count,
    SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pass_rate_pct
FROM meridian_t07.ops.dq_gate_results
GROUP BY check_name
ORDER BY pass_rate_pct ASC;

-- Query 2: Pipeline Ingestion Health & Batch Volume History
SELECT 
    DATE(_ingestion_time) AS run_date,
    _batch_number AS batch_id,
    COUNT(*) AS total_rows_ingested
FROM meridian_t07.bronze.inventory
GROUP BY 1, 2
ORDER BY run_date DESC, batch_id DESC;

-- Query 3: Query Execution History & Latency Trends
SELECT 
    statement_type,
    execution_status,
    total_duration_ms,
    produced_rows,
    start_time
FROM system.query.history
WHERE start_time >= '2026-10-01'
ORDER BY start_time DESC
LIMIT 50;

-- Query 4: Governed Inventory Detail with Masked Product Name
SELECT 
    w.warehouse_name,
    w.warehouse_state,
    meridian_t07.gov.fn_mask_product_name(i.i_product_name) AS i_product_name,
    closing_stock,
    weeks_of_cover
FROM meridian_t07.gold.v_inventory_weeks_of_cover v
JOIN meridian_t07.gold.dim_warehouse w ON v.warehouse_key = w.warehouse_key
JOIN meridian_t07.silver.dim_item i ON v.item_key = i.i_item_sk
WHERE __END_AT IS NULL
ORDER BY w.warehouse_name, i.i_product_name;