-- =====================================================================
-- MERIDIAN PROJECT: PHASE 5 - QUALITY GATE & AUDIT LOG
-- File: 06_quality_gate.sql
-- Catalog: meridian_t07 | Schema: ops
-- =====================================================================

-- 1. Create the Ops Quality Results Table
CREATE TABLE IF NOT EXISTS meridian_t07.ops.dq_gate_results (
    check_name STRING,
    check_status STRING,
    metric_value BIGINT,
    check_timestamp TIMESTAMP
);

-- 2. Check 1: Verify Gold Fact Inventory is not empty
INSERT INTO meridian_t07.ops.dq_gate_results
SELECT 
    'fct_inventory_row_count' AS check_name,
    CASE WHEN count(*) > 0 THEN 'PASSED' ELSE 'FAILED' END AS check_status,
    count(*) AS metric_value,
    current_timestamp() AS check_timestamp
FROM meridian_t07.gold.fct_inventory;

-- 3. Check 2: Check for invalid negative stock in the Gold Fact
INSERT INTO meridian_t07.ops.dq_gate_results
SELECT 
    'fct_inventory_negative_stock' AS check_name,
    CASE WHEN count(*) = 0 THEN 'PASSED' ELSE 'FAILED' END AS check_status,
    count(*) AS metric_value,
    current_timestamp() AS check_timestamp
FROM meridian_t07.gold.fct_inventory
WHERE quantity_on_hand < 0;

-- 4. Check 3: Verify the Weeks of Cover Metric View returns rows
INSERT INTO meridian_t07.ops.dq_gate_results
SELECT 
    'metric_view_populated' AS check_name,
    CASE WHEN count(*) > 0 THEN 'PASSED' ELSE 'FAILED' END AS check_status,
    count(*) AS metric_value,
    current_timestamp() AS check_timestamp
FROM meridian_t07.gold.v_inventory_weeks_of_cover;

-- 5. Quality Gate Assertion: Raise error and fail task if any check FAILED
SELECT assert_true(
    (SELECT count(*) FROM meridian_t07.ops.dq_gate_results 
     WHERE check_timestamp >= current_timestamp() - INTERVAL 5 MINUTES 
       AND check_status = 'FAILED') = 0,
    'DATA QUALITY GATE FAILED: One or more data quality checks failed in meridian_t07.gold'
);