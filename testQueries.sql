SELECT 
    current_user() AS my_email,
    is_account_group_member('t07_admins') AS am_i_admin,
    is_account_group_member('t07_pii_readers') AS am_i_pii_reader;
   -- 1. Test the CLS masking function:
-- If am_i_pii_reader is FALSE, this returns NULL. If TRUE, it returns 45.50.
    SELECT meridian_t07.gov.fn_mask_financial(45.50) AS test_mask_result;



    SELECT meridian_t07.gov.fn_mask_financial(45.50) AS test_mask_result;


    -- If am_i_admin is TRUE, both return TRUE.
-- If am_i_admin is FALSE, only the state assigned to you in gov.warehouse_entitlements returns TRUE.
SELECT 
    meridian_t07.gov.fn_warehouse_rls('TN') AS can_see_tn,
    meridian_t07.gov.fn_warehouse_rls('CA') AS can_see_ca;



    SELECT 
    i_item_sk,
    i_product_name,
    i_wholesale_cost,
    i_current_price
FROM meridian_t07.silver.dim_item
LIMIT 10;

SELECT 
    item_key,
    i_product_name,
    i_wholesale_cost,
    i_current_price
FROM meridian_t07.silver.dim_item
LIMIT 15;










-- verifying rls !!!!!!
SELECT * FROM meridian_t07.gov.user_state_entitlements;

SELECT DISTINCT warehouse_state, warehouse_name 
FROM meridian_t07.gold.dim_warehouse;

SELECT distinct warehouse_state
FROM meridian_t07.gold.dim_warehouse ;

DELETE FROM meridian_t07.gov.user_state_entitlements 
WHERE user_email = 'nimeshcsd@gmail.com' AND allowed_state = 'ALL';

INSERT INTO meridian_t07.gov.user_state_entitlements VALUES
('nimeshcsd@gmail.com', 'CA'),
('nimeshcsd@gmail.com', 'NY');\




SELECT i_product_name, i_current_price, i_wholesale_cost 
FROM meridian_t07.silver.dim_item 
LIMIT 10;



-- testing whether system tables are accessible in  workspace
SELECT * FROM system.lakeflow.jobs LIMIT 5;
SELECT * FROM system.billing.usage LIMIT 5;


SELECT 
    table_schema, 
    table_name, 
    table_type 
FROM system.information_schema.tables 
WHERE table_schema IN ('query', 'access', 'compute', 'lakeflow')
ORDER BY table_schema, table_name 
LIMIT 20;

-- Reconciliation counts using actual table names
SELECT 
    (SELECT COUNT(*) FROM meridian_t07.bronze.inventory) AS bronze_raw_count,
    (SELECT COUNT(*) FROM meridian_t07.silver.inventory) AS silver_clean_count,
    (SELECT COUNT(*) FROM meridian_t07.ops.inventory_quarantine) AS ops_quarantine_count,
    ((SELECT COUNT(*) FROM meridian_t07.silver.inventory) + 
     (SELECT COUNT(*) FROM meridian_t07.ops.inventory_quarantine)) AS silver_plus_quarantine_reconciled;





     SELECT 
    i_item_sk,
    i_product_name,
    i_current_price,
    __start_at,
    __end_at
FROM meridian_t07.silver.dim_item
WHERE i_item_sk IN (
    SELECT i_item_sk 
    FROM meridian_t07.silver.dim_item 
    GROUP BY i_item_sk 
    HAVING COUNT(*) > 1
)
ORDER BY i_item_sk, __start_at ASC;






SELECT * 
FROM meridian_t07.gold.vdp_zero_stock_items 
LIMIT 15;







SELECT 
    'Metric View' AS metric_source,
    SUM(closing_stock) AS total_closing_units,
    ROUND(AVG(weeks_of_cover), 2) AS avg_cover_weeks
FROM meridian_t07.gold.v_inventory_weeks_of_cover
UNION ALL
SELECT 
    'Independent Fact Calculation' AS metric_source,
    SUM(quantity_on_hand) AS total_closing_units,
    ROUND(AVG(quantity_on_hand / NULLIF(d.avg_demand, 0)), 2) AS avg_cover_weeks
FROM (
    SELECT 
        warehouse_key, 
        item_key, 
        quantity_on_hand,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_key, item_key 
            ORDER BY date_key DESC
        ) AS rn
    FROM meridian_t07.gold.fct_inventory
) snap
JOIN (
    SELECT 
        item_key, 
        warehouse_key, 
        AVG(weekly_demand_units) AS avg_demand
    FROM meridian_t07.gold.fct_weekly_demand
    GROUP BY item_key, warehouse_key
) d ON snap.warehouse_key = d.warehouse_key AND snap.item_key = d.item_key
WHERE snap.rn = 1;








SELECT 
    catalog_name AS entity_catalog, 
    schema_name AS entity_schema, 
    NULL AS entity_column, 
    tag_name, 
    tag_value 
FROM meridian_t07.information_schema.schema_tags 
WHERE catalog_name = 'meridian_t07'
UNION ALL
SELECT 
    schema_name, 
    table_name, 
    column_name, 
    tag_name, 
    tag_value 
FROM meridian_t07.information_schema.column_tags 
WHERE schema_name = 'meridian_t07';




SELECT 
    warehouse_state, 
    warehouse_name, 
    COUNT(*) AS record_count
FROM meridian_t07.gold.dim_warehouse
GROUP BY warehouse_state, warehouse_name
ORDER BY warehouse_state;