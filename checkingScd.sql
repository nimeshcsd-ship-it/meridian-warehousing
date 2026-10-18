-- 1. Batch 2: Append 15% price increase for items 1 through 20
INSERT INTO meridian_t07.bronze.item
SELECT 
    i_item_sk,
    i_item_id,
    i_rec_start_date,
    i_rec_end_date,
    i_item_desc,
    round(i_current_price * 1.15, 2) AS i_current_price,
    i_wholesale_cost,
    i_brand_id,
    i_brand,
    i_class_id,
    i_class,
    i_category_id,
    i_category,
    i_manufact_id,
    i_manufact,
    i_size,
    i_formulation,
    i_color,
    i_units,
    i_container,
    i_manager_id,
    i_product_name,
    current_timestamp() AS _ingestion_time
FROM samples.tpcds_sf1.item
WHERE i_item_sk <= 20;

-- 2. Batch 3: Append 30% price increase (timestamp offset by +5 seconds)
INSERT INTO meridian_t07.bronze.item
SELECT 
    i_item_sk,
    i_item_id,
    i_rec_start_date,
    i_rec_end_date,
    i_item_desc,
    round(i_current_price * 1.30, 2) AS i_current_price,
    i_wholesale_cost,
    i_brand_id,
    i_brand,
    i_class_id,
    i_class,
    i_category_id,
    i_category,
    i_manufact_id,
    i_manufact,
    i_size,
    i_formulation,
    i_color,
    i_units,
    i_container,
    i_manager_id,
    i_product_name,
    current_timestamp() + INTERVAL 5 SECONDS AS _ingestion_time
FROM samples.tpcds_sf1.item
WHERE i_item_sk <= 20;

SELECT 
    i_item_sk,
    i_product_name,
    i_current_price,
    __start_at,
    __end_at,
    CASE 
        WHEN __end_at IS NULL THEN 'Current (Active)' 
        ELSE 'Historical (Expired)' 
    END AS status
FROM meridian_t07.silver.dim_item
WHERE i_item_sk = 1
ORDER BY __start_at ASC;