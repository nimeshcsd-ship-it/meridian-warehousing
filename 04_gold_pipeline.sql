-- =====================================================================
-- MERIDIAN PROJECT: PHASE 3 - GOLD STAR SCHEMA & METRIC VIEWS
-- File: 04_gold_pipeline.sql
-- Catalog: meridian_t07 | Target Schema: gold
-- =====================================================================

-- 1. Dim Warehouse (Regional Dimension with State Hierarchy)
CREATE OR REPLACE TABLE meridian_t07.gold.dim_warehouse
COMMENT 'Warehouse dimension with regional attributes for operational hierarchy'
AS
SELECT 
    w_warehouse_sk AS warehouse_key,
    w_warehouse_id AS warehouse_id,
    w_warehouse_name AS warehouse_name,
    w_city AS warehouse_city,
    w_state AS warehouse_state,
    w_country AS warehouse_country
FROM samples.tpcds_sf1.warehouse;

-- 2. Dim Date (Conformed Calendar Dimension)
CREATE OR REPLACE TABLE meridian_t07.gold.dim_date
COMMENT 'Conformed calendar date dimension'
AS
SELECT 
    d_date_sk AS date_key,
    d_date AS calendar_date,
    d_year AS year,
    d_moy AS month_of_year,
    d_qoy AS quarter,
    d_week_seq AS week_seq
FROM samples.tpcds_sf1.date_dim;

-- 3. Fact Inventory (Snapshot Grain: date_key, item_key, warehouse_key)
CREATE OR REPLACE TABLE meridian_t07.gold.fct_inventory
COMMENT 'Periodic snapshot fact table of warehouse inventory'
AS
SELECT 
    inv.date_key,
    inv.item_key,
    inv.warehouse_key,
    w.warehouse_state,
    inv.quantity_on_hand
FROM meridian_t07.silver.inventory inv
LEFT JOIN meridian_t07.gold.dim_warehouse w
    ON inv.warehouse_key = w.warehouse_key;

-- 4. Fact Weekly Demand with Partition Pruning on cs_sold_date_sk
CREATE OR REPLACE TABLE meridian_t07.gold.fct_weekly_demand
COMMENT 'Aggregated weekly sales demand from catalog_sales with partition pruning'
AS
SELECT 
    cs.cs_item_sk AS item_key,
    cs.cs_warehouse_sk AS warehouse_key,
    d.d_week_seq AS week_seq,
    sum(cs.cs_quantity) AS weekly_demand_units
FROM samples.tpcds_sf1.catalog_sales cs
JOIN samples.tpcds_sf1.date_dim d
    ON cs.cs_sold_date_sk = d.d_date_sk
WHERE cs.cs_warehouse_sk IS NOT NULL
  -- Partition Pruning: restricts scan to the target snapshot year
  AND cs.cs_sold_date_sk BETWEEN 2451911 AND 2452275
GROUP BY cs.cs_item_sk, cs.cs_warehouse_sk, d.d_week_seq;

-- 5. Governed Metric View: Semi-Additive Stock & Weeks of Cover
CREATE OR REPLACE VIEW meridian_t07.gold.v_inventory_weeks_of_cover 
COMMENT 'Governed metric view computing semi-additive closing stock and weeks of cover'
AS
WITH latest_snapshot AS (
    SELECT 
        item_key,
        warehouse_key,
        quantity_on_hand AS closing_stock,
        ROW_NUMBER() OVER(
            PARTITION BY warehouse_key, item_key 
            ORDER BY date_key DESC
        ) AS rn
    FROM meridian_t07.gold.fct_inventory
),
avg_demand AS (
    SELECT 
        item_key,
        warehouse_key,
        avg(weekly_demand_units) AS avg_weekly_demand
    FROM meridian_t07.gold.fct_weekly_demand
    GROUP BY item_key, warehouse_key
)
SELECT 
    ls.warehouse_key,
    w.warehouse_name,
    w.warehouse_state,
    ls.item_key,
    dim.i_product_name,
    dim.i_current_price,
    ls.closing_stock,
    round(coalesce(ad.avg_weekly_demand, 0), 2) AS avg_weekly_demand,
    CASE 
        WHEN coalesce(ad.avg_weekly_demand, 0) = 0 THEN NULL
        ELSE round(ls.closing_stock / ad.avg_weekly_demand, 2)
    END AS weeks_of_cover
FROM latest_snapshot ls
JOIN meridian_t07.gold.dim_warehouse w
    ON ls.warehouse_key = w.warehouse_key
JOIN meridian_t07.silver.dim_item dim
    ON ls.item_key = dim.i_item_sk AND dim.__end_at IS NULL
LEFT JOIN avg_demand ad
    ON ls.item_key = ad.item_key AND ls.warehouse_key = ad.warehouse_key
WHERE ls.rn = 1;