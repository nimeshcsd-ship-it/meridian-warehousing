-- 1. Silver Table with Data Quality Expectations
CREATE OR REFRESH STREAMING TABLE meridian_t07.silver.inventory(
    CONSTRAINT valid_item_key EXPECT (item_key IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_warehouse_key EXPECT (warehouse_key IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_quantity EXPECT (quantity_on_hand >= 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned, validated, and deduplicated inventory snapshot data"
AS
SELECT 
    cast(inv_date_sk AS INT) AS date_key,
    cast(inv_item_sk AS INT) AS item_key,
    cast(inv_warehouse_sk AS INT) AS warehouse_key,
    cast(inv_quantity_on_hand AS INT) AS quantity_on_hand,
    _source_file_path,
    _ingestion_time
FROM STREAM(meridian_t07.bronze.inventory);

-- 2. Quarantine Table (Querying directly from Bronze where raw column names exist)
CREATE OR REFRESH STREAMING TABLE meridian_t07.ops.inventory_quarantine
COMMENT "Quarantined inventory records failing Silver data quality checks"
AS
SELECT 
    inv_date_sk,
    inv_item_sk,
    inv_warehouse_sk,
    inv_quantity_on_hand,
    _source_file_path,
    _ingestion_time
FROM STREAM(meridian_t07.bronze.inventory)
WHERE inv_item_sk IS NULL 
   OR inv_warehouse_sk IS NULL 
   OR inv_quantity_on_hand < 0;


-- Target SCD Type 2 Table in Silver
CREATE OR REFRESH STREAMING TABLE meridian_t07.silver.dim_item;

-- Apply Changes to track price changes over time
APPLY CHANGES INTO meridian_t07.silver.dim_item
FROM STREAM(meridian_t07.bronze.item)
KEYS (i_item_sk)
SEQUENCE BY _ingestion_time
STORED AS SCD TYPE 2;

