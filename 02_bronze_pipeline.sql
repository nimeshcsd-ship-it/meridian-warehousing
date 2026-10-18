-- 1. Bronze Inventory Table
CREATE OR REFRESH STREAMING TABLE meridian_t07.bronze.inventory
COMMENT "Incremental append-only ingestion of raw inventory snapshot files"
AS
SELECT 
    inv_date_sk,
    inv_item_sk,
    inv_warehouse_sk,
    inv_quantity_on_hand,
    _metadata.file_path AS _source_file_path,
    current_timestamp() AS _ingestion_time,
    '1' AS _batch_number
FROM STREAM read_files(
    '/Volumes/meridian_t07/landing/raw_files/inventory/*/*',
    format => 'json'
);

-- 2. Bronze Item Table (Required by silver_pipeline.sql for SCD Type 2)
CREATE OR REFRESH STREAMING TABLE meridian_t07.bronze.item
COMMENT "Incremental append-only ingestion of raw item dimension data"
AS
SELECT 
    *,
    current_timestamp() AS _ingestion_time
FROM STREAM(samples.tpcds_sf1.item);