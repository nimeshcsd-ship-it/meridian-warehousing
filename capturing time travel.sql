


-- Step 1: Find valid versions
DESCRIBE HISTORY meridian_t07.bronze.inventory;

-- Step 2: Use the earliest available version:
-- After running DESCRIBE HISTORY above, replace <valid_version_number> with the actual number (e.g., 3 or 5).
-- Example below assumes version 3 is earliest. Update if needed.
SELECT 'Earliest Available State' AS batch_state, COUNT(*) AS total_rows
FROM meridian_t07.bronze.inventory VERSION AS OF 3
UNION ALL
SELECT 'Current Append State' AS batch_state, COUNT(*) AS total_rows
FROM meridian_t07.bronze.inventory;