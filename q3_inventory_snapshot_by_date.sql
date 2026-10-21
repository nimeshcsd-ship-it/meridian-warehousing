SELECT 
    f.date_key,
    d.calendar_date,
    f.warehouse_key,
    f.warehouse_state,
    f.item_key,
    f.quantity_on_hand
FROM meridian_t07.gold.fct_inventory f
JOIN meridian_t07.gold.dim_date d 
    ON f.date_key = d.date_key
WHERE d.calendar_date >= CAST(:param_start_date AS DATE)
ORDER BY d.calendar_date DESC
LIMIT 100;