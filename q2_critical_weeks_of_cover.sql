SELECT 
    warehouse_name,
    warehouse_state,
    item_key,
    i_product_name,
    closing_stock,
    avg_weekly_demand,
    weeks_of_cover
FROM meridian_t07.gold.v_inventory_weeks_of_cover
WHERE weeks_of_cover IS NOT NULL 
  AND weeks_of_cover <= COALESCE(TRY_CAST(:param_max_cover AS DOUBLE), 4.0)
ORDER BY weeks_of_cover ASC;