SELECT 
    warehouse_name,
    warehouse_state,
    item_key,
    i_product_name,
    closing_stock,
    avg_weekly_demand,
    weeks_of_cover
FROM meridian_t07.gold.v_inventory_weeks_of_cover
WHERE warehouse_state = :param_state
ORDER BY weeks_of_cover ASC;
