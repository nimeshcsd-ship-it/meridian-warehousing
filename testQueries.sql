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