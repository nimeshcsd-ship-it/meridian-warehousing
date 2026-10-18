-- =====================================================================
-- MERIDIAN PROJECT: PHASE 4 - GOVERNANCE (RLS & COLUMN MASKING)
-- File: 05_governance.sql
-- Catalog: meridian_t07 | Schemas: gov, gold, silver
-- =====================================================================

-- 1. Create User-to-State Entitlement Table in 'gov'
CREATE OR REPLACE TABLE meridian_t07.gov.user_state_entitlements (
    user_email STRING,
    allowed_state STRING
)
COMMENT 'User-to-state mapping table for fine-grained row-level security';

-- 2. Seed Initial Entitlements
-- Maps current deployer to Tennessee and grants workspace admins full visibility
INSERT INTO meridian_t07.gov.user_state_entitlements 
VALUES 
    (current_user(), 'TN'),
    (current_user(), 'ALL');

-- 3. Row Filter UDF: Evaluates admin group membership and mapping table
CREATE OR REPLACE FUNCTION meridian_t07.gov.fn_warehouse_rls(state_col STRING)
RETURN 
    is_account_group_member('t07_admins')
    OR EXISTS (
        SELECT 1 
        FROM meridian_t07.gov.user_state_entitlements e
        WHERE e.user_email = current_user() 
          AND (e.allowed_state = 'ALL' OR e.allowed_state = state_col)
    );

-- 4. Column Masking UDF: Package 07 requires masking financial columns (i_wholesale_cost)
-- Only members of t07_pii_readers or t07_admins can see raw financial values
CREATE OR REPLACE FUNCTION meridian_t07.gov.fn_mask_financial(val DOUBLE)
RETURN 
    CASE 
        WHEN is_account_group_member('t07_pii_readers') 
          OR is_account_group_member('t07_admins') 
        THEN val 
        ELSE NULL 
    END;

-- 5. Attach Row Filters to Reporting Tables
ALTER TABLE meridian_t07.gold.dim_warehouse 
SET ROW FILTER meridian_t07.gov.fn_warehouse_rls ON (warehouse_state);

ALTER TABLE meridian_t07.gold.fct_inventory 
SET ROW FILTER meridian_t07.gov.fn_warehouse_rls ON (warehouse_state);




-- Check active user context and function evaluation
SELECT 
    current_user() AS my_email,
    is_account_group_member('t07_admins') AS is_admin,
    meridian_t07.gov.fn_warehouse_rls('TN') AS can_see_tn,
    meridian_t07.gov.fn_warehouse_rls('CA') AS can_see_ca;

-- Query the row-filtered warehouse dimension
SELECT warehouse_key, warehouse_name, warehouse_city, warehouse_state 
FROM meridian_t07.gold.dim_warehouse;



SELECT 
    i_item_sk, 
    i_product_name, 
    i_current_price, 
    i_wholesale_cost 
FROM meridian_t07.silver.dim_item
WHERE i_wholesale_cost IS NOT NULL
LIMIT 10;

-- setting the col lvl mask

ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_wholesale_cost 
SET MASK meridian_t07.gov.fn_mask_financial;


SELECT 
    i_item_sk, 
    i_product_name, 
    i_current_price, 
    i_wholesale_cost 
FROM meridian_t07.silver.dim_item 
LIMIT 10;


-- Apply Governed Tags

-- Tag the Medallion schemas
ALTER SCHEMA meridian_t07.bronze SET TAGS ('layer' = 'bronze', 'confidentiality' = 'internal');
ALTER SCHEMA meridian_t07.silver SET TAGS ('layer' = 'silver', 'confidentiality' = 'internal');
ALTER SCHEMA meridian_t07.gold SET TAGS ('layer' = 'gold', 'confidentiality' = 'general');
ALTER SCHEMA meridian_t07.gov SET TAGS ('layer' = 'governance', 'confidentiality' = 'restricted');

-- Tag sensitive columns
ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_wholesale_cost 
SET TAGS ('data_classification' = 'financial_confidential');

--Audit query
SELECT 
    table_catalog,
    table_schema,
    table_name,
    column_name,
    mask_name
FROM meridian_t07.information_schema.column_masks
WHERE table_catalog = 'meridian_t07';


-- 1. Apply mask to the second pricing/financial column
ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_current_price 
SET MASK meridian_t07.gov.fn_mask_financial;

-- 2. Tag the column for classification compliance
ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_current_price 
SET TAGS ('data_classification' = 'financial_confidential');