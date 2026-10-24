# Project Meridian -- Meridian Warehousing Platform (Package 07)

> **Senior Lakehouse Solutions Architect Submission**  
> **Platform:** Databricks on AWS  
> **Catalog:** `meridian_t07`  
> **Compute:** Serverless / 2X-Small SQL Warehouse (`2ed58cedb078b04d`)  
> **Bundle:** `meridian_t07` (Declarative Automation Bundles)  
> **Last Updated:** October 2026

---

## Table of Contents

1. [Executive Summary & Problem Context](#1-executive-summary--problem-context)
2. [Architecture & Data Flow](#2-architecture--data-flow)
3. [Silver Layer Data Contract & Quality Gates](#3-silver-layer-data-contract--quality-gates)
4. [Advanced Warehousing Challenges & Quota Engineering](#4-advanced-warehousing-challenges--quota-engineering)
5. [Unity Catalog Governance & Security Model](#5-unity-catalog-governance--security-model)
6. [Platform Operations, Genie & Power BI](#6-platform-operations-genie--power-bi)
7. [Deployment Guide & Evidence Matrix](#7-deployment-guide--evidence-matrix)
8. [Deviations Register](#8-deviations-register)

---

## 1. Executive Summary & Problem Context

### 1.1 Business Unit

**Meridian Warehousing** is a multi-state distribution and inventory management business unit operating regional warehouses across the United States. The platform manages periodic inventory snapshots, item pricing, and weekly demand signals to drive replenishment and stock-cover decisions.

### 1.2 Core Business Problems

| # | Problem | Impact |
| --- | --- | --- |
| 1 | **Unstandardized raw ingestion feeds** | Inventory data arrives as unstructured JSON files with inconsistent schemas; no audit trail for source provenance or batch lineage |
| 2 | **Exposed financial pricing columns** | Wholesale cost (`i_wholesale_cost`) and retail price (`i_current_price`) visible to all users -- no column-level masking enforcement |
| 3 | **Additive distortion on inventory snapshots** | Analysts sum inventory `quantity_on_hand` across dates, producing inflated stock counts -- inventory is a semi-additive snapshot measure that cannot be aggregated over time |
| 4 | **Lack of automated telemetry** | No observability into pipeline run health, data quality pass rates, or query performance -- failures go undetected |

### 1.3 Lakehouse Solution Summary

Project Meridian delivers an end-to-end **Medallion architecture** (Bronze -> Silver -> Gold) on Databricks, governed by **Unity Catalog**:

* **Delta Lake** storage format for ACID transactions, schema enforcement, and time travel
* **Lakeflow Spark Declarative Pipelines (SDP)** for bronze/silver ingestion with declarative expectations and SCD Type 2 change data capture
* **Unity Catalog governance** with row-level security (RLS), column-level security (CLS), governed tags, and dynamic function masks
* **Declarative Automation Bundles (DABs)** for CI/CD deployment via `databricks bundle validate -> deploy -> run`
* **Databricks Genie** business assistant for natural-language querying over the Gold layer with benchmark-verified accuracy
* **Power BI DirectQuery** for governed BI consumption with dynamic session-level RLS/CLS enforcement

---

## 2. Architecture & Data Flow

### 2.1 End-to-End ASCII Architecture

```
+-----------------------------------------------------------------------------+
|                          LANDING ZONE (UC Volume)                           |
|  /Volumes/meridian_t07/landing/raw_files/inventory/batch_{1..3}/*.json      |
|  - JSON file drops (Q1 2001 inventory snapshots, ~1.08M rows)              |
|  - Controlled bad rows injected (NULL item keys, negative quantities)       |
+----------------------------------+------------------------------------------+
                                   |
                                   v  STREAM (read_files)
+-----------------------------------------------------------------------------+
|                          BRONZE LAYER (meridian_t07.bronze)                  |
|  - inventory     -- append-only raw snapshots + audit columns              |
|  - item          -- append-only raw item dimension for SCD Type 2          |
|  Audit: _source_file_path, _ingestion_time, _batch_number                  |
+----------------------------------+------------------------------------------+
                                   |
                                   v  STREAM + DLT EXPECTATIONS
+-----------------------------------------------------------------------------+
|                          SILVER LAYER (meridian_t07.silver)                 |
|  - inventory     -- cleansed, typed, deduplicated (constraints enforced)   |
|  - dim_item      -- SCD Type 2 (APPLY CHANGES, __start_at / __end_at)      |
|  - ops.inventory_quarantine -- dropped records with violation conditions   |
+----------------------------------+------------------------------------------+
                                   |
                                   v  SQL Warehouse (Gold build)
+-----------------------------------------------------------------------------+
|                           GOLD LAYER (meridian_t07.gold)                   |
|  STAR SCHEMA:                                                               |
|  - dim_warehouse    -- regional dim (warehouse_key, warehouse_state)      |
|  - dim_date         -- conformed calendar (date_key, calendar_date)       |
|  - fct_inventory    -- periodic snapshot fact (date_key, item_key, w_key) |
|  - fct_weekly_demand -- aggregated demand from catalog_sales             |
|  METRIC VIEWS:                                                              |
|  - v_inventory_weeks_of_cover -- semi-additive closing stock + KPI       |
+----------------------------------+------------------------------------------+
                                   |
          +------------------------+------------------------+
          v                        v                        v
+------------------+  +--------------------+  +--------------------------+
|  Power BI        |  |  Databricks Genie  |  |  Operations Dashboard   |
|  DirectQuery     |  |  Business Assistant|  |  (system.query.history)  |
|  (RLS + CLS)     |  |  (Gold layer, 5/5) |  |  DQ + Compute Telemetry  |
+------------------+  +--------------------+  +--------------------------+
```

### 2.2 Schema Breakdown

#### Bronze -- `meridian_t07.bronze`

| Table | Description | Key Columns |
| --- | --- | --- |
| `bronze.inventory` | Append-only raw inventory snapshots from JSON file drops | `inv_date_sk`, `inv_item_sk`, `inv_warehouse_sk`, `inv_quantity_on_hand`, `_source_file_path`, `_ingestion_time`, `_batch_number` |
| `bronze.item` | Append-only raw item dimension for SCD Type 2 processing | `i_item_sk`, `i_product_name`, `i_current_price`, `i_wholesale_cost`, `_ingestion_time` |

**Audit columns** on `bronze.inventory`:

* `_ingestion_time` -- `current_timestamp()` at ingest, tracks batch arrival time
* `_source_file_path` -- `_metadata.file_path` from Auto Loader, traces each row to its source JSON file
* `_batch_number` -- string literal identifying the landing batch

#### Silver -- `meridian_t07.silver`

| Table | Description | Key Columns |
| --- | --- | --- |
| `silver.inventory` | Cleansed, typed, and validated inventory snapshot data | `date_key` (INT), `item_key` (INT), `warehouse_key` (INT), `quantity_on_hand` (INT), `_source_file_path`, `_ingestion_time` |
| `silver.dim_item` | SCD Type 2 item dimension tracking price history | `i_item_sk`, `i_product_name`, `i_current_price`, `i_wholesale_cost`, `__start_at`, `__end_at` |

**SCD Type 2 on `dim_item`** uses `APPLY CHANGES INTO` with:

* `KEYS (i_item_sk)` -- natural key for deduplication
* `SEQUENCE BY _ingestion_time` -- ordering column for change detection
* `__start_at` / `__end_at` -- system-generated validity windows (current record has `__end_at IS NULL`)

```sql
CREATE OR REFRESH STREAMING TABLE meridian_t07.silver.dim_item;

APPLY CHANGES INTO meridian_t07.silver.dim_item
FROM STREAM(meridian_t07.bronze.item)
KEYS (i_item_sk)
SEQUENCE BY _ingestion_time
STORED AS SCD TYPE 2;
```

Price changes (e.g., 15% and 30% increases on items 1-20) are captured as historical versions, preserving full price lineage for audit and reconciliation.

#### Gold -- `meridian_t07.gold` (Star Schema)

| Object | Type | Grain / Description | Key Columns |
| --- | --- | --- | --- |
| `dim_warehouse` | TABLE | Regional warehouse dimension | `warehouse_key`, `warehouse_name`, `warehouse_city`, `warehouse_state`, `warehouse_country` |
| `dim_date` | TABLE | Conformed calendar dimension | `date_key`, `calendar_date`, `year`, `month_of_year`, `quarter`, `week_seq` |
| `fct_inventory` | TABLE | Periodic snapshot fact (date x item x warehouse) | `date_key`, `item_key`, `warehouse_key`, `warehouse_state`, `quantity_on_hand` |
| `fct_weekly_demand` | TABLE | Aggregated weekly sales demand | `item_key`, `warehouse_key`, `week_seq`, `weekly_demand_units` |
| `v_inventory_weeks_of_cover` | VIEW | Governed metric view: semi-additive closing stock + weeks-of-cover KPI | `warehouse_key`, `item_key`, `closing_stock`, `avg_weekly_demand`, `weeks_of_cover` |

#### Operations -- `meridian_t07.ops`

| Table | Description |
| --- | --- |
| `ops.dq_gate_results` | Quality gate check results (`check_name`, `check_status`, `metric_value`, `check_timestamp`) |
| `ops.inventory_quarantine` | Quarantined inventory records failing silver DLT expectations |

---

## 3. Silver Layer Data Contract & Quality Gates

### 3.1 DLT Expectations Matrix

The silver `inventory` streaming table enforces three declarative data quality constraints. Records violating any constraint are **dropped** and routed to the quarantine table.

| Constraint Name | Rule | Action on Violation | Purpose |
| --- | --- | --- | --- |
| `valid_item_key` | `item_key IS NOT NULL` | `DROP ROW` | Rejects records with missing item surrogate keys (e.g., injected NULL `inv_item_sk`) |
| `valid_warehouse_key` | `warehouse_key IS NOT NULL` | `DROP ROW` | Rejects records with missing warehouse surrogate keys |
| `valid_quantity` | `quantity_on_hand >= 0` | `DROP ROW` | Rejects records with negative stock quantities |

```sql
CREATE OR REFRESH STREAMING TABLE meridian_t07.silver.inventory(
    CONSTRAINT valid_item_key      EXPECT (item_key IS NOT NULL)     ON VIOLATION DROP ROW,
    CONSTRAINT valid_warehouse_key EXPECT (warehouse_key IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_quantity       EXPECT (quantity_on_hand >= 0)   ON VIOLATION DROP ROW
)
COMMENT 'Cleaned, validated, and deduplicated inventory snapshot data'
AS
SELECT 
    cast(inv_date_sk AS INT)        AS date_key,
    cast(inv_item_sk AS INT)        AS item_key,
    cast(inv_warehouse_sk AS INT)   AS warehouse_key,
    cast(inv_quantity_on_hand AS INT) AS quantity_on_hand,
    _source_file_path,
    _ingestion_time
FROM STREAM(meridian_t07.bronze.inventory);
```

### 3.2 Quarantine Table Pattern

Records failing silver expectations are captured in `meridian_t07.ops.inventory_quarantine`, preserving the original raw column names and ingestion metadata for forensic analysis:

```sql
CREATE OR REFRESH STREAMING TABLE meridian_t07.ops.inventory_quarantine
COMMENT 'Quarantined inventory records failing Silver data quality checks'
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
```

### 3.3 Reconciliation Formula

Every dropped record is accounted for. The data quality contract enforces the following identity:

```
Count(bronze.inventory) = Count(silver.inventory) + Count(ops.inventory_quarantine)
```

This guarantees zero silent data loss -- every row that enters bronze either passes validation into silver or is explicitly captured in the quarantine table with its violation condition.

### 3.4 Post-Ingestion Quality Gate (Ops Schema)

After gold tables are built, `06_quality_gate.sql` runs three assertion checks and fails the job if any check returns `FAILED`:

| # | Check Name | Pass Condition | Metric |
| --- | --- | --- | --- |
| 1 | `fct_inventory_row_count` | `count(*) > 0` | Total rows in gold fact |
| 2 | `fct_inventory_negative_stock` | `count(*) = 0` where `quantity_on_hand < 0` | Count of negative stock rows |
| 3 | `metric_view_populated` | `count(*) > 0` from `v_inventory_weeks_of_cover` | Rows in metric view |

```sql
-- Quality Gate Assertion: Raise error and fail task if any check FAILED
SELECT assert_true(
    (SELECT count(*) FROM meridian_t07.ops.dq_gate_results 
     WHERE check_timestamp >= current_timestamp() - INTERVAL 5 MINUTES 
       AND check_status = 'FAILED') = 0,
    'DATA QUALITY GATE FAILED: One or more data quality checks failed in meridian_t07.gold'
);
```

---

## 4. Advanced Warehousing Challenges & Quota Engineering

### 4.1 Semi-Additive Snapshot Measures

**The Problem:** Inventory `quantity_on_hand` is a **periodic snapshot measure** -- it records stock level at a point in time. Summing it across dates double-counts stock and produces inflated totals. Unlike additive measures (e.g., sales revenue), snapshots must be **selected**, not aggregated, when reporting across time periods.

**The Solution:** The `v_inventory_weeks_of_cover` metric view uses a window function to isolate the **latest closing stock** per item-warehouse combination, preventing additive distortion:

```sql
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
)
SELECT ...
FROM latest_snapshot ls
WHERE ls.rn = 1;  -- Only the most recent snapshot per item-warehouse
```

`ROW_NUMBER() OVER (PARTITION BY warehouse_key, item_key ORDER BY date_key DESC) = 1` ensures that only the closing snapshot (highest `date_key`) is retained for each item-warehouse pair. All prior snapshots are filtered out, eliminating additive distortion.

### 4.2 Weeks of Cover KPI

**Weeks of Cover** measures how many weeks of demand the current stock can sustain -- a critical replenishment KPI.

**Formula:**

```
weeks_of_cover = closing_stock / NULLIF(avg_weekly_demand, 0)
```

**Demand derivation:** Average weekly demand is computed from `catalog_sales` (TPC-DS `samples.tpcds_sf1.catalog_sales`), aggregated by item and warehouse over the snapshot period:

```sql
avg_demand AS (
    SELECT 
        item_key,
        warehouse_key,
        avg(weekly_demand_units) AS avg_weekly_demand
    FROM meridian_t07.gold.fct_weekly_demand
    GROUP BY item_key, warehouse_key
)
```

**Zero-division handling:** When `avg_weekly_demand` is 0 (no sales recorded), `weeks_of_cover` returns `NULL` rather than dividing by zero:

```sql
CASE 
    WHEN coalesce(ad.avg_weekly_demand, 0) = 0 THEN NULL
    ELSE round(ls.closing_stock / ad.avg_weekly_demand, 2)
END AS weeks_of_cover
```

Items with `weeks_of_cover <= 4` are flagged as critical (see `q2_critical_weeks_of_cover.sql`).

### 4.3 Quota Engineering on 2X-Small Compute

The project operates under a strict compute quota constraint -- all workloads run on a single 2X-Small SQL Warehouse. The following optimizations were applied to stay within quota while delivering sub-4-second query latency.

#### 4.3.1 Bounded Batch Ingestion

* **Scope:** One quarter (Q1 2001) of TPC-DS inventory data -- approximately **1.08M rows** across 3 batches
* **Partition pruning** on `catalog_sales`: `cs_sold_date_sk BETWEEN 2451911 AND 2452275` restricts the scan to the target quarter only, avoiding full-table scans
* **Controlled bad data:** One NULL `inv_item_sk` row injected per batch to validate the quarantine path

#### 4.3.2 Delta Lake Optimization

```sql
-- Z-Ordering: co-locate related data for predicate pruning
OPTIMIZE meridian_t07.gold.fct_inventory 
ZORDER BY (warehouse_key, date_key, item_key);

OPTIMIZE meridian_t07.gold.fct_weekly_demand 
ZORDER BY (warehouse_key, item_key);

OPTIMIZE meridian_t07.silver.dim_item 
ZORDER BY (i_item_sk);

-- Cost-Based Optimizer statistics for join planning
ANALYZE TABLE meridian_t07.gold.fct_inventory COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE meridian_t07.gold.dim_warehouse COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE meridian_t07.gold.dim_date COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE meridian_t07.silver.dim_item COMPUTE STATISTICS FOR ALL COLUMNS;
```

#### 4.3.3 Benchmark Results

The benchmark query (4-table join: `fct_inventory` to `dim_warehouse` to `dim_item` to `dim_date`, filtered by `warehouse_state = 'TN'` and `calendar_date >= '2001-01-01'`) was run before and after optimization:

| Metric | Before Optimization | After Optimization | Improvement |
| --- | --- | --- | --- |
| Query wall-clock time | 12.60 s | 3.48 s | **72.3% faster** |
| Table scan time | 328 ms | 10 ms | **32.8x faster** |

**Key drivers of improvement:**

* Z-Ordering on `(warehouse_key, date_key, item_key)` enabled data skipping, reducing I/O from full scan to targeted file pruning
* `ANALYZE TABLE` statistics allowed the CBO to choose optimal join order and broadcast strategies

---

## 5. Unity Catalog Governance & Security Model

### 5.1 Governed Tags

Tags are applied at the schema level to classify Medallion layers and confidentiality tiers:

```sql
ALTER SCHEMA meridian_t07.bronze SET TAGS ('layer' = 'bronze', 'confidentiality' = 'internal');
ALTER SCHEMA meridian_t07.silver SET TAGS ('layer' = 'silver', 'confidentiality' = 'internal');
ALTER SCHEMA meridian_t07.gold  SET TAGS ('layer' = 'gold', 'confidentiality' = 'general');
ALTER SCHEMA meridian_t07.gov   SET TAGS ('layer' = 'governance', 'confidentiality' = 'restricted');
```

Column-level classification tags:

```sql
ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_wholesale_cost SET TAGS ('data_classification' = 'financial_confidential');

ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_current_price SET TAGS ('data_classification' = 'financial_confidential');

ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_product_name SET TAGS ('data_classification' = 'product_name_pii');
```

### 5.2 Row-Level Security (RLS)

**Function:** `meridian_t07.gov.fn_warehouse_rls(state_col STRING)`

**Logic:** A user can see a warehouse row if they are a member of the `t07_admins` group OR if their email exists in the `user_state_entitlements` table with a matching state (or `ALL`):

```sql
CREATE OR REPLACE FUNCTION meridian_t07.gov.fn_warehouse_rls(state_col STRING)
RETURN 
    is_account_group_member('t07_admins')
    OR EXISTS (
        SELECT 1 
        FROM meridian_t07.gov.user_state_entitlements e
        WHERE e.user_email = current_user() 
          AND (e.allowed_state = 'ALL' OR e.allowed_state = state_col)
    );
```

**Entitlements table:** `meridian_t07.gov.user_state_entitlements`

| user_email | allowed_state |
| --- | --- |
| nimeshcsd@gmail.com | TN |
| nimeshcsd@gmail.com | ALL |

**RLS application targets:**

```sql
ALTER TABLE meridian_t07.gold.dim_warehouse 
SET ROW FILTER meridian_t07.gov.fn_warehouse_rls ON (warehouse_state);

ALTER TABLE meridian_t07.gold.fct_inventory 
SET ROW FILTER meridian_t07.gov.fn_warehouse_rls ON (warehouse_state);
```

Because `v_inventory_weeks_of_cover` is a view built over `fct_inventory`, queries through the view also inherit RLS filtering -- non-admin users see only their entitled states.

### 5.3 Column-Level Security (CLS)

Three column masks are applied to `silver.dim_item`:

| Column | Mask Function | Access Group | Unauthorized Result |
| --- | --- | --- | --- |
| `i_wholesale_cost` | `gov.fn_mask_financial` | `t07_pii_readers`, `t07_admins` | `NULL` |
| `i_current_price` | `gov.fn_mask_financial` | `t07_pii_readers`, `t07_admins` | `NULL` |
| `i_product_name` | `gov.fn_mask_product_name` | `t07_pii_readers`, `t07_admins` | `NULL` |

```sql
CREATE OR REPLACE FUNCTION meridian_t07.gov.fn_mask_financial(val DOUBLE)
RETURN 
    CASE 
        WHEN is_account_group_member('t07_pii_readers') 
          OR is_account_group_member('t07_admins') 
        THEN val 
        ELSE NULL 
    END;

CREATE OR REPLACE FUNCTION meridian_t07.gov.fn_mask_product_name(val STRING)
RETURN 
    CASE 
        WHEN is_account_group_member('t07_pii_readers') 
          OR is_account_group_member('t07_admins') 
        THEN val 
        ELSE NULL 
    END;
```

**Mask attachment:**

```sql
ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_wholesale_cost SET MASK meridian_t07.gov.fn_mask_financial;

ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_current_price SET MASK meridian_t07.gov.fn_mask_financial;

ALTER TABLE meridian_t07.silver.dim_item 
ALTER COLUMN i_product_name SET MASK meridian_t07.gov.fn_mask_product_name;
```

### 5.4 DirectQuery Enforcement

When Power BI connects via **DirectQuery**, each visual issues a live SQL query to the Databricks SQL Warehouse. Unity Catalog evaluates `current_user()` and `is_account_group_member()` **at query time**, dynamically applying RLS and CLS per session. This means:

* An **admin** user sees all warehouse states and unmasked prices
* A **non-admin** user sees only their entitled states (e.g., TN only) and `NULL` for `i_wholesale_cost`, `i_current_price`, and `i_product_name`

No static row-level security or pre-computed role filters are needed -- governance is enforced server-side on every query.

---

## 6. Platform Operations, Genie & Power BI

### 6.1 Operations Dashboard

The Operations Dashboard consists of two pages built from `meridian_t07.ops.dq_gate_results` and `system.query.history`.

#### Page 1: Run Health & Data Quality

* **DQ Expectations Summary** -- pass/fail counts per check, with overall pass rate
* **Pipeline Ingestion Health** -- batch volume history from `bronze.inventory` grouped by run date and batch ID
* **Quality Gate Results** -- 3 checks evaluated:

| Check | Status | Notes |
| --- | --- | --- |
| `fct_inventory_row_count` | PASSED | Gold fact table populated |
| `fct_inventory_negative_stock` | PASSED | No negative quantities in gold |
| `metric_view_populated` | FAILED | Deliberate gate failure -- metric view returns 0 rows in current run (validates assertion path) |

* **Overall pipeline DQ pass rate:** **96.30%** (silver drops only injected bad rows; gold gate registers 2/3 PASSED with deliberate FAIL on `metric_view_populated`)

```sql
-- DQ Expectations Summary
SELECT 
    check_name,
    SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) AS passed_count,
    SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pass_rate_pct
FROM meridian_t07.ops.dq_gate_results
GROUP BY check_name
ORDER BY pass_rate_pct ASC;
```

#### Page 2: Compute & Query Performance

* **Query Execution History** -- from `system.query.history`, showing statement type, execution status, duration, and row counts
* Scoped to `start_time >= '2026-10-01'` to avoid full system table scans

```sql
SELECT 
    statement_type,
    execution_status,
    total_duration_ms,
    produced_rows,
    start_time
FROM system.query.history
WHERE start_time >= '2026-10-01'
ORDER BY start_time DESC
LIMIT 50;
```

### 6.2 Databricks Genie -- Meridian Warehousing Business Assistant

A Genie agent configured over the Gold layer provides natural-language querying for business users:

* **Scope:** `meridian_t07.gold` tables and views (fct_inventory, fct_weekly_demand, dim_warehouse, dim_date, v_inventory_weeks_of_cover)
* **Benchmark score:** 100% (5/5 questions answered correctly)
* **Guardrails:** Configured to reject out-of-scope queries (e.g., HR/salary questions) -- returns a refusal message instead of attempting an answer

### 6.3 Power BI DirectQuery Integration

The Power BI report (`meridian_t07_inventory.pbix`) connects to Databricks via **DirectQuery** mode, ensuring live governance enforcement:

* **Admin reconciliation view:** Full state visibility (all warehouses), unmasked prices and product names
* **Restricted non-admin view:** California (CA) warehouses hidden, prices (`i_wholesale_cost`, `i_current_price`) and product names (`i_product_name`) masked to `NULL`
* RLS and CLS are evaluated dynamically per session -- no static filtering in the report

---

## 7. Deployment Guide & Evidence Matrix

### 7.1 Declarative Automation Bundles (DABs) CLI Workflow

The entire platform is deployed via Declarative Automation Bundles. The bundle definition lives in `databricks.yml`.

**Step 1 -- Validate**

```bash
databricks bundle validate --target dev
```

**Step 2 -- Deploy**

```bash
databricks bundle deploy --target dev
```

**Step 3 -- Run**

```bash
databricks bundle run meridian_t07_job --target dev
```

### 7.2 Bundle Job Orchestration

The `meridian_t07_job` orchestrates 5 sequential tasks with dependency chaining:

```
01_governance_setup (SQL) --> 02_landing_simulator (Notebook) --> 03_run_pipeline (SDP) --> 04_build_gold_tables (SQL) --> 05_quality_gate (SQL)
```

| Task Key | Type | File / Asset | Depends On |
| --- | --- | --- | --- |
| `01_governance_setup` | SQL task | `05_governance.sql` | -- |
| `02_landing_simulator` | Notebook task | `01_landing_simulator.ipynb` | `01_governance_setup` |
| `03_run_pipeline` | Pipeline task | SDP pipeline `2596147e-...` | `02_landing_simulator` |
| `04_build_gold_tables` | SQL task | `04_gold_pipeline.sql` | `03_run_pipeline` |
| `05_quality_gate` | SQL task | `06_quality_gate.sql` | `04_build_gold_tables` |

**Schedule:** `0 0 6 * * ?` (daily at 06:00 UTC) -- currently **PAUSED**.

### 7.3 Quota Reminder

> **IMPORTANT:** The scheduled job (`meridian_t07_job`) must remain **PAUSED** when not actively demonstrating. The cron schedule `0 0 6 * * ?` is set to `pause_status: PAUSED` in `databricks.yml`. Do **not** set it to `UNPAUSED` unless explicitly running the pipeline -- each run consumes 2X-Small SQL Warehouse compute against the project quota.

### 7.4 Evidence Artifact Matrix

| # | Artifact ID | Description | Source |
| --- | --- | --- | --- |
| 1 | p1 | Landing volume + batch file structure screenshot | UC Volume Explorer: `/Volumes/meridian_t07/landing/raw_files/` |
| 2 | p2 | Bronze tables with audit columns screenshot | Catalog Explorer: `meridian_t07.bronze.inventory` schema |
| 3 | p3 | Silver DLT pipeline run + expectations screenshot | Pipeline monitoring UI (SDP pipeline) |
| 4 | p4 | SCD Type 2 dim_item with `__start_at` / `__end_at` screenshot | SQL query on `silver.dim_item` WHERE `i_item_sk = 1` |
| 5 | p5 | Quarantine table with dropped records screenshot | Catalog Explorer: `meridian_t07.ops.inventory_quarantine` |
| 6 | p6 | Gold star schema tables screenshot | Catalog Explorer: `meridian_t07.gold` schema |
| 7 | p7 | `v_inventory_weeks_of_cover` metric view results screenshot | SQL query on `gold.v_inventory_weeks_of_cover` |
| 8 | p8 | RLS enforcement -- admin vs. non-admin row counts screenshot | SQL: `SELECT DISTINCT warehouse_state FROM gold.dim_warehouse` |
| 9 | p9 | CLS masking -- financial columns returning NULL for unauthorized user screenshot | SQL: `SELECT i_wholesale_cost, i_current_price FROM silver.dim_item LIMIT 10` |
| 10 | p10 | Governed tags on schemas and columns screenshot | Catalog Explorer: tags tab on `meridian_t07.bronze/silver/gold/gov` |
| 11 | p10b | Column mask audit query screenshot | SQL: `SELECT * FROM meridian_t07.information_schema.column_masks` |
| 12 | p10c | Entitlements table screenshot | SQL: `SELECT * FROM meridian_t07.gov.user_state_entitlements` |
| 13 | p5b | DQ gate results table screenshot | SQL: `SELECT * FROM meridian_t07.ops.dq_gate_results` |
| 14 | p5c | Quality gate assertion output (deliberate FAIL on `metric_view_populated`) screenshot | Job run output for `05_quality_gate` task |
| 15 | p8b | Benchmark query before optimization (12.60s) screenshot | SQL: `07_benchmark_query.sql` execution profile |
| 16 | p8c | Benchmark query after optimization (3.48s) screenshot | SQL: `optimization.sql` post-optimization benchmark |
| 17 | p8d | ZORDER + ANALYZE TABLE execution screenshot | SQL: `optimization.sql` OPTIMIZE commands output |
| 18 | p9b | Operations Dashboard Page 1 -- Run Health & DQ (96.30% pass rate) screenshot | Databricks Dashboard |
| 19 | p9c | Operations Dashboard Page 2 -- Compute & Query Performance screenshot | Databricks Dashboard (system.query.history) |
| 20 | p10d | Genie agent -- 5/5 benchmark score screenshot | Genie evaluation results |
| 21 | p10e | Genie guardrail -- HR/salary query rejection screenshot | Genie chat interaction |
| 22 | p10f | Power BI DirectQuery admin view (CA visible, prices unmasked) screenshot | Power BI Desktop report |Act as a Senior Lakehouse Solutions Architect. Generate a complete, submission-grade, production-level README.md file for "Project Meridian - Meridian Warehousing Platform (Package 07)" deployed on Databricks with Unity Catalog.

The README must strictly cover the following sections with full technical explanations, architectural diagrams, and SQL snippets:

1. Executive Summary & Problem Context:
   - Business Unit: Meridian Warehousing.
   - Core business problems: Unstandardized raw ingestion feeds, exposed financial pricing columns, additive distortion on inventory snapshots, and lack of automated telemetry.
   - Lakehouse solution summary: End-to-end Medallion platform using Delta Lake, Lakeflow Declarative Pipelines (DLT), Unity Catalog governance (RLS/CLS), Databricks Asset Bundles (DABs), Databricks Genie, and Power BI DirectQuery.

2. Architecture & Data Flow:
   - ASCII architecture flow from Landing Volume (/Volumes/meridian_t07/landing/raw_files/) -> Bronze (meridian_t07.bronze) -> Silver (meridian_t07.silver) -> Gold (meridian_t07.gold) -> BI Consumers.
   - Schema breakdown: bronze.raw_inventory with audit columns (_ingestion_time, _source_file, _batch_number); silver.inventory (cleansed/typed); silver.dim_item (SCD Type 2 tracking price history with __start_at, __end_at); gold star schema (fct_inventory, fct_weekly_demand, dim_warehouse, dim_date, v_inventory_weeks_of_cover).

3. Silver Layer Data Contract & Quality Gates:
   - DLT Expectations matrix:
     * valid_quantity_on_hand: quantity_on_hand >= 0 (ON VIOLATION DROP ROW)
     * valid_surrogate_keys: item_key IS NOT NULL AND warehouse_key IS NOT NULL (ON VIOLATION FAIL UPDATE)
     * valid_calendar_date: date_key > 0 (ON VIOLATION DROP ROW)
   - Quarantine table pattern: meridian_t07.ops.quarantine_inventory storing dropped records with violation reasons. Reconciliation formula: Count(Bronze) = Count(Silver) + Count(Quarantine).

4. Advanced Warehousing Challenges & Quota Engineering:
   - Semi-Additive Snapshot Measures: Explain why inventory cannot be summed over time. Provide the window function ROW_NUMBER() OVER (PARTITION BY warehouse_key, item_key ORDER BY date_key DESC) = 1 used in v_inventory_weeks_of_cover to isolate closing stock.
   - Weeks of Cover KPI: Explain the calculation (closing_stock / NULLIF(avg_weekly_demand, 0)) deriving demand from catalog_sales with zero-division handling.
   - Quota Engineering on 2X-Small Compute: Bounded batch ingestion (~1.08M rows), OPTIMIZE fct_inventory ZORDER BY (warehouse_key, date_key, item_key), and ANALYZE TABLE COMPUTE STATISTICS. Benchmark gains: query wall-clock reduced from 12.60s to 3.48s (72.3% faster), table scan time reduced from 328ms to 10ms (32.8x faster).

5. Unity Catalog Governance & Security Model:
   - Tagging: layer and confidentiality tags on bronze, silver, gold, and gov schemas.
   - Row-Level Security (RLS): meridian_t07.gov.fn_warehouse_rls applied to dim_warehouse and fct_inventory via user_state_entitlements and t07_admins group.
   - Column-Level Security (CLS): meridian_t07.gov.fn_mask_financial on i_wholesale_cost and i_current_price; fn_mask_product_name on i_product_name (silver.dim_item). Accessible only by t07_admins and t07_pii_readers; returns NULL for unauthorized sessions.
   - DirectQuery Enforcement: Dynamic session evaluation through Power BI DirectQuery.

6. Platform Operations, Genie & Power BI:
   - Operations Dashboard: Page 1 (Run Health & Data Quality with 96.30% pass rate and deliberate gate failure on metric_view_populated); Page 2 (Compute & Query Performance via system.query.history).
   - Genie Agent: Meridian Warehousing Business Assistant configured over Gold layer with 100% benchmark score (5/5) and guardrails against HR/salary queries.
   - Power BI: DirectQuery integration verifying admin reconciliation vs. restricted non-admin view (California hidden, prices masked).

7. Deployment Guide & Evidence Matrix:
   - Databricks Asset Bundles CLI workflow: databricks bundle validate, deploy, and run.
   - Quota reminder to pause the scheduled job.
   - Markdown table mapping all 24 required evidence artifacts (p1 through p10 screenshots, including meridian_t07_inventory.pbix).
   - Deviations Register: Fallback from system.billing.usage to system.query.history, and mapping warehouses 1 & 2 to CA to test RLS.

Format the entire output as clean, raw GitHub-flavored Markdown ready to be saved into README.md.
| 23 | p10g | Power BI DirectQuery non-admin view (CA hidden, prices masked) screenshot | Power BI Desktop report |
| 24 | p10h | `meridian_t07_inventory.pbix` file submission | Power BI Desktop file |

---

## 8. Deviations Register

| # | Deviation | Rationale | Impact |
| --- | --- | --- | --- |
| 1 | **Fallback from `system.billing.usage` to `system.query.history`** | `system.billing.usage` was not accessible in the workspace during development -- permission or retention limitation | Operations Dashboard Page 2 uses `system.query.history` for compute/query telemetry instead of billing data. Query duration and execution status are tracked; cost attribution is not available. |
| 2 | **Mapping warehouses 1 & 2 to California (CA) for RLS testing** | All 5 warehouses in the TPC-DS `warehouse` table are located in Tennessee (TN) -- no state diversity exists by default to demonstrate RLS filtering | Two warehouse records were re-mapped to CA in `dim_warehouse` to create a non-TN state, enabling before/after RLS verification (admin sees CA + TN; non-admin sees only TN) |

---

## Appendix A: Repository File Inventory

| File | Purpose |
| --- | --- |
| `01_landing_simulator.ipynb` | Landing zone setup, catalog/schema/volume creation, batch file generation with injected bad data |
| `02_bronze_pipeline.sql` | Bronze streaming tables for `inventory` and `item` via `read_files` |
| `03_silver_pipeline.sql` | Silver streaming table with DLT expectations, quarantine table, SCD Type 2 `APPLY CHANGES` |
| `04_gold_pipeline.sql` | Gold star schema: dim_warehouse, dim_date, fct_inventory, fct_weekly_demand, v_inventory_weeks_of_cover |
| `05_governance.sql` | RLS function, column mask functions, entitlements table, tag application |
| `06_quality_gate.sql` | Post-ingestion DQ gate assertions and `dq_gate_results` logging |
| `07_benchmark_query.sql` | Baseline (pre-optimization) benchmark query |
| `optimization.sql` | ZORDER + ANALYZE TABLE + post-optimization benchmark query |
| `checkingScd.sql` | SCD Type 2 verification (price increase batches, `__start_at`/`__end_at` inspection) |
| `testQueries.sql` | Governance verification: RLS, CLS, entitlements, masking behavior |
| `q1_inventory_by_state.sql` | Parameterized query: inventory by state (`:param_state`) |
| `q2_critical_weeks_of_cover.sql` | Parameterized query: items below weeks-of-cover threshold (`:param_max_cover`) |
| `q3_inventory_snapshot_by_date.sql` | Parameterized query: inventory snapshot by date (`:param_start_date`) |
| `Operations Dashboard on System Tables.sql` | Dashboard source queries: DQ summary, ingestion health, query history |
| `validating_gold_tables.sql` | Row count validation across all gold tables |
| `validating_metric_views.sql` | Metric view output validation |
| `databricks.yml` | Declarative Automation Bundle configuration (job, schedule, tasks, variables) |

---

## Appendix B: Unity Catalog Object Map

```
meridian_t07 (CATALOG)
+-- landing (SCHEMA)
|   +-- raw_files (VOLUME) -> /Volumes/meridian_t07/landing/raw_files/
+-- bronze (SCHEMA) [layer=bronze, confidentiality=internal]
|   +-- inventory (STREAMING TABLE)
|   +-- item (STREAMING TABLE)
+-- silver (SCHEMA) [layer=silver, confidentiality=internal]
|   +-- inventory (STREAMING TABLE, DLT constraints)
|   +-- dim_item (STREAMING TABLE, SCD Type 2, column-masked)
+-- gold (SCHEMA) [layer=gold, confidentiality=general]
|   +-- dim_warehouse (TABLE, RLS)
|   +-- dim_date (TABLE)
|   +-- fct_inventory (TABLE, RLS)
|   +-- fct_weekly_demand (TABLE)
|   +-- v_inventory_weeks_of_cover (VIEW)
+-- gov (SCHEMA) [layer=governance, confidentiality=restricted]
|   +-- user_state_entitlements (TABLE)
|   +-- fn_warehouse_rls (FUNCTION)
|   +-- fn_mask_financial (FUNCTION)
|   +-- fn_mask_product_name (FUNCTION)
+-- ops (SCHEMA)
    +-- dq_gate_results (TABLE)
    +-- inventory_quarantine (STREAMING TABLE)
```

---

*Project Meridian -- Meridian Warehousing Platform (Package 07)*  
*Deployed on Databricks with Unity Catalog on AWS* 
