# Bronze Layer Ingestion Setup

## Purpose

This document records the initial Bronze-layer implementation after establishing Oracle-to-Azure Databricks connectivity. The Oracle source is accessed through the read-only foreign catalog and materialized into Unity Catalog-managed Delta tables for downstream Silver and Gold processing.

## 1. Source Layer

Oracle source objects are exposed through the Databricks foreign catalog:

```text
oracle_finance_source_catalog
└── finance_app
    ├── client
    ├── account
    ├── portfolio
    ├── security
    ├── holdings
    └── trn_transactions
```

The foreign catalog is the source-access layer; it is not the final storage layer for the Lakehouse.

## 2. Bronze Catalog Structure

The Bronze schema is created in the project Unity Catalog:

```sql
CREATE SCHEMA IF NOT EXISTS `databricks-cata`.bronze;
```

Target structure:

```text
databricks-cata
└── bronze
    ├── client
    ├── account
    ├── portfolio
    ├── security
    ├── holdings
    └── trn_transactions
```

## 3. Initial Bronze Pilot — CLIENT

The first Bronze table was implemented using `CLIENT` as the pilot source table.

The initial test copy using a simple `CREATE OR REPLACE TABLE ... AS SELECT *` was removed before the proper Bronze implementation was created.

The final pilot pattern uses a managed Delta table and retains source-level columns while adding ingestion metadata:

```sql
CREATE TABLE `databricks-cata`.bronze.client
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.CLIENT' AS source_table
FROM oracle_finance_source_catalog.finance_app.client;
```

## 4. Validation

Row counts were compared between the foreign Oracle source and the Bronze table.

Source:

```sql
SELECT COUNT(*)
FROM oracle_finance_source_catalog.finance_app.client;
```

Bronze:

```sql
SELECT COUNT(*)
FROM `databricks-cata`.bronze.client;
```

The Bronze data was verified successfully and the source rows were visible in the Delta table.

Ingestion metadata can be validated with:

```sql
SELECT
    source_system,
    source_table,
    ingestion_timestamp,
    COUNT(*) AS record_count
FROM `databricks-cata`.bronze.client
GROUP BY
    source_system,
    source_table,
    ingestion_timestamp;
```

## 5. Design Decision

The Bronze layer will preserve source-level data with minimal business transformation. Ingestion metadata will be added consistently across Bronze tables so that downstream processing can identify the source system, source object, and load event.

Business cleansing, enrichment, and conformance will be performed in Silver rather than Bronze.

## 6. Next Work

- [ ] Define reusable ingestion pattern for all six Oracle source tables
- [ ] Materialize `ACCOUNT` into Bronze
- [ ] Materialize `PORTFOLIO` into Bronze
- [ ] Materialize `SECURITY` into Bronze
- [ ] Materialize `HOLDINGS` into Bronze
- [ ] Materialize `TRN_TRANSACTIONS` into Bronze
- [ ] Decide full-load versus incremental strategy by table
- [ ] Add deterministic ingestion/load identifiers where required
- [ ] Add Bronze data-quality checks
- [ ] Build Databricks Workflow orchestration

## 7. Important Connectivity Note

The current Oracle connection uses a temporary Pinggy TCP tunnel for development. The tunnel is not a production architecture and must remain active while the Oracle foreign catalog is queried. A production implementation would use private connectivity such as VPN or ExpressRoute between the Oracle environment and Azure/Databricks.
