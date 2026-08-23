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

## 3. Why Bronze Uses Delta

Bronze is a persisted Lakehouse layer, not simply a temporary copy of Oracle data. The project therefore uses **Delta Lake** for Bronze tables.

The `USING DELTA` clause tells Databricks to create the table as a Delta table rather than as a generic Parquet or other file-format table:

```sql
CREATE TABLE `databricks-cata`.bronze.account
USING DELTA
AS
SELECT ...;
```

Delta is useful here because the downstream pipeline will need reliable table transactions, schema management, repeatable writes, and incremental processing. Delta also provides table history/time-travel capabilities and efficient support for operations such as `MERGE`, which will become relevant when the project moves from full loads to incremental ingestion for `HOLDINGS` and `TRN_TRANSACTIONS`.

This is an architectural choice rather than syntax used only because it is common in Databricks tutorials: the Bronze layer is intended to become a durable, replayable input to Silver, so a transactional table format is appropriate.

## 4. Initial Bronze Pilot — CLIENT

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

## 5. Account Bronze Ingestion

After validating the `CLIENT` pilot, the same basic pattern was applied to `ACCOUNT`:

```sql
CREATE TABLE `databricks-cata`.bronze.account
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.ACCOUNT' AS source_table
FROM oracle_finance_source_catalog.finance_app.ACCOUNT;
```

The `ACCOUNT` table is a small reference/master-style source table, so a full initial materialization is appropriate. Later orchestration can replace or extend this with a repeatable load strategy if the source requirements change.

### Why add ingestion metadata?

The metadata columns are intentionally not business attributes from Oracle. They allow the Lakehouse to answer operational questions such as:

- Which source system produced this row?
- Which source object was ingested?
- When was this Bronze load performed?

This separation keeps source data recognizable while giving downstream engineering and troubleshooting logic a basic audit trail.

## 6. Validation

Row counts were compared between the foreign Oracle source and the Bronze tables.

For `CLIENT`:

```sql
SELECT COUNT(*)
FROM oracle_finance_source_catalog.finance_app.client;
```

```sql
SELECT COUNT(*)
FROM `databricks-cata`.bronze.client;
```

For `ACCOUNT`:

```sql
SELECT COUNT(*)
FROM oracle_finance_source_catalog.finance_app.account;
```

```sql
SELECT COUNT(*)
FROM `databricks-cata`.bronze.account;
```

The Bronze data was verified successfully and the source rows were visible in the Delta tables.

Ingestion metadata can be validated with:

```sql
SELECT
    source_system,
    source_table,
    ingestion_timestamp,
    COUNT(*) AS record_count
FROM `databricks-cata`.bronze.account
GROUP BY
    source_system,
    source_table,
    ingestion_timestamp;
```

## 7. Design Decisions

### Preserve source-level data in Bronze

Bronze should apply minimal business transformation. The goal is to preserve a trustworthy source representation that can be reprocessed downstream.

### Use Delta for persisted Bronze tables

Delta gives the Bronze layer a durable table abstraction suitable for repeatable ingestion, table history, transactional writes, and future incremental operations.

### Add technical metadata, not business logic

`ingestion_timestamp`, `source_system`, and `source_table` are technical/audit metadata. Business cleansing, enrichment, and conformance remain Silver responsibilities.

## 8. Next Work

- [x] Materialize `CLIENT` into Bronze
- [x] Materialize `ACCOUNT` into Bronze
- [ ] Materialize `PORTFOLIO` into Bronze
- [ ] Materialize `SECURITY` into Bronze
- [ ] Materialize `HOLDINGS` into Bronze
- [ ] Materialize `TRN_TRANSACTIONS` into Bronze
- [ ] Decide full-load versus incremental strategy by table
- [ ] Add deterministic ingestion/load identifiers where required
- [ ] Add Bronze data-quality checks
- [ ] Build Databricks Workflow orchestration

The expected strategy is to treat small/reference-oriented tables such as `CLIENT`, `ACCOUNT`, `PORTFOLIO`, and `SECURITY` differently from larger changing datasets such as `HOLDINGS` and `TRN_TRANSACTIONS`, where incremental extraction should be demonstrated.

## 9. Important Connectivity Note

The current Oracle connection uses a temporary Pinggy TCP tunnel for development. The tunnel is not a production architecture and must remain active while the Oracle foreign catalog is queried. A production implementation would use private connectivity such as VPN or ExpressRoute between the Oracle environment and Azure/Databricks.
