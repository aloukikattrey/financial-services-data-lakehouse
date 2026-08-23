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

## 6. Portfolio Bronze Ingestion

The `PORTFOLIO` source table was materialized into a managed Delta Bronze table using the same source-preserving pattern:

```sql
CREATE TABLE `databricks-cata`.bronze.portfolio
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.PORTFOLIO' AS source_table
FROM oracle_finance_source_catalog.finance_app.portfolio;
```

`PORTFOLIO` is treated as a relatively small reference/master-style dataset for the initial implementation, so a full snapshot load is appropriate.

## 7. Security Bronze Ingestion

The `SECURITY` source table was also materialized into a managed Delta Bronze table:

```sql
CREATE TABLE `databricks-cata`.bronze.security
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.SECURITY' AS source_table
FROM oracle_finance_source_catalog.finance_app.security;
```

The security master is similarly treated as a reference/master-style source for the first version of the pipeline. A later version can introduce a source-specific refresh strategy if security attributes become slowly changing.

## 8. Why the Initial Reference Tables Use Full Loads

`CLIENT`, `ACCOUNT`, `PORTFOLIO`, and `SECURITY` are relatively small compared with the transactional tables in this project. For the first Bronze implementation, full materialization keeps the ingestion logic simple while establishing the source-to-Delta pattern.

This does **not** mean every production financial pipeline should use full loads. The project intentionally reserves incremental processing for the larger, more frequently changing datasets, especially `HOLDINGS` and `TRN_TRANSACTIONS`.

The distinction demonstrates an important data-engineering principle: ingestion strategy should be chosen based on source-table characteristics, change volume, business requirements, and recovery/reprocessing needs rather than applying one pattern universally.

## 9. Ingestion Metadata and Operational Context

The Bronze tables retain source-level columns and add technical metadata:

- `ingestion_timestamp` — when the row was materialized into Bronze.
- `source_system` — identifies the originating platform (`ORACLE`).
- `source_table` — identifies the originating Oracle object.

These fields are intentionally technical rather than business attributes. They create a basic audit trail and make troubleshooting easier without moving business transformations into Bronze.

## 10. Validation

Row counts were compared between the foreign Oracle source and each Bronze table.

Example source validation:

```sql
SELECT COUNT(*)
FROM oracle_finance_source_catalog.finance_app.account;
```

Example Bronze validation:

```sql
SELECT COUNT(*)
FROM `databricks-cata`.bronze.account;
```

The same comparison was performed for `CLIENT`, `PORTFOLIO`, and `SECURITY`, and the Bronze data was verified successfully.

Ingestion metadata can be validated with:

```sql
SELECT
    source_system,
    source_table,
    ingestion_timestamp,
    COUNT(*) AS record_count
FROM `databricks-cata`.bronze.security
GROUP BY
    source_system,
    source_table,
    ingestion_timestamp;
```

## 11. Design Decisions

### Preserve source-level data in Bronze

Bronze should apply minimal business transformation. The goal is to preserve a trustworthy source representation that can be reprocessed downstream.

### Use Delta for persisted Bronze tables

Delta gives the Bronze layer a durable table abstraction suitable for repeatable ingestion, table history, transactional writes, and future incremental operations.

### Add technical metadata, not business logic

`ingestion_timestamp`, `source_system`, and `source_table` are technical/audit metadata. Business cleansing, enrichment, and conformance remain Silver responsibilities.

### Different tables can use different ingestion strategies

Small reference-oriented tables can start with full snapshots, while larger operational datasets can use incremental extraction. The project will demonstrate both patterns rather than forcing every source table into the same ingestion design.

## 12. Next Work

- [x] Materialize `CLIENT` into Bronze
- [x] Materialize `ACCOUNT` into Bronze
- [x] Materialize `PORTFOLIO` into Bronze
- [x] Materialize `SECURITY` into Bronze
- [ ] Materialize `HOLDINGS` into Bronze
- [ ] Materialize `TRN_TRANSACTIONS` into Bronze
- [ ] Decide full-load versus incremental strategy by table
- [ ] Add deterministic ingestion/load identifiers where required
- [ ] Add Bronze data-quality checks
- [ ] Build Databricks Workflow orchestration

The expected strategy is to treat `HOLDINGS` and `TRN_TRANSACTIONS` differently from the smaller reference-oriented datasets. Before creating those Bronze tables, the project will inspect their date/change columns and design the incremental extraction pattern deliberately.

## 13. Important Connectivity Note

The current Oracle connection uses a temporary Pinggy TCP tunnel for development. The tunnel is not a production architecture and must remain active while the Oracle foreign catalog is queried. A production implementation would use private connectivity such as VPN or ExpressRoute between the Oracle environment and Azure/Databricks.
