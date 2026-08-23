# Bronze Layer Ingestion Setup

## Purpose

This document records the Bronze-layer implementation after establishing Oracle-to-Azure Databricks connectivity. The Oracle source is accessed through the read-only foreign catalog and materialized into Unity Catalog-managed Delta tables for downstream Silver and Gold processing.

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

Delta is useful here because the downstream pipeline will need reliable table transactions, schema management, repeatable writes, and incremental processing. Delta also provides table history/time-travel capabilities and efficient support for operations such as `MERGE`, which becomes relevant when the project moves from full loads to incremental ingestion for changing datasets.

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

## 9. Holdings: Source Analysis Before Incremental Ingestion

Before creating the Bronze `HOLDINGS` table, the source schema was inspected rather than assuming that a generic timestamp watermark would work.

The relevant Oracle columns are:

```text
HOLDING_ID
PORTFOLIO_ID
SECURITY_ID
AS_OF_DATE
QUANTITY
UNIT_PRICE
MARKET_VALUE
COST_BASIS
CURRENCY_CODE
CREATED_AT
UPDATED_AT
```

Two source characteristics were then validated:

1. `HOLDING_ID` is globally unique in the current source dataset.
2. `UPDATED_AT` is not a useful incremental watermark in the synthetic source because all currently generated rows share the same timestamp.

The source is instead structured as periodic holdings snapshots. The observed snapshot dates were:

```text
2026-06-30    480 rows
2026-07-31    480 rows
2026-08-21    480 rows
```

### Incremental design decision

For this project, `AS_OF_DATE` is used as the incremental boundary for `HOLDINGS`, under the documented business assumption that **completed holdings snapshots are immutable and new snapshots are added with newer `AS_OF_DATE` values**.

This is a snapshot-ingestion pattern, not a generic CDC pattern.

Conceptually:

```text
Bronze MAX(AS_OF_DATE)
        ↓
Oracle WHERE AS_OF_DATE > watermark
        ↓
New snapshot rows
        ↓
Append to Bronze Delta
```

The design is logical for an immutable periodic snapshot source. It would **not** be sufficient if historical snapshots could be corrected after loading; such a production source would require a reliable change timestamp, reconciliation process, or CDC mechanism.

## 10. Initial Holdings Bronze Load

The initial Bronze snapshot was materialized with the same source-preserving pattern:

```sql
CREATE TABLE `databricks-cata`.bronze.holdings
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.HOLDINGS' AS source_table
FROM oracle_finance_source_catalog.finance_app.HOLDINGS;
```

The initial Bronze table contained:

```text
2026-06-30    480
2026-07-31    480
2026-08-21    480
-------------------
             1440 rows
```

## 11. Holdings Incremental Load Test

To prove the incremental pattern, a controlled synthetic source snapshot for `2026-08-22` was added to Oracle. The new snapshot contained 480 rows.

The Bronze watermark before the test was:

```text
MAX(AS_OF_DATE) = 2026-08-21
```

The incremental query was:

```sql
SELECT
    AS_OF_DATE,
    COUNT(*) AS record_count
FROM oracle_finance_source_catalog.finance_app.HOLDINGS
WHERE AS_OF_DATE >
      (
          SELECT MAX(AS_OF_DATE)
          FROM `databricks-cata`.bronze.holdings
      )
GROUP BY AS_OF_DATE
ORDER BY AS_OF_DATE;
```

The query returned exactly:

```text
2026-08-22    480
```

The new snapshot was then appended to Bronze:

```sql
INSERT INTO `databricks-cata`.bronze.holdings
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.HOLDINGS' AS source_table
FROM oracle_finance_source_catalog.finance_app.HOLDINGS
WHERE AS_OF_DATE >
      (
          SELECT MAX(AS_OF_DATE)
          FROM `databricks-cata`.bronze.holdings
      );
```

After ingestion, Bronze contained:

```text
2026-06-30    480
2026-07-31    480
2026-08-21    480
2026-08-22    480
-------------------
             1920 rows
```

A second execution of the same incremental extraction found no new records because the Bronze watermark had advanced to `2026-08-22`. This demonstrated snapshot-level idempotent behavior: rerunning the load did not duplicate the already-ingested snapshot.

### Why this matters

This test demonstrates several core data-engineering concepts rather than just copying a table:

- **Watermarking** — the latest loaded `AS_OF_DATE` controls the next extraction boundary.
- **Incremental ingestion** — only new snapshots are read after the initial load.
- **Append semantics** — immutable snapshots can be appended instead of rewritten.
- **Idempotent reruns at the snapshot level** — a second run with no newer snapshot adds no rows.
- **Source-specific design** — the ingestion strategy is based on how the source data behaves rather than applying one generic pattern to every table.

## 12. Why `UPDATED_AT` Was Not Used

In a production system, `UPDATED_AT` would often be a strong candidate for incremental extraction. In the current synthetic source, however, `UPDATED_AT` is identical across all generated holdings records, so it cannot reliably distinguish newly changed records.

Rather than manufacturing a false change-data-capture story, the project uses the source's actual snapshot behavior and explicitly documents the assumption behind `AS_OF_DATE` watermarking.

## 13. Ingestion Metadata and Operational Context

The Bronze tables retain source-level columns and add technical metadata:

- `ingestion_timestamp` — when the row was materialized into Bronze.
- `source_system` — identifies the originating platform (`ORACLE`).
- `source_table` — identifies the originating Oracle object.

These fields are intentionally technical rather than business attributes. They create a basic audit trail and make troubleshooting easier without moving business transformations into Bronze.

## 14. Validation

Row counts were compared between the foreign Oracle source and the Bronze tables.

For example:

```sql
SELECT COUNT(*)
FROM oracle_finance_source_catalog.finance_app.account;
```

```sql
SELECT COUNT(*)
FROM `databricks-cata`.bronze.account;
```

For holdings, snapshot counts and the incremental watermark were validated before and after ingestion.

The Bronze data was verified successfully for the completed tables and the incremental `HOLDINGS` test.

## 15. Design Decisions

### Preserve source-level data in Bronze

Bronze should apply minimal business transformation. The goal is to preserve a trustworthy source representation that can be reprocessed downstream.

### Use Delta for persisted Bronze tables

Delta gives the Bronze layer a durable table abstraction suitable for repeatable ingestion, table history, transactional writes, and future incremental operations.

### Add technical metadata, not business logic

`ingestion_timestamp`, `source_system`, and `source_table` are technical/audit metadata. Business cleansing, enrichment, and conformance remain Silver responsibilities.

### Different tables can use different ingestion strategies

Small reference-oriented tables can start with full snapshots, while larger operational datasets can use incremental extraction. The project demonstrates both patterns rather than forcing every source table into the same ingestion design.

### Document source assumptions explicitly

For `HOLDINGS`, `AS_OF_DATE` watermarking is valid only under the assumption that completed snapshots are immutable. If the production source permits historical corrections, the ingestion design must change to detect and reconcile those changes.

## 16. Current Progress

```text
CLIENT              ✓ Bronze full load
ACCOUNT             ✓ Bronze full load
PORTFOLIO           ✓ Bronze full load
SECURITY            ✓ Bronze full load
HOLDINGS            ✓ Bronze initial load
HOLDINGS incremental ✓ New snapshot detected and appended
                      ✓ Second-run no-new-data test passed
TRN_TRANSACTIONS    pending
```

## 17. Next Work

- [x] Materialize `CLIENT` into Bronze
- [x] Materialize `ACCOUNT` into Bronze
- [x] Materialize `PORTFOLIO` into Bronze
- [x] Materialize `SECURITY` into Bronze
- [x] Materialize `HOLDINGS` into Bronze
- [x] Validate `HOLDINGS` incremental watermark
- [x] Test new-snapshot ingestion
- [x] Test second-run/no-duplicate behavior
- [ ] Inspect `TRN_TRANSACTIONS` source change columns
- [ ] Design transaction-specific incremental ingestion
- [ ] Materialize `TRN_TRANSACTIONS` into Bronze
- [ ] Add deterministic ingestion/load identifiers where required
- [ ] Add Bronze data-quality checks
- [ ] Build Databricks Workflow orchestration

## 18. Important Connectivity Note

The current Oracle connection uses a temporary Pinggy TCP tunnel for development. The tunnel is not a production architecture and must remain active while the Oracle foreign catalog is queried. A production implementation would use private connectivity such as VPN or ExpressRoute between the Oracle environment and Azure/Databricks.
