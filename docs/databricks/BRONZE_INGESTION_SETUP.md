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

## 12. Transaction Bronze: Source Redesign for Change-Aware Ingestion

The original synthetic `TRN_TRANSACTIONS` table did not contain `CREATED_AT` or `UPDATED_AT`. That would have forced the project to rely on `TRANSACTION_DATE`, which can represent business event time but is not a reliable indicator that a source record was newly inserted or changed.

Because this project is intended to model a realistic operational source, the transaction table was deliberately recreated with explicit technical timestamps:

```text
TRANSACTION_ID
PORTFOLIO_ID
SECURITY_ID
TRANSACTION_DATE
SETTLEMENT_DATE
TRANSACTION_TYPE
BUY_SELL_FLAG
QUANTITY
PRICE
GROSS_AMOUNT
FEES
NET_AMOUNT
CURRENCY_CODE
TRADE_STATUS
SOURCE_REFERENCE
CREATED_AT
UPDATED_AT
```

The table was regenerated with 5,000 synthetic transactions. `TRANSACTION_ID` is unique and serves as the merge/business key for the Bronze Delta table.

### Why this redesign matters

`TRANSACTION_DATE` and `UPDATED_AT` represent different concepts:

```text
TRANSACTION_DATE
→ when the financial event occurred

UPDATED_AT
→ when the operational source record was last changed
```

Using `TRANSACTION_DATE` as a change watermark could miss late-arriving records or corrections to older transactions. `UPDATED_AT` is therefore a better technical watermark for a mutable transaction source, provided the source system maintains it reliably.

This is intentionally different from `HOLDINGS`, where the source is modeled as immutable periodic snapshots and `AS_OF_DATE` is the appropriate extraction boundary.

## 13. Initial Transaction Bronze Load

The recreated `TRN_TRANSACTIONS` source was exposed through the foreign catalog and validated from Databricks:

- 5,000 source rows
- `TRANSACTION_ID` values 1 through 5,000
- `TRANSACTION_ID` unique
- `CREATED_AT` and `UPDATED_AT` available for change detection

The initial Bronze materialization was:

```sql
CREATE TABLE `databricks-cata`.bronze.trn_transactions
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.TRN_TRANSACTIONS' AS source_table
FROM oracle_finance_source_catalog.finance_app.TRN_TRANSACTIONS;
```

The resulting Bronze row count was 5,000.

The initial incremental watermark was established using:

```sql
SELECT
    MAX(UPDATED_AT) AS last_loaded_updated_at
FROM `databricks-cata`.bronze.trn_transactions;
```

The captured baseline watermark for the test was:

```text
2026-08-17 11:00:00 UTC
```

## 14. Transaction Incremental Load Test

The source was then changed in two ways to test both sides of the incremental pattern:

1. A new transaction (`TRANSACTION_ID = 5001`) was inserted.
2. An existing transaction (`TRANSACTION_ID = 100`) was updated and its `UPDATED_AT` timestamp advanced.

Both records had `UPDATED_AT` later than the Bronze watermark.

The extraction query was implemented using Spark SQL:

```sql
%sql

SELECT
    TRANSACTION_ID,
    TRADE_STATUS,
    UPDATED_AT
FROM oracle_finance_source_catalog.finance_app.TRN_TRANSACTIONS
WHERE UPDATED_AT >
      (
          SELECT MAX(UPDATED_AT)
          FROM `databricks-cata`.bronze.trn_transactions
      )
ORDER BY TRANSACTION_ID;
```

The changed set contained the expected existing and new transactions.

### Delta MERGE design

The changed source rows were merged into Bronze using `TRANSACTION_ID` as the match key:

```sql
%sql

MERGE INTO `databricks-cata`.bronze.trn_transactions AS target
USING (
    SELECT
        *,
        current_timestamp() AS ingestion_timestamp,
        'ORACLE' AS source_system,
        'FINANCE_APP.TRN_TRANSACTIONS' AS source_table
    FROM oracle_finance_source_catalog.finance_app.TRN_TRANSACTIONS
    WHERE UPDATED_AT >
          (
              SELECT MAX(UPDATED_AT)
              FROM `databricks-cata`.bronze.trn_transactions
          )
) AS source
ON target.TRANSACTION_ID = source.TRANSACTION_ID

WHEN MATCHED THEN UPDATE SET
    target.PORTFOLIO_ID = source.PORTFOLIO_ID,
    target.SECURITY_ID = source.SECURITY_ID,
    target.TRANSACTION_DATE = source.TRANSACTION_DATE,
    target.SETTLEMENT_DATE = source.SETTLEMENT_DATE,
    target.TRANSACTION_TYPE = source.TRANSACTION_TYPE,
    target.BUY_SELL_FLAG = source.BUY_SELL_FLAG,
    target.QUANTITY = source.QUANTITY,
    target.PRICE = source.PRICE,
    target.GROSS_AMOUNT = source.GROSS_AMOUNT,
    target.FEES = source.FEES,
    target.NET_AMOUNT = source.NET_AMOUNT,
    target.CURRENCY_CODE = source.CURRENCY_CODE,
    target.TRADE_STATUS = source.TRADE_STATUS,
    target.SOURCE_REFERENCE = source.SOURCE_REFERENCE,
    target.CREATED_AT = source.CREATED_AT,
    target.UPDATED_AT = source.UPDATED_AT,
    target.ingestion_timestamp = source.ingestion_timestamp

WHEN NOT MATCHED THEN INSERT (
    TRANSACTION_ID,
    PORTFOLIO_ID,
    SECURITY_ID,
    TRANSACTION_DATE,
    SETTLEMENT_DATE,
    TRANSACTION_TYPE,
    BUY_SELL_FLAG,
    QUANTITY,
    PRICE,
    GROSS_AMOUNT,
    FEES,
    NET_AMOUNT,
    CURRENCY_CODE,
    TRADE_STATUS,
    SOURCE_REFERENCE,
    CREATED_AT,
    UPDATED_AT,
    ingestion_timestamp,
    source_system,
    source_table
)
VALUES (
    source.TRANSACTION_ID,
    source.PORTFOLIO_ID,
    source.SECURITY_ID,
    source.TRANSACTION_DATE,
    source.SETTLEMENT_DATE,
    source.TRANSACTION_TYPE,
    source.BUY_SELL_FLAG,
    source.QUANTITY,
    source.PRICE,
    source.GROSS_AMOUNT,
    source.FEES,
    source.NET_AMOUNT,
    source.CURRENCY_CODE,
    source.TRADE_STATUS,
    source.SOURCE_REFERENCE,
    source.CREATED_AT,
    source.UPDATED_AT,
    source.ingestion_timestamp,
    source.source_system,
    source.source_table
);
```

### Result

The merge produced the intended behavior:

```text
TRANSACTION_ID 100
→ existing Bronze row updated

TRANSACTION_ID 5001
→ new Bronze row inserted
```

The Bronze table moved from 5,000 to 5,001 rows, confirming that the updated existing transaction was not duplicated while the new transaction was added.

A follow-up watermark check returned no records newer than the new Bronze watermark, demonstrating that a subsequent incremental extraction has no additional rows to process.

### Why `MERGE` is appropriate here

This is a mutable event-source pattern:

```text
Oracle
   │
   │ UPDATED_AT > watermark
   ▼
New / changed transactions
   │
   ▼
Delta MERGE
   ├── matching TRANSACTION_ID → UPDATE
   └── new TRANSACTION_ID      → INSERT
```

The project therefore demonstrates two different incremental ingestion patterns:

```text
HOLDINGS
→ immutable snapshots
→ AS_OF_DATE watermark
→ append

TRN_TRANSACTIONS
→ mutable event records
→ UPDATED_AT watermark
→ MERGE by TRANSACTION_ID
```

This distinction is intentional. It reflects a core data-engineering principle: ingestion logic should follow the source's change semantics rather than forcing every table into one template.

## 15. Why `UPDATED_AT` Is a Better Transaction Watermark

`TRANSACTION_DATE` represents event/business time. A transaction can occur on one date and be corrected later. A reliable `UPDATED_AT` provides technical change-detection time and therefore makes it possible to capture:

- newly inserted transactions
- modified statuses
- corrected financial attributes
- other source-record changes

In production, the reliability of this column should be validated against the source system's update semantics. If late-arriving records, clock skew, timestamp precision, or unreliable source timestamps exist, a lookback window, source CDC, or another reconciliation mechanism may be required.

## 16. Ingestion Metadata and Operational Context

The Bronze tables retain source-level columns and add technical metadata:

- `ingestion_timestamp` — when the row was materialized into Bronze.
- `source_system` — identifies the originating platform (`ORACLE`).
- `source_table` — identifies the originating Oracle object.

These fields are intentionally technical rather than business attributes. They create a basic audit trail and make troubleshooting easier without moving business transformations into Bronze.

## 17. Validation

Row counts were compared between the foreign Oracle source and Bronze tables.

For reference tables, source and Bronze counts were compared after initial full loads.

For `HOLDINGS`, snapshot counts, the watermark, the new snapshot, and second-run no-new-data behavior were validated.

For `TRN_TRANSACTIONS`, the unique transaction ID range, the initial watermark, the new transaction, the updated transaction, the Delta `MERGE`, and the post-merge row count were validated.

## 18. Design Decisions

### Preserve source-level data in Bronze

Bronze should apply minimal business transformation. The goal is to preserve a trustworthy source representation that can be reprocessed downstream.

### Use Delta for persisted Bronze tables

Delta gives the Bronze layer a durable table abstraction suitable for repeatable ingestion, table history, transactional writes, and future incremental operations.

### Add technical metadata, not business logic

`ingestion_timestamp`, `source_system`, and `source_table` are technical/audit metadata. Business cleansing, enrichment, and conformance remain Silver responsibilities.

### Different tables can use different ingestion strategies

Small reference-oriented tables can start with full snapshots, `HOLDINGS` uses immutable snapshot ingestion, and `TRN_TRANSACTIONS` uses change-aware ingestion with `UPDATED_AT` and Delta `MERGE`.

### Document source assumptions explicitly

For `HOLDINGS`, `AS_OF_DATE` watermarking is valid only under the assumption that completed snapshots are immutable. For `TRN_TRANSACTIONS`, `UPDATED_AT` is treated as reliable source change-detection time for this project; a production implementation should verify the source's timestamp semantics and consider CDC/reconciliation where needed.

## 19. Current Progress

```text
CLIENT               ✓ Bronze full load
ACCOUNT              ✓ Bronze full load
PORTFOLIO            ✓ Bronze full load
SECURITY             ✓ Bronze full load
HOLDINGS             ✓ Bronze initial load
HOLDINGS incremental ✓ New snapshot detected and appended
                       ✓ Second-run no-new-data test passed
TRN_TRANSACTIONS     ✓ Source redesigned with CREATED_AT / UPDATED_AT
TRN_TRANSACTIONS     ✓ Bronze initial load
TRN_TRANSACTIONS     ✓ New transaction + update test
TRN_TRANSACTIONS     ✓ Delta MERGE validated
```

## 20. Next Work

- [x] Materialize `CLIENT` into Bronze
- [x] Materialize `ACCOUNT` into Bronze
- [x] Materialize `PORTFOLIO` into Bronze
- [x] Materialize `SECURITY` into Bronze
- [x] Materialize `HOLDINGS` into Bronze
- [x] Validate `HOLDINGS` incremental watermark
- [x] Test new-snapshot ingestion
- [x] Test second-run/no-duplicate behavior
- [x] Inspect `TRN_TRANSACTIONS` source change columns
- [x] Design transaction-specific incremental ingestion
- [x] Materialize `TRN_TRANSACTIONS` into Bronze
- [x] Add and validate `UPDATED_AT` watermarking
- [x] Validate Delta `MERGE` for updates and inserts
- [ ] Add deterministic ingestion/load identifiers where required
- [ ] Add Bronze data-quality checks
- [ ] Build Databricks Workflow orchestration
- [ ] Design Silver layer transformations

## 21. Important Connectivity Note

The current Oracle connection uses a temporary Pinggy TCP tunnel for development. The tunnel is not a production architecture and must remain active while the Oracle foreign catalog is queried. A production implementation would use private connectivity such as VPN or ExpressRoute between the Oracle environment and Azure/Databricks.
