# Silver Layer Setup

## Purpose

This document records the initial Silver-layer implementation after the Oracle source was successfully ingested into Delta Bronze tables.

Silver is the layer where source data is cleaned, standardized, validated, and conformed for reliable downstream use. Unlike Bronze, Silver is allowed to apply controlled transformations because its purpose is to produce a consistent analytical representation of operational data.

## 1. Layer Architecture

```text
Oracle
  ↓
Foreign Catalog
  ↓
Bronze — source-preserving + technical metadata
  ↓
Silver — cleaned + standardized + validated
  ↓
Gold — business-ready data products
```

The project uses the following Unity Catalog structure:

```text
databricks-cata
├── bronze
│   ├── client
│   ├── account
│   ├── portfolio
│   ├── security
│   ├── holdings
│   └── trn_transactions
│
└── silver
    └── client
```

## 2. Why Silver Is Separate From Bronze

Bronze is intentionally close to the source system so that the raw operational representation remains available for replay and troubleshooting.

Silver applies controlled transformations such as:

- standardizing data types;
- trimming string values;
- normalizing categorical fields;
- validating keys and required fields;
- enforcing relationships between related entities;
- preparing data for downstream business logic.

This separation prevents business logic from being mixed into the ingestion layer and gives the pipeline a clean progression from source preservation to conformed data.

## 3. Create the Silver Schema

The project Silver schema was created with Spark SQL:

```sql
%sql

CREATE SCHEMA IF NOT EXISTS `databricks-cata`.silver;
```

## 4. Silver Pilot — CLIENT

`CLIENT` was selected as the first Silver pilot because it is a relatively simple master/reference entity and allows the transformation pattern to be validated before applying more complex relationship logic to accounts, holdings, and transactions.

The Bronze schema was inspected before applying transformations:

```text
CLIENT_ID
CLIENT_CODE
CLIENT_NAME
CLIENT_TYPE
BASE_CURRENCY
COUNTRY_CODE
STATUS
CREATED_AT
UPDATED_AT
ingestion_timestamp
source_system
source_table
```

The source-quality checks were also performed before transformation.

### Data-quality baseline

The following checks were performed against Bronze `CLIENT`:

- `CLIENT_ID` uniqueness;
- NULL checks on key/required fields;
- distribution of `STATUS` values;
- distribution of `CLIENT_TYPE` values;
- whitespace checks on string attributes.

The current synthetic source passed these checks cleanly. No artificial data-quality issue was introduced simply to justify a transformation.

## 5. Silver Transformation

The Silver table was created with Spark SQL and Delta:

```sql
%sql

CREATE TABLE `databricks-cata`.silver.client
USING DELTA
AS
SELECT
    CAST(CLIENT_ID AS BIGINT) AS CLIENT_ID,
    TRIM(CLIENT_CODE) AS CLIENT_CODE,
    TRIM(CLIENT_NAME) AS CLIENT_NAME,
    UPPER(TRIM(CLIENT_TYPE)) AS CLIENT_TYPE,
    UPPER(TRIM(BASE_CURRENCY)) AS BASE_CURRENCY,
    UPPER(TRIM(COUNTRY_CODE)) AS COUNTRY_CODE,
    UPPER(TRIM(STATUS)) AS STATUS,
    CREATED_AT,
    UPDATED_AT,
    ingestion_timestamp,
    source_system,
    source_table
FROM `databricks-cata`.bronze.client;
```

### Transformation rationale

The transformations are intentionally conservative:

- `CLIENT_ID` is standardized from the source decimal representation to `BIGINT`, since it functions as an integer identifier.
- String attributes are trimmed to remove accidental leading/trailing whitespace.
- Categorical attributes such as `CLIENT_TYPE`, `BASE_CURRENCY`, `COUNTRY_CODE`, and `STATUS` are normalized to uppercase so downstream filters and joins do not depend on inconsistent casing.
- Source timestamps are preserved rather than regenerated.
- Bronze technical metadata is retained to maintain lineage and operational traceability.

No business meaning is changed and no source records are deliberately filtered out in this first Silver implementation.

## 6. Silver Validation

The Bronze and Silver row counts were compared to ensure that the transformation did not unintentionally remove source records:

```sql
%sql

SELECT COUNT(*) AS bronze_count
FROM `databricks-cata`.bronze.client;
```

```sql
%sql

SELECT COUNT(*) AS silver_count
FROM `databricks-cata`.silver.client;
```

The counts matched.

The Silver key was also validated:

```sql
%sql

SELECT
    CLIENT_ID,
    COUNT(*) AS record_count
FROM `databricks-cata`.silver.client
GROUP BY CLIENT_ID
HAVING COUNT(*) > 1;
```

The validation returned no duplicate `CLIENT_ID` values.

## 7. Spark SQL as the Databricks Transformation Layer

The project will use Spark SQL explicitly in Databricks notebook cells, using the `%sql` cell directive.

Example:

```sql
%sql
SELECT *
FROM `databricks-cata`.silver.client;
```

For reusable programmatic pipelines, the same Spark SQL can also be executed through PySpark with `spark.sql(...)`.

The objective is to keep transformations executable by the Spark engine and compatible with Delta-backed Silver and Gold tables.

## 8. Design Decisions

### Do not manufacture data-quality problems

The synthetic Oracle source is intentionally clean in the current `CLIENT` dataset. Silver transformations are therefore based on real observations from the source schema rather than invented defects.

### Standardize before enriching

The first Silver step focuses on consistent representations and validation. Business enrichment and cross-entity logic will be introduced after the basic Silver contracts are established.

### Preserve lineage metadata

Technical metadata from Bronze is retained so downstream users can trace the data back to its source and ingestion event.

### Use Delta for Silver

Silver tables are persisted as Delta because they need reliable writes, table history, and support for downstream incremental processing and transformations.

## 9. Current Progress

```text
Silver schema             ✓
CLIENT inspection         ✓
CLIENT quality baseline   ✓
CLIENT Silver table       ✓
CLIENT validation         ✓
ACCOUNT Silver            pending
PORTFOLIO Silver          pending
SECURITY Silver           pending
HOLDINGS Silver           pending
TRN_TRANSACTIONS Silver   pending
```

## 10. Next Work

The next Silver implementation will be `ACCOUNT`, where referential-integrity checks can be introduced between:

```text
ACCOUNT.CLIENT_ID
        ↓
CLIENT.CLIENT_ID
```

Later Silver work will address:

- portfolio-to-account conformance;
- holdings-to-portfolio/security validation;
- transaction-to-portfolio/security validation;
- transaction financial consistency checks;
- deduplication and business-key enforcement where appropriate;
- common dimensions/reference data needed by Gold data products.
