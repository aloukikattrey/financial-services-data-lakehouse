# Financial Services Data Lakehouse

A personal Databricks data engineering project for building a realistic financial-services data platform around transaction, holdings, security, and related reference data.

## Project Goal

Build a governed Lakehouse that ingests financial source data, creates reusable Bronze/Silver/Gold datasets, applies data-quality rules, and exposes financial data products for analytics, reporting, and downstream applications.

This project is independent of any employer system. All data will be synthetic and will not contain proprietary client information.

## Core Business Domain

The initial source model is based on:

- `TRN_TRANSACTIONS` — transaction-level financial activity
- `HOLDINGS` — position/holding snapshots
- `SECURITY` — security/reference information
- Additional account, portfolio, client, and reference entities will be added as the data model is refined

## High-Level Architecture

```text
Synthetic Financial Source Data
            |
            v
Azure Data Lake Storage Gen2
            |
            v
     Azure Databricks
            |
        +---+---+
        | Bronze |
        +---+---+
            |
        +---+---+
        | Silver |
        +---+---+
            |
        +---+---+
        |  Gold  |
        +---+---+
            |
   +--------+---------+---------+
   |        |         |         |
Holdings Transactions Intraday Monthly
Data Product Data Product Data Product Data Product
```

## Planned Technology

- Azure Databricks
- Azure Data Lake Storage Gen2
- PySpark
- SQL
- Delta Lake
- Unity Catalog
- Databricks Workflows
- Auto Loader / incremental ingestion
- Data quality and validation

## Planned Data Products

- Client Holdings
- Client Transactions
- Intraday Positions
- Monthly Financial Activity
- Security Reference

Excel/CSV files may be downstream consumers of these curated data products, but file generation is not the primary product of the platform.

## Development Principles

1. Use a realistic financial-services domain instead of a generic tutorial dataset.
2. Separate raw, curated, and business-ready datasets.
3. Build reusable data products rather than one-off report jobs.
4. Add technologies only when they serve a real architectural purpose.
5. Use synthetic data only; never commit employer/client data.
6. Document architecture, assumptions, decisions, and progress as the project evolves.

## Current Status

**Phase 0 — Project initialization**

- [x] Repository created
- [x] Project scope defined
- [x] Core business problem defined
- [x] Initial architecture defined
- [x] Core entities identified
- [ ] Finalize source data model
- [ ] Create synthetic datasets

See [`docs/PROGRESS.md`](docs/PROGRESS.md) for the detailed project tracker.
