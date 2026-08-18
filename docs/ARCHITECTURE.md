# Architecture

## Target Architecture

```text
                 Synthetic Financial Sources
                           |
                           v
                Azure Data Lake Storage Gen2
                           |
                           v
                    Azure Databricks
                           |
                    +------+------+
                    |             |
                 Bronze         Config
                    |             |
                    v             |
                 Silver <---------+
                    |
                    v
                  Gold
                    |
        +-----------+-----------+-----------+
        |           |           |           |
    Holdings   Transactions  Intraday    Monthly
    Product       Product     Product     Product
        |           |           |           |
        +-----------+-----------+-----------+
                    |
                    v
          Analytics / Reporting / AI
```

## Layer Responsibilities

### Source

Synthetic representations of operational financial data. The initial model is centered on `TRN_TRANSACTIONS`, `HOLDINGS`, and `SECURITY`, with supporting account, portfolio, client, and reference entities added when required.

### Bronze

Purpose: preserve source-level data with minimal transformation.

Characteristics:
- Raw structure retained where practical
- Ingestion metadata captured
- Source and load timestamps captured
- Incremental ingestion supported
- Delta Lake storage

### Silver

Purpose: create clean, standardized, enriched financial datasets.

Examples:
- Datatype standardization
- Null and duplicate handling
- Security enrichment
- Account/client/portfolio enrichment
- Referential-integrity checks
- Financial business rules

### Gold

Purpose: provide reusable business-ready data products.

Initial products:
- Client Holdings
- Client Transactions
- Intraday Positions
- Monthly Financial Activity
- Security Reference

## Cross-Cutting Components

### Data Quality

Quality checks will validate required keys, reference relationships, dates, numeric values, duplicates, and other domain-specific rules before data reaches trusted Gold products.

### Orchestration

Databricks Workflows will coordinate ingestion, transformation, quality checks, and Gold data-product processing.

### Governance

Unity Catalog will be used conceptually and, where the environment permits, practically for catalog/schema organization, permissions, metadata, and lineage.

### Consumption

Gold data products will be designed for multiple consumers. CSV/Excel reporting may be one downstream consumer, but it is not the primary purpose of the platform.

## Design Principle

The project is intentionally different from a legacy report-generation system. The primary output is a governed set of reusable financial data products; report files are optional downstream outputs.
