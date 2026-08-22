# Architecture

## Current Realized Architecture

```text
                    Local Oracle Database
                           |
                 Financial source tables
                           |
          +----------------+----------------+
          |                |                |
   TRN_TRANSACTIONS     HOLDINGS         SECURITY
          |                |                |
          +----------------+----------------+
                           |
                     JDBC ingestion
                           |
                           v
                 Azure Databricks
                           |
                     Unity Catalog
                           |
                   databricks-cata
                           |
              +------------+------------+
              |            |            |
           bronze       silver         gold
              |            |            |
              +------------+------------+
                           |
                 Financial Data Products
                           |
       +-------------------+--------------------+
       |                   |                    |
 Client Holdings   Client Transactions   Intraday / Monthly
                           |
                           v
              Analytics / Reporting / AI
```

## Infrastructure Foundation Completed

- Azure subscription is available under the user's Azure for Students subscription.
- Azure Databricks workspace created in Central India.
- Workspace was initially created as a Hybrid workspace because that was the available option for the selected Trial configuration.
- Unity Catalog was initially unavailable on the workspace and was subsequently enabled by creating/assigning a Unity Catalog metastore.
- A Databricks Access Connector with a system-assigned managed identity was created for Azure storage access.
- The Access Connector identity was granted `Storage Blob Data Contributor` access to the storage account used by the metastore managed-storage root.
- A Unity Catalog catalog named `databricks-cata` was created successfully.
- The workspace has access to a Serverless Starter Warehouse for SQL operations.
- Docker Desktop has been installed locally and is running for the local Oracle source-system setup.

## Source-System Strategy

The project intentionally models the client's current enterprise pattern rather than starting from uploaded CSV files. The source of truth for the project will be a locally hosted synthetic Oracle database that represents an operational financial system.

Initial logical source entities:

- `TRN_TRANSACTIONS`
- `HOLDINGS`
- `SECURITY`
- Supporting client, account, portfolio, and reference entities as required

No employer or client data, credentials, SQL, or proprietary schemas will be used.

## Ingestion Strategy

The first implementation will use Oracle-to-Databricks ingestion through JDBC. This provides a direct and explainable migration pattern:

```text
Oracle -> JDBC -> Databricks Bronze -> Silver -> Gold
```

Oracle CDC may be evaluated later as an advanced phase. It is not part of the initial implementation.

## Layer Responsibilities

### Source

Synthetic Oracle representations of operational financial data. The model is centered on `TRN_TRANSACTIONS`, `HOLDINGS`, and `SECURITY`, with supporting entities added when required.

### Bronze

Purpose: preserve source-level financial data with minimal transformation.

Characteristics:
- Source structure retained where practical
- Ingestion metadata captured
- Source/load timestamps captured
- Incremental ingestion supported where appropriate
- Delta Lake storage in Databricks

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

Purpose: provide reusable business-ready financial data products.

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

Unity Catalog is now part of the actual project environment. The `databricks-cata` catalog will contain project schemas for Bronze, Silver, Gold, and metadata once Phase 1 data modeling is finalized.

### Consumption

Gold data products will be designed for multiple consumers. CSV/Excel reporting may be one downstream consumer, but it is not the primary purpose of the platform.

## Design Principle

The project is intentionally different from a legacy report-generation system. The primary output is a governed set of reusable financial data products. The architecture uses an operational Oracle source and modern Databricks processing rather than simply recreating the existing Python/Perl report-generation flow.
