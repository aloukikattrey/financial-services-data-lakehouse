# Project Plan

## Project

**Financial Services Data Lakehouse**

## Objective

Build a realistic, independent financial-services Lakehouse using synthetic data based on a transaction-and-holdings domain. The platform will turn raw financial source data into governed, reusable data products for analytics, reporting, and downstream applications.

## Business Problem

Financial-services organizations commonly maintain transaction, holdings, security, account, portfolio, and related reference data across operational systems. Business consumers need consistent and trusted datasets for holdings, transaction activity, intraday positions, and monthly analysis.

The project will demonstrate how this type of financial data platform can be designed using modern Databricks architecture rather than implementing another one-off file-generation job.

## Scope

### In scope

- Synthetic financial source data
- Transaction and holdings data modeling
- Incremental ingestion
- Bronze/Silver/Gold architecture
- PySpark and SQL transformations
- Delta Lake tables
- Data-quality validation
- Reusable financial data products
- Databricks Workflows
- Unity Catalog concepts
- Documentation and architecture decisions
- Optional analytics/AI consumption layer

### Out of scope for the initial version

- Real client or employer data
- Production credentials or secrets
- Direct connection to employer systems
- Real-money trading
- Actual client file delivery
- Unnecessary ML components that do not solve a real problem

## Phases

### Phase 0 — Initialization
- Repository and documentation structure
- Business problem
- Initial architecture
- Core entities

### Phase 1 — Source Data Model
- Define TRN_TRANSACTIONS
- Define HOLDINGS
- Define SECURITY
- Identify supporting entities
- Create synthetic datasets
- Document relationships

### Phase 2 — Bronze Layer
- Source ingestion
- Raw Delta tables
- Incremental ingestion design
- Audit metadata

### Phase 3 — Silver Layer
- Cleansing
- Standardization
- Referential validation
- Financial business transformations
- Enrichment

### Phase 4 — Gold Data Products
- Holdings data product
- Transactions data product
- Intraday positions
- Monthly activity

### Phase 5 — Data Quality & Governance
- Quality rules
- Error handling
- Unity Catalog structure
- Metadata and lineage

### Phase 6 — Orchestration
- Databricks Workflows
- Dependencies
- Parameterization
- Incremental processing
- Failure handling

### Phase 7 — Consumption
- Databricks SQL
- Analytics use cases
- Optional AI/Genie layer

### Phase 8 — Finalization
- Architecture documentation
- Performance observations
- Testing
- Resume-ready project summary
- Demo walkthrough
