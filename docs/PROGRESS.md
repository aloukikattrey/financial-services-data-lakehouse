# Project Progress

## Current Phase

**Phase 1 — Source Data Model / Oracle Source Complete**

## Completed

### Repository foundation
- [x] GitHub repository created
- [x] README created
- [x] Project scope defined
- [x] Business problem defined
- [x] Initial target architecture documented
- [x] Core financial entities identified
- [x] Data-model documentation created
- [x] Data-dictionary template created
- [x] Initial design decisions documented
- [x] Repository template directories created
- [x] Oracle setup documentation added under `docs/oracle/ORACLE_SOURCE_SETUP.md`

### Azure / Databricks foundation
- [x] Azure for Students subscription confirmed as the project subscription
- [x] Azure Databricks workspace created in Central India
- [x] Hybrid workspace selected because it was the available workspace type for the selected trial configuration
- [x] Databricks account access established
- [x] Account admin access established
- [x] Unity Catalog metastore created and assigned to the workspace
- [x] Azure Databricks Access Connector created
- [x] Access Connector configured with a system-assigned managed identity
- [x] Access Connector storage permissions configured for the metastore storage account
- [x] Unity Catalog enabled on the workspace
- [x] Unity Catalog catalog `databricks-cata` created
- [x] Serverless Starter Warehouse available for SQL work

### Local Oracle source foundation
- [x] Docker Desktop installed and running
- [x] Existing Oracle Database Free Lite image selected
- [x] Persistent Docker volume created for Oracle data
- [x] Fresh `oracle-finance-db` container created and verified
- [x] Oracle listener verified on port `1521`
- [x] `FREE` and `FREEPDB1` services verified as ready
- [x] Connection to `FREE` verified from the host
- [x] `FREEPDB1` selected for application data
- [x] Dedicated `FINANCE_DATA` tablespace created
- [x] `FINANCE_APP` application schema created and configured
- [x] Oracle source tables created:
  - `CLIENT`
  - `ACCOUNT`
  - `PORTFOLIO`
  - `SECURITY`
  - `HOLDINGS`
  - `TRN_TRANSACTIONS`
- [x] Synthetic client data generated
- [x] Synthetic account data generated
- [x] Synthetic portfolio data generated
- [x] Synthetic security master data generated
- [x] Synthetic holdings data generated across multiple as-of dates
- [x] Synthetic transaction data generated
- [x] Initial transaction financial-consistency validation performed
- [x] Source-system relationships established through primary/foreign keys

## In Progress

- [ ] Final validation of Oracle source data and counts
- [ ] Validate transaction amount calculations and domain consistency
- [ ] Validate holdings values and date coverage
- [ ] Add/verify source indexes where needed
- [ ] Document final source-table column definitions in `DATA_DICTIONARY.md`
- [ ] Document source-table relationships in `DATA_MODEL.md`

## Upcoming

### Phase 2 — Oracle to Bronze
- [ ] Establish network connectivity from Azure Databricks to the local Oracle source
- [ ] Create Databricks Oracle connection
- [ ] Test Oracle connectivity from Databricks
- [ ] Implement JDBC extraction
- [ ] Create Bronze Delta tables
- [ ] Add ingestion metadata
- [ ] Implement incremental extraction strategy
- [ ] Test repeatable loads

### Phase 3 — Silver
- [ ] Clean transaction data
- [ ] Clean holdings data
- [ ] Enrich with security/reference data
- [ ] Implement domain validations
- [ ] Create Silver Delta tables

### Phase 4 — Gold
- [ ] Create Holdings data product
- [ ] Create Transactions data product
- [ ] Create Intraday Positions data product
- [ ] Create Monthly Activity data product
- [ ] Validate Gold outputs

### Phase 5 — Quality & Governance
- [ ] Build reusable quality checks
- [ ] Define error/quarantine handling
- [ ] Configure project schemas in `databricks-cata`
- [ ] Document lineage and governance

### Phase 6 — Orchestration
- [ ] Create Databricks Workflow
- [ ] Add task dependencies
- [ ] Add parameters
- [ ] Test failure/retry behavior

### Phase 7 — Consumption
- [ ] Build Databricks SQL examples
- [ ] Add analytics use cases
- [ ] Evaluate optional AI/Genie layer

### Phase 8 — Advanced Enhancements
- [ ] Evaluate Oracle CDC
- [ ] Optimize incremental processing
- [ ] Evaluate partitioning/clustering where justified
- [ ] Performance benchmark

### Phase 9 — Finalization
- [ ] Add architecture diagram
- [ ] Add testing documentation
- [ ] Add performance observations
- [ ] Prepare final project walkthrough
- [ ] Prepare resume bullets

## Change Log

### Initial setup
Repository structure and project documentation created.

### Databricks foundation
Azure Databricks workspace, Unity Catalog metastore, Access Connector, managed storage, and project catalog were configured.

### Oracle source setup
Docker Desktop was installed to host a local synthetic Oracle operational source without introducing an always-on Azure Oracle VM cost.

### Oracle source implementation
A fresh Oracle Database Free Lite container was configured locally. The `FINANCE_APP` schema, financial tables, relationships, and synthetic source datasets were created. The detailed setup and SQL are recorded in `docs/oracle/ORACLE_SOURCE_SETUP.md`.
