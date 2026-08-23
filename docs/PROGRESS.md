# Project Progress

## Current Phase

**Phase 2 — Oracle to Databricks Connectivity / Bronze Preparation**

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
- [x] Oracle-to-Databricks connectivity documentation added under `docs/oracle/ORACLE_TO_DATABRICKS_SETUP.md`

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
- [x] Databricks storage credential `aloukik_creds` created
- [x] `aloukik_creds` assigned as the metastore root storage credential
- [x] Serverless Starter Warehouse available for SQL work
- [x] Development all-purpose compute configured for Oracle connectivity testing
- [x] Development compute set to DBR 17.3 LTS, single-node, Photon disabled, short auto-termination

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

### Oracle → Databricks connectivity foundation
- [x] Local Oracle port `1521` verified as reachable
- [x] Temporary TCP development tunnel established from local Oracle to an Internet-accessible endpoint
- [x] Databricks notebook created for connectivity testing
- [x] Databricks-to-tunnel TCP connectivity test succeeded
- [x] Project schema `databricks-cata.finance` created
- [x] Managed volume `databricks-cata.finance.jdbc_drivers` created successfully
- [x] Oracle JDBC driver `ojdbc11.jar` uploaded to the managed volume
- [x] Databricks metastore root storage credential issue diagnosed and fixed
- [x] Existing `aloukik_creds` credential associated with the metastore root storage
- [x] Databricks Oracle connection created
- [x] Oracle foreign catalog `oracle_finance_source_catalog` created using service `FREEPDB1`
- [x] Foreign catalog access configured for the project owner
- [x] Oracle `FINANCE_APP` schema exposed through the foreign catalog
- [x] Source tables visible in Databricks through Lakehouse Federation
- [x] Source data successfully queried from Databricks

## In Progress

- [ ] Validate JDBC/federation access across all source tables
- [ ] Finalize source validation and row-count checks
- [ ] Decide batch JDBC vs incremental extraction pattern
- [ ] Document final source-table column definitions in `DATA_DICTIONARY.md`
- [ ] Document source-table relationships in `DATA_MODEL.md`

## Upcoming

### Phase 2 — Oracle to Bronze
- [ ] Implement Oracle-to-Bronze ingestion
- [ ] Create Bronze Delta tables
- [ ] Add ingestion metadata
- [ ] Implement incremental extraction strategy
- [ ] Test repeatable loads
- [ ] Add data-quality checks for Bronze ingestion

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

### Oracle source implementation
A fresh Oracle Database Free Lite container was configured locally. The `FINANCE_APP` schema, financial tables, relationships, and synthetic source datasets were created. The detailed setup and SQL are recorded in `docs/oracle/ORACLE_SOURCE_SETUP.md`.

### Oracle-to-Databricks connectivity
A temporary development tunnel was established between the local Oracle listener and Azure Databricks. TCP connectivity from the Databricks cluster was verified successfully. Unity Catalog managed-storage configuration was corrected by assigning `aloukik_creds` as the metastore root storage credential, after which the JDBC-driver managed volume was created successfully. `ojdbc11.jar` was uploaded, the Oracle connection was created, and the Oracle foreign catalog was successfully created and queried. The detailed connectivity setup is recorded in `docs/oracle/ORACLE_TO_DATABRICKS_SETUP.md`.
