# Project Progress

## Current Phase

**Phase 4 — Gold Business Products**

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
- [x] Oracle setup documentation added
- [x] Oracle-to-Databricks connectivity documentation added

### Azure / Databricks foundation
- [x] Azure Databricks workspace created
- [x] Unity Catalog metastore created and assigned to the workspace
- [x] Azure Databricks Access Connector configured
- [x] Storage permissions configured for the metastore storage account
- [x] Unity Catalog enabled
- [x] Project catalog `databricks-cata` created
- [x] Databricks storage credential `aloukik_creds` created and configured
- [x] Development Databricks compute configured for Oracle connectivity testing

### Local Oracle source foundation
- [x] Oracle Database Free Lite container created and verified
- [x] `FINANCE_APP` schema created
- [x] Oracle source tables created:
  - `CLIENT`
  - `ACCOUNT`
  - `PORTFOLIO`
  - `SECURITY`
  - `HOLDINGS`
  - `TRN_TRANSACTIONS`
- [x] Synthetic client, account, portfolio, security, holdings and transaction data generated
- [x] Initial transaction financial-consistency validation performed
- [x] Source-system primary/foreign-key relationships established

### Oracle → Databricks connectivity
- [x] Temporary TCP development tunnel established
- [x] Databricks-to-Oracle TCP connectivity verified
- [x] Project schema and JDBC-driver managed volume created
- [x] Oracle JDBC driver uploaded
- [x] Databricks Oracle connection created
- [x] Oracle foreign catalog `oracle_finance_source_catalog` created
- [x] Oracle `FINANCE_APP` schema exposed through Lakehouse Federation
- [x] Source tables queried successfully from Databricks

### Bronze
- [x] Oracle source ingestion foundation documented
- [x] Bronze layer design and ingestion metadata documented
- [x] Bronze Delta ingestion implemented for the project source domains

### Silver
- [x] `silver.client` implemented
- [x] `silver.account` implemented
- [x] `silver.portfolio` implemented
- [x] `silver.security` implemented
- [x] `silver.holdings` implemented with historical as-of-date positions
- [x] `silver.current_holdings` implemented as latest position snapshot
- [x] `silver.trn_transactions` implemented with transaction enrichment and reconciliation
- [x] Silver data-quality and referential validations performed

### Gold
- [x] Gold layer design documented under `docs/gold/GOLD_LAYER_DESIGN.md`
- [x] `gold.portfolio_position_exposure` transformation notebook created
- [x] Portfolio position allocation and ranking logic defined
- [x] Asset-class exposure aggregation defined
- [x] Gold reconciliation validations defined

## In Progress

### Phase 4 — Gold
- [ ] Run and validate `gold.portfolio_position_exposure` in Databricks
- [ ] Review exposure and concentration outputs against business expectations
- [ ] Implement `gold.portfolio_performance_risk`
- [ ] Implement `gold.client_wealth_activity`
- [ ] Validate Gold outputs and reconciliation across products

## Upcoming

### Phase 5 — Quality & Governance
- [ ] Build reusable quality checks
- [ ] Define error/quarantine handling
- [ ] Configure project schemas and grants
- [ ] Document lineage and governance

### Phase 6 — Orchestration
- [ ] Create Databricks Workflow
- [ ] Add task dependencies
- [ ] Add parameters
- [ ] Test failure/retry behavior

### Phase 7 — Consumption
- [ ] Build Databricks SQL examples
- [ ] Add portfolio and client analytics use cases
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

## Gold Product Roadmap

| Product | Grain | Primary purpose | Status |
|---|---|---|---|
| `gold.portfolio_position_exposure` | Portfolio + Security | Current positions, allocation, concentration and asset-class exposure | In implementation |
| `gold.portfolio_performance_risk` | Portfolio + Reporting Date | AUM, P&L, trading activity, turnover and portfolio risk indicators | Planned |
| `gold.client_wealth_activity` | Client + Reporting Period | Client AUM, portfolio count, net cash flow and trading activity | Planned |

## Design Principle

The Gold layer is treated as a set of business products, not as a collection of arbitrary aggregations. Metrics are included only when they answer a defensible financial or operational question. Historical context remains in Silver and is used where the business question requires an as-of-date or trend perspective.

## Change Log

### Initial setup
Repository structure and project documentation created.

### Databricks foundation
Azure Databricks workspace, Unity Catalog metastore, Access Connector, managed storage, and project catalog were configured.

### Oracle source implementation
A local Oracle Database Free Lite environment was configured with the `FINANCE_APP` schema, financial tables, relationships, and representative source datasets.

### Oracle-to-Databricks connectivity
A temporary development tunnel was established between local Oracle and Azure Databricks. Databricks connectivity was verified, the Oracle JDBC driver was uploaded to a Unity Catalog managed volume, the Oracle connection was created, and the foreign catalog was successfully queried.

### Silver implementation
Conformed Silver datasets were implemented for clients, accounts, portfolios, securities, holdings, current holdings and transactions. Silver transformations add analytical standardization, cross-domain enrichment, business semantics, and reconciliation while leaving source-level data quality ownership with the upstream Oracle/source system.

### Gold implementation
Gold design was refined around three meaningful financial products. The first implementation, `gold.portfolio_position_exposure`, combines current holdings with the Security master to expose position allocation, security ranking, concentration buckets and asset-class exposure. Documentation was added under `docs/gold/` and the Gold notebook was added under `notebooks/gold/`.
