# Gold Layer Design

## Purpose

The Gold layer exposes business-ready financial products rather than generic aggregations. Silver provides conformed, enriched datasets; Gold combines those datasets into views that support portfolio monitoring, performance/risk analysis, and client wealth activity.

The design currently targets three core business products:

1. `gold.portfolio_position_exposure`
2. `gold.portfolio_performance_risk`
3. `gold.client_wealth_activity`

The first product is implemented in the repository. The remaining products will be added as the corresponding transformations are completed and validated in Databricks.

## 1. Portfolio Position & Exposure

### Business questions

- What securities are currently held by each portfolio?
- What is each position worth?
- What proportion of portfolio AUM is represented by each security?
- How exposed is the portfolio to each asset class?
- Which positions are the largest concentration points?

### Silver inputs

- `silver.current_holdings`: latest position snapshot for each portfolio/security combination.
- `silver.security`: canonical security attributes and asset-class classification.

### Grain

One row per `PORTFOLIO_ID` + `SECURITY_ID` using the latest available holding snapshot.

### Key measures

- Position market value
- Cost basis
- Unrealized P&L
- Position allocation percentage
- Position rank within portfolio
- Asset-class market value
- Asset-class exposure percentage
- Asset-class security count
- Concentration bucket

### Business logic

`POSITION_ALLOCATION_PCT` is calculated as position market value divided by portfolio AUM.

`ASSET_CLASS_EXPOSURE_PCT` is calculated as asset-class market value divided by portfolio AUM.

Portfolio AUM is taken from `silver.current_holdings.PORTFOLIO_TOTAL_MARKET_VALUE`, which already represents the total market value of the current portfolio position set. This avoids recomputing the denominator from a grouped Gold dataset.

### Reconciliation

Asset-class exposure for a portfolio must reconcile to approximately 100%. A tolerance of +/- 0.01 percentage points is used in the notebook validation to account for decimal arithmetic.

## 2. Portfolio Performance & Risk

Planned product at one row per portfolio/reporting date. It will combine historical holdings and transaction activity to provide AUM, P&L, realized trading activity, turnover, concentration and asset-class exposure indicators.

This product should use the historical `silver.holdings` dataset rather than only `silver.current_holdings`, because performance and risk reporting require an as-of-date perspective.

## 3. Client Wealth Activity

Planned product at one row per client/reporting period. It will aggregate portfolio and transaction activity to provide total AUM, portfolio count, transaction volume, net cash flow and trading activity indicators.

Client linkage is provided through the Silver relationship `portfolio -> account -> client`.

## Design principles

- Gold metrics must have a defensible business interpretation.
- Do not add min/max/average statistics merely because they are easy to calculate.
- Preserve historical information in Silver and use current snapshots only where the business question is current-state position exposure.
- Keep Silver responsible for conformance, enrichment and reconciliation; Gold is responsible for business-facing products and measures.
- Build transformations in PySpark and persist the resulting business products as Delta tables in Unity Catalog.
