# Gold: Portfolio Position & Exposure

## Overview

`gold.portfolio_position_exposure` is the first Gold-layer business product in the lakehouse. It combines the latest portfolio positions with the conformed Security master to provide a reusable current-state view for portfolio monitoring and exposure analysis.

## Data flow

```text
silver.current_holdings ─────┐
                             ├──> gold.portfolio_position_exposure
silver.security ─────────────┘
```

## Grain

One row per `PORTFOLIO_ID` + `SECURITY_ID`.

The input is `silver.current_holdings`, so each row represents the latest known position for that portfolio/security combination.

## Main transformations

### Security enrichment

Current holdings are left-joined to `silver.security` to add:

- `SECURITY_CODE`
- `SECURITY_NAME`
- `SECURITY_TYPE`
- `ASSET_CLASS`
- `IS_ACTIVE`

`ASSET_CLASS` comes from the canonical classification created in Silver and is therefore reusable across Gold products.

### Position allocation

```text
POSITION_ALLOCATION_PCT
    = MARKET_VALUE / PORTFOLIO_TOTAL_MARKET_VALUE * 100
```

This answers how much of portfolio AUM is represented by an individual position.

### Position ranking

Positions are ranked within each portfolio using descending market value. This makes the largest positions immediately identifiable for concentration analysis.

### Concentration bucket

The current implementation uses simple business thresholds:

- `HIGH`: position allocation >= 10%
- `MEDIUM`: position allocation >= 5% and < 10%
- `LOW`: position allocation < 5%

These thresholds are project-defined indicators, not regulatory limits or investment-policy recommendations.

### Asset-class exposure

Positions are grouped by portfolio and canonical asset class. The transformation calculates:

```text
ASSET_CLASS_MARKET_VALUE
ASSET_CLASS_COST_BASIS
ASSET_CLASS_UNREALIZED_PNL
SECURITY_COUNT
PORTFOLIO_AUM
ASSET_CLASS_EXPOSURE_PCT
```

`ASSET_CLASS_EXPOSURE_PCT` is calculated as:

```text
ASSET_CLASS_MARKET_VALUE / PORTFOLIO_AUM * 100
```

The denominator is taken from `silver.current_holdings.PORTFOLIO_TOTAL_MARKET_VALUE`, rather than recomputed from a grouped Gold result.

## Validation rules

The notebook validates that:

1. The Gold grain is unique by portfolio and security.
2. Every current position successfully enriches to a Security record.
3. Asset-class exposure reconciles to approximately 100% per portfolio within a 0.01 percentage-point tolerance.

## Why this belongs in Gold

The input tables are already standardized and enriched in Silver, but they are not yet a business-facing portfolio product. Gold turns those reusable conformed datasets into measures and indicators that can directly support portfolio reporting, concentration review and downstream BI.

## Implementation

Notebook:

`notebooks/gold/portfolio_position_exposure_gold.py`
