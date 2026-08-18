# Data Dictionary

This document tracks the logical meaning of the source and curated datasets. Exact column definitions will be filled in as the synthetic source data is created.

## TRN_TRANSACTIONS

| Column | Type | Description | Key/Rule |
|---|---|---|---|
| transaction_id | TBD | Unique transaction identifier | Primary/business key |
| account_id | TBD | Account associated with transaction | Reference to account |
| security_id | TBD | Security involved in transaction | Reference to security |
| transaction_type | TBD | Type of transaction | Reference/business rule |
| trade_date | TBD | Date transaction was traded | Required |
| settlement_date | TBD | Settlement date where applicable | Domain rule |
| quantity | TBD | Units involved in transaction | Domain validation |
| price | TBD | Transaction price | Domain validation |
| currency | TBD | Currency of transaction | Reference validation |
| amount | TBD | Transaction monetary amount | Derived/source |

## HOLDINGS

| Column | Type | Description | Key/Rule |
|---|---|---|---|
| account_id | TBD | Account holding the position | Composite relationship |
| security_id | TBD | Held security | Reference to security |
| as_of_date | TBD | Position snapshot date | Required |
| quantity | TBD | Position quantity | Domain validation |
| market_value | TBD | Market value of position | Domain validation |
| currency | TBD | Position currency | Reference validation |

## SECURITY

| Column | Type | Description | Key/Rule |
|---|---|---|---|
| security_id | TBD | Unique security identifier | Primary/business key |
| security_name | TBD | Security name | Reference |
| security_type | TBD | Instrument/security classification | Reference |
| currency | TBD | Security currency | Reference |

## Gold Data Products

Detailed dictionaries for Gold datasets will be created after the Silver transformations are finalized.

Planned datasets:

- `gold.client_holdings`
- `gold.client_transactions`
- `gold.intraday_positions`
- `gold.monthly_activity`
- `gold.security_reference`

## Status

This is a living document. Definitions will be updated whenever the data model changes.
