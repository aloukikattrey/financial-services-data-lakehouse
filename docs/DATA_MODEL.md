# Data Model

## Initial Domain Model

The project starts from a financial-services domain centered on three primary entities.

```text
                 +----------------+
                 |    SECURITY    |
                 +--------+-------+
                          |
             +------------+------------+
             |                         |
             v                         v
   +------------------+       +------------------+
   |  TRN_TRANSACTIONS|       |     HOLDINGS     |
   +------------------+       +------------------+
             |                         |
             +------------+------------+
                          |
                 Supporting entities
                 account / portfolio /
                 client / reference
```

## TRN_TRANSACTIONS

Represents transaction-level financial activity.

Expected logical attributes will include:

- Transaction identifier
- Account identifier
- Security identifier
- Transaction type
- Trade date
- Settlement date where applicable
- Quantity
- Price
- Currency
- Amount/value fields where applicable

The exact schema will be finalized from the synthetic model rather than copied from any employer system.

## HOLDINGS

Represents a position or holding snapshot.

Expected logical attributes will include:

- Account identifier
- Security identifier
- As-of date
- Quantity
- Market value
- Currency
- Portfolio/client relationship where applicable

## SECURITY

Reference/master entity describing financial instruments.

Expected logical attributes will include:

- Security identifier
- Security name
- Security type
- Currency
- Market/reference attributes needed for enrichment

## Supporting Entities

Potential supporting entities:

- Client
- Account
- Portfolio
- Currency/reference data
- Transaction type/reference data

These will only be added when they provide a real modeling or transformation purpose.

## Modeling Direction

The final Gold model is expected to contain reusable financial data products rather than a single report-specific table.

Potential outputs:

- `gold.client_holdings`
- `gold.client_transactions`
- `gold.intraday_positions`
- `gold.monthly_activity`
- `gold.security_reference`

## Important Constraint

No production/client schema, records, identifiers, SQL, or confidential business logic will be copied into this repository. The model will use synthetic equivalents that preserve the business relationships needed for the project.
