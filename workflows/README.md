# Databricks Workflows

This directory contains the version-controlled definitions and documentation for Databricks orchestration.

## Planned workflow

`financial_lakehouse_daily`

```text
Bronze Ingestion
      |
      v
Silver Pipeline
      |
      v
Gold Position Exposure
      |
      v
Data Quality Validation
```

The workflow is intentionally small at first. Additional Gold products will be added only when they provide a meaningful business outcome.

## Deployment

The workflow definitions are maintained as source-controlled project artifacts. The first implementation will be created and validated in the Databricks workspace before deployment automation is introduced.
