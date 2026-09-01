# Databricks Pipelines and Jobs

## Purpose

The project uses Databricks orchestration features to move beyond notebook-only processing and represent a repeatable financial data platform.

The implementation separates two concerns:

- **Databricks Pipeline** — manages the Bronze-to-Silver transformation flow and its data-quality rules.
- **Databricks Job** — orchestrates the end-to-end execution, including ingestion, the Silver pipeline, Gold processing, and validation.

## Target execution flow

```text
Oracle / Source
      |
      v
Bronze ingestion
      |
      v
Silver Pipeline
      |
      +--> Client
      +--> Account
      +--> Portfolio
      +--> Security
      +--> Holdings
      +--> Current Holdings
      +--> Transactions
      |
      v
Gold transformations
      |
      v
Data-quality / reconciliation checks
```

## Why both are used

A pipeline is appropriate for managing a connected transformation graph and data-quality expectations. A Job is appropriate for operational orchestration: task dependencies, parameters, retries, scheduling, and run monitoring.

This avoids using Jobs and Pipelines as unrelated demonstrations. Each feature has a defined role in the financial lakehouse.

## Initial implementation scope

### Pipeline

The first pipeline should own the Bronze-to-Silver processing path. Existing Silver PySpark transformations provide the business logic; the pipeline provides dependency management, execution, and quality controls.

### Job

The first Job should coordinate:

1. Bronze ingestion
2. Silver pipeline execution
3. Gold portfolio-position transformation
4. Data-quality validation

The Job should fail when a required upstream task or validation fails.

## Future enhancements

Once the basic orchestration is working, the project can add:

- incremental processing parameters
- retry policies
- scheduled execution
- SCD processing where business requirements justify it
- operational metrics and run logging
- separate development and production configuration
- additional Gold products

The objective is to demonstrate production-oriented Databricks engineering without adding artificial components solely to increase feature count.
