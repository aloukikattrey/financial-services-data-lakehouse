# Design Decisions

This document records important architectural decisions and the reasoning behind them.

## DD-001 — Build as a Separate Personal Project

**Decision:** Keep this project completely separate from the existing employer/client reporting platform.

**Reason:** The project should demonstrate independent Databricks engineering ability without copying proprietary architecture, code, data, schemas, or business rules.

## DD-002 — Financial Domain Instead of Generic Tutorial Data

**Decision:** Use transaction, holdings, security, and related financial entities as the core domain.

**Reason:** This produces a realistic financial-services use case and connects naturally to the type of data engineering problems the project is intended to demonstrate.

## DD-003 — Data Products Are the Primary Output

**Decision:** The primary output is reusable Gold financial data products, not Excel/CSV files.

**Reason:** A simple file generator would duplicate the concept of a traditional batch reporting system. The Lakehouse should instead provide trusted datasets that can support analytics, reporting, and downstream applications.

## DD-004 — Bronze/Silver/Gold Architecture

**Decision:** Separate raw ingestion, curated transformations, and business-ready outputs.

**Reason:** This provides clear data lifecycle boundaries and makes data quality, lineage, reprocessing, and downstream consumption easier to manage.

## DD-005 — Synthetic Data Only

**Decision:** All data in the repository will be synthetic.

**Reason:** Employer/client data, identifiers, SQL, schemas, and confidential business rules must not be exposed in a public personal project.

## DD-006 — Do Not Force MLflow Into the Core Pipeline

**Decision:** MLflow will only be introduced if a genuine ML/model-management requirement is added.

**Reason:** The Databricks Industry Solutions reference project may use MLflow, but adding it solely to match the reference would make the architecture artificial. The core project is primarily a data-engineering platform.

## DD-007 — Use the Databricks Industry Solution as Inspiration, Not a Copy

**Decision:** The `fsi-mrm-generation` solution is used as architectural/domain inspiration rather than copied as the final application.

**Reason:** The personal project should solve a distinct financial data-platform problem while demonstrating relevant Databricks capabilities.

## DD-008 — Excel/CSV as Optional Consumers

**Decision:** File generation may be implemented later as one downstream consumer of Gold data products.

**Reason:** This preserves a realistic reporting use case without making file creation the central architecture.
