-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze ingestion
-- MAGIC Extracted from docs/databricks/BRONZE_INGESTION_SETUP.md.
-- MAGIC Requires oracle_finance_source_catalog.finance_app and the databricks-cata catalog.
-- MAGIC The Oracle connection/tunnel must be active and the job identity must have access.
-- MAGIC Run with at most one concurrent job run; do not run this notebook concurrently elsewhere.
-- MAGIC Reference tables refresh in full. Holdings snapshots must be complete, immutable and arrive
-- MAGIC with strictly newer AS_OF_DATE values. Transactions require reliable UPDATED_AT values.
-- MAGIC Strict timestamp watermarks do not capture late arrivals at/below the watermark or deletes.
-- MAGIC Each table write is atomic; the entire notebook is not one transaction.
-- MAGIC Initial loads are automatic. No source test-data changes are executed.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS `databricks-cata`.bronze;

-- COMMAND ----------

-- Full refresh: client
CREATE OR REPLACE TABLE `databricks-cata`.bronze.client
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.CLIENT' AS source_table
FROM oracle_finance_source_catalog.finance_app.client;

-- COMMAND ----------

-- Full refresh: account
CREATE OR REPLACE TABLE `databricks-cata`.bronze.account
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.ACCOUNT' AS source_table
FROM oracle_finance_source_catalog.finance_app.ACCOUNT;

-- COMMAND ----------

-- Full refresh: portfolio
CREATE OR REPLACE TABLE `databricks-cata`.bronze.portfolio
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.PORTFOLIO' AS source_table
FROM oracle_finance_source_catalog.finance_app.portfolio;

-- COMMAND ----------

-- Full refresh: security
CREATE OR REPLACE TABLE `databricks-cata`.bronze.security
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.SECURITY' AS source_table
FROM oracle_finance_source_catalog.finance_app.security;

-- COMMAND ----------

-- Initial load only: holdings
CREATE TABLE IF NOT EXISTS `databricks-cata`.bronze.holdings
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.HOLDINGS' AS source_table
FROM oracle_finance_source_catalog.finance_app.HOLDINGS;

-- COMMAND ----------

-- Initial load only: trn_transactions
CREATE TABLE IF NOT EXISTS `databricks-cata`.bronze.trn_transactions
USING DELTA
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.TRN_TRANSACTIONS' AS source_table
FROM oracle_finance_source_catalog.finance_app.TRN_TRANSACTIONS;

-- COMMAND ----------

-- Append newer holdings snapshots; also handles an empty target.
INSERT INTO `databricks-cata`.bronze.holdings
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'ORACLE' AS source_system,
    'FINANCE_APP.HOLDINGS' AS source_table
FROM oracle_finance_source_catalog.finance_app.HOLDINGS
WHERE (SELECT MAX(AS_OF_DATE) FROM `databricks-cata`.bronze.holdings) IS NULL
   OR AS_OF_DATE >
      (
          SELECT MAX(AS_OF_DATE)
          FROM `databricks-cata`.bronze.holdings
      );

-- COMMAND ----------

-- Upsert changed transactions; also handles an empty target.
MERGE INTO `databricks-cata`.bronze.trn_transactions AS target
USING (
    SELECT
        *,
        current_timestamp() AS ingestion_timestamp,
        'ORACLE' AS source_system,
        'FINANCE_APP.TRN_TRANSACTIONS' AS source_table
    FROM oracle_finance_source_catalog.finance_app.TRN_TRANSACTIONS
    WHERE (SELECT MAX(UPDATED_AT) FROM `databricks-cata`.bronze.trn_transactions) IS NULL
       OR UPDATED_AT >
          (
              SELECT MAX(UPDATED_AT)
              FROM `databricks-cata`.bronze.trn_transactions
          )
) AS source
ON target.TRANSACTION_ID = source.TRANSACTION_ID

WHEN MATCHED THEN UPDATE SET
    target.PORTFOLIO_ID = source.PORTFOLIO_ID,
    target.SECURITY_ID = source.SECURITY_ID,
    target.TRANSACTION_DATE = source.TRANSACTION_DATE,
    target.SETTLEMENT_DATE = source.SETTLEMENT_DATE,
    target.TRANSACTION_TYPE = source.TRANSACTION_TYPE,
    target.BUY_SELL_FLAG = source.BUY_SELL_FLAG,
    target.QUANTITY = source.QUANTITY,
    target.PRICE = source.PRICE,
    target.GROSS_AMOUNT = source.GROSS_AMOUNT,
    target.FEES = source.FEES,
    target.NET_AMOUNT = source.NET_AMOUNT,
    target.CURRENCY_CODE = source.CURRENCY_CODE,
    target.TRADE_STATUS = source.TRADE_STATUS,
    target.SOURCE_REFERENCE = source.SOURCE_REFERENCE,
    target.CREATED_AT = source.CREATED_AT,
    target.UPDATED_AT = source.UPDATED_AT,
    target.ingestion_timestamp = source.ingestion_timestamp

WHEN NOT MATCHED THEN INSERT (
    TRANSACTION_ID,
    PORTFOLIO_ID,
    SECURITY_ID,
    TRANSACTION_DATE,
    SETTLEMENT_DATE,
    TRANSACTION_TYPE,
    BUY_SELL_FLAG,
    QUANTITY,
    PRICE,
    GROSS_AMOUNT,
    FEES,
    NET_AMOUNT,
    CURRENCY_CODE,
    TRADE_STATUS,
    SOURCE_REFERENCE,
    CREATED_AT,
    UPDATED_AT,
    ingestion_timestamp,
    source_system,
    source_table
)
VALUES (
    source.TRANSACTION_ID,
    source.PORTFOLIO_ID,
    source.SECURITY_ID,
    source.TRANSACTION_DATE,
    source.SETTLEMENT_DATE,
    source.TRANSACTION_TYPE,
    source.BUY_SELL_FLAG,
    source.QUANTITY,
    source.PRICE,
    source.GROSS_AMOUNT,
    source.FEES,
    source.NET_AMOUNT,
    source.CURRENCY_CODE,
    source.TRADE_STATUS,
    source.SOURCE_REFERENCE,
    source.CREATED_AT,
    source.UPDATED_AT,
    source.ingestion_timestamp,
    source.source_system,
    source.source_table
);

-- COMMAND ----------

-- Observability: counts and watermarks (not source reconciliation).
SELECT 'client' AS table_name, COUNT(*) AS record_count FROM `databricks-cata`.bronze.client
UNION ALL
SELECT 'account' AS table_name, COUNT(*) AS record_count FROM `databricks-cata`.bronze.account
UNION ALL
SELECT 'portfolio' AS table_name, COUNT(*) AS record_count FROM `databricks-cata`.bronze.portfolio
UNION ALL
SELECT 'security' AS table_name, COUNT(*) AS record_count FROM `databricks-cata`.bronze.security
UNION ALL
SELECT 'holdings' AS table_name, COUNT(*) AS record_count FROM `databricks-cata`.bronze.holdings
UNION ALL
SELECT 'trn_transactions' AS table_name, COUNT(*) AS record_count FROM `databricks-cata`.bronze.trn_transactions;

-- COMMAND ----------

SELECT MAX(AS_OF_DATE) AS holdings_watermark FROM `databricks-cata`.bronze.holdings;

-- COMMAND ----------

SELECT MAX(UPDATED_AT) AS transactions_watermark FROM `databricks-cata`.bronze.trn_transactions;
