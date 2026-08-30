# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Transaction Transformation
# MAGIC
# MAGIC **Purpose**
# MAGIC - Read `TRN_TRANSACTIONS` from the Bronze layer.
# MAGIC - Standardize transaction identifiers and categorical attributes.
# MAGIC - Derive trade economics and cash-flow measures.
# MAGIC - Reconcile source `GROSS_AMOUNT` against `QUANTITY × PRICE`.
# MAGIC - Derive settlement and fee analytics.
# MAGIC - Enrich transactions with Security and Portfolio/Client dimensions.
# MAGIC - Classify transaction data quality.
# MAGIC - Persist the curated dataset as a Delta Silver table.
# MAGIC
# MAGIC **Architecture**
# MAGIC
# MAGIC `Oracle → Foreign Catalog → Bronze → PySpark transformations → Silver Transactions`
# MAGIC
# MAGIC **Design note:** Transaction Silver is intended to be a trusted, enriched transaction dataset. It remains transaction-grain data; portfolio-level aggregations belong downstream in Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports and source tables
# MAGIC
# MAGIC PySpark DataFrame transformations are used for the main processing logic. Spark SQL is reserved for cases such as DDL or ad-hoc inspection.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "`databricks-cata`.bronze.trn_transactions"
PORTFOLIO_TABLE = "`databricks-cata`.silver.portfolio"
SECURITY_TABLE = "`databricks-cata`.silver.security"
TARGET_TABLE = "`databricks-cata`.silver.trn_transactions"

transactions_df = spark.table(SOURCE_TABLE)
portfolio_df = spark.table(PORTFOLIO_TABLE)
security_df = spark.table(SECURITY_TABLE)

transactions_df.printSchema()

display(transactions_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source baseline and data-quality profile
# MAGIC
# MAGIC The transaction business key is `TRANSACTION_ID`. Before applying transformations, establish row counts and identifier uniqueness.

# COMMAND ----------

source_count = transactions_df.count()
distinct_transaction_ids = transactions_df.select("TRANSACTION_ID").distinct().count()

print(f"Total transactions: {source_count}")
print(f"Distinct transaction IDs: {distinct_transaction_ids}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect source classifications
# MAGIC
# MAGIC These distributions are used to understand the source semantics before defining Silver business rules.

# COMMAND ----------

display(
    transactions_df
    .groupBy("TRANSACTION_TYPE")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    transactions_df
    .groupBy("BUY_SELL_FLAG")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    transactions_df
    .groupBy("TRADE_STATUS")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize transaction attributes
# MAGIC
# MAGIC Convert Oracle decimal identifiers to `long` for the curated model and normalize categorical/string attributes for consistent downstream joins and filters.

# COMMAND ----------

transactions_silver_df = (
    transactions_df
    .withColumn("TRANSACTION_ID", F.col("TRANSACTION_ID").cast("long"))
    .withColumn("PORTFOLIO_ID", F.col("PORTFOLIO_ID").cast("long"))
    .withColumn("SECURITY_ID", F.col("SECURITY_ID").cast("long"))
    .withColumn("TRANSACTION_TYPE", F.upper(F.trim(F.col("TRANSACTION_TYPE"))))
    .withColumn("BUY_SELL_FLAG", F.upper(F.trim(F.col("BUY_SELL_FLAG"))))
    .withColumn("CURRENCY_CODE", F.upper(F.trim(F.col("CURRENCY_CODE"))))
    .withColumn("TRADE_STATUS", F.upper(F.trim(F.col("TRADE_STATUS"))))
    .withColumn("SOURCE_REFERENCE", F.trim(F.col("SOURCE_REFERENCE")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Derive trade economics
# MAGIC
# MAGIC `TRADE_VALUE` is independently derived as `QUANTITY × PRICE`. The source `GROSS_AMOUNT` is retained so it can be reconciled instead of blindly trusted.

# COMMAND ----------

transactions_silver_df = (
    transactions_silver_df
    .withColumn(
        "TRADE_VALUE",
        F.col("QUANTITY") * F.col("PRICE")
    )
    .withColumn(
        "GROSS_AMOUNT_VARIANCE",
        F.col("GROSS_AMOUNT") - F.col("TRADE_VALUE")
    )
    .withColumn(
        "AMOUNT_RECONCILIATION_STATUS",
        F.when(
            F.abs(F.col("GROSS_AMOUNT_VARIANCE")) < F.lit(0.01),
            F.lit("MATCHED")
        ).otherwise(F.lit("MISMATCH"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Derive transaction direction and cash-flow impact
# MAGIC
# MAGIC A buy represents cash leaving the portfolio, while a sell represents cash entering the portfolio. `NET_CASH_FLOW` therefore uses signed values for downstream portfolio-flow analysis.

# COMMAND ----------

transactions_silver_df = (
    transactions_silver_df
    .withColumn(
        "TRANSACTION_DIRECTION",
        F.when(F.col("BUY_SELL_FLAG") == "B", F.lit("BUY"))
         .when(F.col("BUY_SELL_FLAG") == "S", F.lit("SELL"))
         .otherwise(F.lit("UNKNOWN"))
    )
    .withColumn(
        "NET_CASH_FLOW",
        F.when(
            F.col("BUY_SELL_FLAG") == "B",
            -F.col("NET_AMOUNT")
        )
        .when(
            F.col("BUY_SELL_FLAG") == "S",
            F.col("NET_AMOUNT")
        )
        .otherwise(F.lit(0))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fee and settlement analytics
# MAGIC
# MAGIC Derive the effective fee rate and settlement lag. The derived settlement status separates the source trade status from a business interpretation of settlement state.

# COMMAND ----------

transactions_silver_df = (
    transactions_silver_df
    .withColumn(
        "FEE_RATE_PCT",
        F.when(
            F.col("GROSS_AMOUNT") != 0,
            (F.col("FEES") / F.col("GROSS_AMOUNT")) * F.lit(100.0)
        )
    )
    .withColumn(
        "SETTLEMENT_LAG_DAYS",
        F.datediff(
            F.to_date("SETTLEMENT_DATE"),
            F.to_date("TRANSACTION_DATE")
        )
    )
    .withColumn(
        "SETTLEMENT_STATUS",
        F.when(F.col("TRADE_STATUS") == "CANCELLED", F.lit("CANCELLED"))
         .when(F.col("SETTLEMENT_LAG_DAYS") < 0, F.lit("INVALID"))
         .when(F.col("TRADE_STATUS") == "SETTLED", F.lit("SETTLED"))
         .otherwise(F.lit("OUTSTANDING"))
    )
    .withColumn(
        "SETTLEMENT_LAG_CATEGORY",
        F.when(F.col("SETTLEMENT_STATUS") == "CANCELLED", F.lit("CANCELLED"))
         .when(F.col("SETTLEMENT_LAG_DAYS") == 0, F.lit("SAME_DAY"))
         .when(F.col("SETTLEMENT_LAG_DAYS") <= 2, F.lit("1_2_DAYS"))
         .when(F.col("SETTLEMENT_LAG_DAYS") > 2, F.lit("3_PLUS_DAYS"))
         .otherwise(F.lit("UNKNOWN"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Reporting attributes

# COMMAND ----------

transactions_silver_df = (
    transactions_silver_df
    .withColumn("TRANSACTION_YEAR", F.year("TRANSACTION_DATE"))
    .withColumn("TRANSACTION_MONTH", F.month("TRANSACTION_DATE"))
    .withColumn(
        "TRANSACTION_DATE_KEY",
        F.date_format("TRANSACTION_DATE", "yyyyMMdd").cast("int")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Enrich with Security master data
# MAGIC
# MAGIC The Security Silver table provides the canonical `ASSET_CLASS`. This avoids repeating security-type mapping logic in every downstream transaction consumer.

# COMMAND ----------

transactions_silver_df = (
    transactions_silver_df.alias("t")
    .join(
        security_df.select(
            "SECURITY_ID",
            "SECURITY_CODE",
            "SECURITY_NAME",
            "SECURITY_TYPE",
            "ASSET_CLASS"
        ).alias("s"),
        F.col("t.SECURITY_ID") == F.col("s.SECURITY_ID"),
        "left"
    )
    .select(
        "t.*",
        "s.SECURITY_CODE",
        "s.SECURITY_NAME",
        "s.SECURITY_TYPE",
        "s.ASSET_CLASS"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Enrich with Portfolio and Client hierarchy
# MAGIC
# MAGIC Portfolio Silver resolves `ACCOUNT_ID` and `CLIENT_ID`, allowing transactions to carry the business hierarchy required for client- and account-level analytics.

# COMMAND ----------

transactions_silver_df = (
    transactions_silver_df.alias("t")
    .join(
        portfolio_df.select(
            "PORTFOLIO_ID",
            "ACCOUNT_ID",
            "CLIENT_ID",
            "PORTFOLIO_TYPE",
            "BASE_CURRENCY"
        ).alias("p"),
        F.col("t.PORTFOLIO_ID") == F.col("p.PORTFOLIO_ID"),
        "left"
    )
    .select(
        "t.*",
        "p.ACCOUNT_ID",
        "p.CLIENT_ID",
        "p.PORTFOLIO_TYPE",
        "p.BASE_CURRENCY"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Transaction data-quality classification
# MAGIC
# MAGIC Invalid records are classified rather than silently dropped. This leaves room for a future quarantine/error table while preserving source observability.

# COMMAND ----------

transactions_silver_df = (
    transactions_silver_df
    .withColumn(
        "DATA_QUALITY_STATUS",
        F.when(F.col("TRANSACTION_ID").isNull(), F.lit("INVALID_TRANSACTION_ID"))
         .when(F.col("PORTFOLIO_ID").isNull(), F.lit("INVALID_PORTFOLIO"))
         .when(F.col("SECURITY_ID").isNull(), F.lit("INVALID_SECURITY"))
         .when(F.col("QUANTITY") <= 0, F.lit("INVALID_QUANTITY"))
         .when(F.col("PRICE") <= 0, F.lit("INVALID_PRICE"))
         .when(F.col("GROSS_AMOUNT") < 0, F.lit("INVALID_GROSS_AMOUNT"))
         .when(F.col("FEES") < 0, F.lit("INVALID_FEES"))
         .when(
             F.col("AMOUNT_RECONCILIATION_STATUS") == "MISMATCH",
             F.lit("AMOUNT_MISMATCH")
         )
         .when(
             F.col("SETTLEMENT_STATUS") == "INVALID",
             F.lit("INVALID_SETTLEMENT")
         )
         .otherwise(F.lit("VALID"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validation

# COMMAND ----------

print(f"Source transactions: {source_count}")
print(f"Transformed transactions: {transactions_silver_df.count()}")

# COMMAND ----------

duplicate_transactions = (
    transactions_silver_df
    .groupBy("TRANSACTION_ID")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_transactions)

# COMMAND ----------

display(
    transactions_silver_df
    .groupBy("AMOUNT_RECONCILIATION_STATUS")
    .count()
)

# COMMAND ----------

display(
    transactions_silver_df
    .groupBy("SETTLEMENT_STATUS")
    .count()
)

# COMMAND ----------

display(
    transactions_silver_df
    .groupBy("ASSET_CLASS")
    .count()
)

# COMMAND ----------

display(
    transactions_silver_df
    .groupBy("DATA_QUALITY_STATUS")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Persist the Silver transaction table

# COMMAND ----------

(
    transactions_silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Final validation after persistence

# COMMAND ----------

silver_transactions_df = spark.table(TARGET_TABLE)

print(f"Silver transaction records: {silver_transactions_df.count()}")

# COMMAND ----------

display(
    silver_transactions_df.select(
        "TRANSACTION_ID",
        "CLIENT_ID",
        "ACCOUNT_ID",
        "PORTFOLIO_ID",
        "SECURITY_ID",
        "SECURITY_NAME",
        "ASSET_CLASS",
        "TRANSACTION_TYPE",
        "TRANSACTION_DIRECTION",
        "QUANTITY",
        "PRICE",
        "TRADE_VALUE",
        "GROSS_AMOUNT",
        "FEES",
        "NET_AMOUNT",
        "NET_CASH_FLOW",
        "FEE_RATE_PCT",
        "TRADE_STATUS",
        "SETTLEMENT_STATUS",
        "SETTLEMENT_LAG_DAYS",
        "SETTLEMENT_LAG_CATEGORY",
        "AMOUNT_RECONCILIATION_STATUS",
        "DATA_QUALITY_STATUS"
    ).limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Outcome
# MAGIC
# MAGIC The Transaction Silver layer now contains transaction-grain data with:
# MAGIC
# MAGIC - Standardized identifiers and categorical fields
# MAGIC - Independently calculated trade value
# MAGIC - Source amount reconciliation
# MAGIC - Buy/sell direction and signed cash-flow impact
# MAGIC - Fee-rate analytics
# MAGIC - Settlement lag and derived settlement status
# MAGIC - Reporting date attributes
# MAGIC - Security and asset-class enrichment
# MAGIC - Portfolio, account, and client enrichment
# MAGIC - Explicit data-quality classification
# MAGIC - Delta persistence and post-write validation
# MAGIC
# MAGIC The next layer can safely aggregate these curated transactions into Gold business products such as portfolio cash flows, client activity, turnover, and performance analytics.
