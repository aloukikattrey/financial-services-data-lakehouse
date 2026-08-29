# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Holdings Transformation
# MAGIC
# MAGIC **Purpose**
# MAGIC - Read the Bronze `holdings` dataset.
# MAGIC - Deduplicate positions by portfolio, security, and snapshot date.
# MAGIC - Calculate financial measures such as unrealized P&L.
# MAGIC - Calculate portfolio-level exposure using a window function.
# MAGIC - Derive business classifications and reporting attributes.
# MAGIC - Persist the transformed dataset as a Delta Silver table.
# MAGIC
# MAGIC **Architecture**
# MAGIC
# MAGIC `Oracle → Foreign Catalog → Bronze Delta → PySpark transformations → Silver Delta`
# MAGIC
# MAGIC **Design note:** `HOLDINGS` is modeled as periodic point-in-time snapshots. Historical snapshots are retained; the Silver transformation does not collapse the table to only the latest date.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports and source table
# MAGIC
# MAGIC PySpark is used as the primary transformation framework. Spark SQL can still be used for ad-hoc validation, but the transformation logic is intentionally expressed with the DataFrame API.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "`databricks-cata`.bronze.holdings"
TARGET_TABLE = "`databricks-cata`.silver.holdings"

holdings_df = spark.table(SOURCE_TABLE)

display(holdings_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source quality baseline
# MAGIC
# MAGIC The Bronze source currently contains the controlled holdings snapshots used for the project. Before transformation, establish the row count and check the expected position key.
# MAGIC
# MAGIC For this project the logical position grain is:
# MAGIC
# MAGIC **`PORTFOLIO_ID + SECURITY_ID + AS_OF_DATE`**
# MAGIC
# MAGIC `HOLDING_ID` remains the source identifier.

# COMMAND ----------

source_count = holdings_df.count()
print(f"Bronze row count: {source_count}")

position_key_duplicates = (
    holdings_df
    .groupBy("PORTFOLIO_ID", "SECURITY_ID", "AS_OF_DATE")
    .count()
    .filter(F.col("count") > 1)
)

display(position_key_duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deduplicate the position grain
# MAGIC
# MAGIC A financial source can deliver multiple versions of the same position for the same portfolio, security, and valuation date. Silver keeps the most recently updated version.
# MAGIC
# MAGIC `row_number()` is used as the window function so the rule is deterministic when duplicates exist.

# COMMAND ----------

position_window = (
    Window
    .partitionBy("PORTFOLIO_ID", "SECURITY_ID", "AS_OF_DATE")
    .orderBy(
        F.col("UPDATED_AT").desc_nulls_last(),
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("HOLDING_ID").desc()
    )
)

holdings_deduped = (
    holdings_df
    .withColumn("_row_number", F.row_number().over(position_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)

print(f"Rows after deduplication: {holdings_deduped.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Financial calculations
# MAGIC
# MAGIC `MARKET_VALUE` and `COST_BASIS` are treated as source valuation fields. We derive:
# MAGIC
# MAGIC - **UNREALIZED_PNL** = `MARKET_VALUE - COST_BASIS`
# MAGIC - **UNREALIZED_PNL_PCT** = `(MARKET_VALUE - COST_BASIS) / COST_BASIS × 100`
# MAGIC
# MAGIC Division by zero is explicitly handled.

# COMMAND ----------

holdings_transformed = (
    holdings_deduped
    .withColumn(
        "UNREALIZED_PNL",
        F.col("MARKET_VALUE") - F.col("COST_BASIS")
    )
    .withColumn(
        "UNREALIZED_PNL_PCT",
        F.when(
            F.col("COST_BASIS") != 0,
            (
                (F.col("MARKET_VALUE") - F.col("COST_BASIS"))
                / F.col("COST_BASIS")
            ) * F.lit(100.0)
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Portfolio-level window calculations
# MAGIC
# MAGIC The same Silver row can carry both position-level and portfolio-level context.
# MAGIC
# MAGIC The partition is `PORTFOLIO_ID + AS_OF_DATE`, so every holding is compared with the total value of its portfolio on the same snapshot date.
# MAGIC
# MAGIC **PORTFOLIO_ALLOCATION_PCT** = position `MARKET_VALUE` / portfolio total `MARKET_VALUE` × 100

# COMMAND ----------

portfolio_window = Window.partitionBy("PORTFOLIO_ID", "AS_OF_DATE")

holdings_transformed = (
    holdings_transformed
    .withColumn(
        "PORTFOLIO_TOTAL_MARKET_VALUE",
        F.sum("MARKET_VALUE").over(portfolio_window)
    )
    .withColumn(
        "PORTFOLIO_ALLOCATION_PCT",
        F.when(
            F.col("PORTFOLIO_TOTAL_MARKET_VALUE") != 0,
            (
                F.col("MARKET_VALUE")
                / F.col("PORTFOLIO_TOTAL_MARKET_VALUE")
            ) * F.lit(100.0)
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Business classifications and reporting attributes
# MAGIC
# MAGIC Convert the numeric P&L into a business-friendly classification and derive calendar attributes used by downstream reporting.

# COMMAND ----------

holdings_transformed = (
    holdings_transformed
    .withColumn(
        "POSITION_PERFORMANCE",
        F.when(F.col("UNREALIZED_PNL") > 0, F.lit("GAIN"))
         .when(F.col("UNREALIZED_PNL") < 0, F.lit("LOSS"))
         .otherwise(F.lit("FLAT"))
    )
    .withColumn("AS_OF_YEAR", F.year("AS_OF_DATE"))
    .withColumn("AS_OF_MONTH", F.month("AS_OF_DATE"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver data-quality classification
# MAGIC
# MAGIC Silver identifies basic valuation/relationship issues without silently discarding records. This allows downstream consumers or a future quarantine process to decide how invalid records should be handled.

# COMMAND ----------

holdings_transformed = holdings_transformed.withColumn(
    "DATA_QUALITY_STATUS",
    F.when(F.col("MARKET_VALUE").isNull(), F.lit("MISSING_MARKET_VALUE"))
     .when(F.col("COST_BASIS").isNull(), F.lit("MISSING_COST_BASIS"))
     .when(F.col("QUANTITY").isNull(), F.lit("MISSING_QUANTITY"))
     .when(F.col("PORTFOLIO_ID").isNull(), F.lit("MISSING_PORTFOLIO"))
     .when(F.col("SECURITY_ID").isNull(), F.lit("MISSING_SECURITY"))
     .otherwise(F.lit("VALID"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Review the transformed dataset

# COMMAND ----------

display(
    holdings_transformed.select(
        "HOLDING_ID",
        "PORTFOLIO_ID",
        "SECURITY_ID",
        "AS_OF_DATE",
        "QUANTITY",
        "UNIT_PRICE",
        "MARKET_VALUE",
        "COST_BASIS",
        "UNREALIZED_PNL",
        "UNREALIZED_PNL_PCT",
        "PORTFOLIO_TOTAL_MARKET_VALUE",
        "PORTFOLIO_ALLOCATION_PCT",
        "POSITION_PERFORMANCE",
        "DATA_QUALITY_STATUS"
    ).orderBy("PORTFOLIO_ID", "AS_OF_DATE", F.desc("MARKET_VALUE"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Persist the Silver Delta table
# MAGIC
# MAGIC The initial implementation uses overwrite to create the target table. This is intentionally an initial-build pattern; later incremental Silver processing should use a controlled Delta upsert/merge design.

# COMMAND ----------

(
    holdings_transformed
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validation
# MAGIC
# MAGIC Validate row preservation, portfolio allocation, P&L calculation, and the business classification.

# COMMAND ----------

silver_holdings_df = spark.table(TARGET_TABLE)

print(f"Bronze rows: {source_count}")
print(f"Silver rows: {silver_holdings_df.count()}")

# COMMAND ----------

allocation_check = (
    silver_holdings_df
    .groupBy("PORTFOLIO_ID", "AS_OF_DATE")
    .agg(
        F.sum("PORTFOLIO_ALLOCATION_PCT").alias("total_allocation_pct"),
        F.first("PORTFOLIO_TOTAL_MARKET_VALUE").alias("portfolio_total_market_value")
    )
    .orderBy("PORTFOLIO_ID", "AS_OF_DATE")
)

display(allocation_check)

# COMMAND ----------

pnl_check = (
    silver_holdings_df
    .withColumn(
        "expected_pnl",
        F.col("MARKET_VALUE") - F.col("COST_BASIS")
    )
    .filter(
        ~F.col("UNREALIZED_PNL").eqNullSafe(F.col("expected_pnl"))
    )
)

display(pnl_check)

# COMMAND ----------

display(
    silver_holdings_df
    .groupBy("POSITION_PERFORMANCE", "DATA_QUALITY_STATUS")
    .count()
    .orderBy("POSITION_PERFORMANCE", "DATA_QUALITY_STATUS")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Outcome
# MAGIC
# MAGIC The Silver `HOLDINGS` transformation now demonstrates hands-on data-engineering logic rather than source copying:
# MAGIC
# MAGIC - Window-based deduplication
# MAGIC - Financial measure derivation
# MAGIC - Portfolio-level window aggregation
# MAGIC - Business classification
# MAGIC - Data-quality classification
# MAGIC - Delta persistence and validation
# MAGIC
# MAGIC Historical `AS_OF_DATE` snapshots remain available in Silver. A separate current-position transformation can be built downstream without losing the historical record.
