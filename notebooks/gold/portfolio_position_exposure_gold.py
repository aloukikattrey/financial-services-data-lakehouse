# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Portfolio Position & Exposure
# MAGIC
# MAGIC **Business purpose**
# MAGIC
# MAGIC Create a portfolio-level investment view that answers:
# MAGIC - What securities are currently held in each portfolio?
# MAGIC - What is each position worth?
# MAGIC - What percentage of portfolio AUM does each position represent?
# MAGIC - How concentrated is the portfolio by security and asset class?
# MAGIC
# MAGIC **Inputs**
# MAGIC - `silver.current_holdings`
# MAGIC - `silver.security`
# MAGIC
# MAGIC **Output**
# MAGIC - `gold.portfolio_position_exposure`
# MAGIC
# MAGIC The product is intentionally focused on current positions. Historical holdings remain available through `silver.holdings` and can support future as-of-date analytics.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Load Silver inputs
current_holdings_df = spark.table("silver.current_holdings")
security_df = spark.table("silver.security")

# COMMAND ----------

# Enrich current positions with the security master.
# Security Silver provides the canonical asset classification used by Gold.
position_df = (
    current_holdings_df.alias("h")
    .join(
        security_df.select(
            "SECURITY_ID",
            "SECURITY_CODE",
            "SECURITY_NAME",
            "SECURITY_TYPE",
            "ASSET_CLASS",
            "IS_ACTIVE"
        ).alias("s"),
        on="SECURITY_ID",
        how="left"
    )
    .select(
        "h.*",
        "s.SECURITY_CODE",
        "s.SECURITY_NAME",
        "s.SECURITY_TYPE",
        "s.ASSET_CLASS",
        "s.IS_ACTIVE"
    )
)

# COMMAND ----------

# Security-level position metrics.
# Current holdings already contains PORTFOLIO_TOTAL_MARKET_VALUE, so use it as
# the portfolio AUM denominator rather than recomputing it from grouped rows.
security_window = Window.partitionBy("PORTFOLIO_ID").orderBy(
    F.col("MARKET_VALUE").desc(),
    F.col("SECURITY_ID")
)

portfolio_position_df = (
    position_df
    .withColumn(
        "POSITION_ALLOCATION_PCT",
        F.when(
            F.col("PORTFOLIO_TOTAL_MARKET_VALUE") != 0,
            (F.col("MARKET_VALUE") / F.col("PORTFOLIO_TOTAL_MARKET_VALUE")) * 100
        )
    )
    .withColumn("POSITION_RANK", F.row_number().over(security_window))
    .withColumn(
        "CONCENTRATION_BUCKET",
        F.when(F.col("POSITION_ALLOCATION_PCT") >= 10, F.lit("HIGH"))
         .when(F.col("POSITION_ALLOCATION_PCT") >= 5, F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    )
)

# COMMAND ----------

# Asset-class exposure provides a portfolio-level diversification view.
asset_class_exposure_df = (
    portfolio_position_df
    .groupBy(
        "PORTFOLIO_ID",
        "ASSET_CLASS"
    )
    .agg(
        F.sum("MARKET_VALUE").alias("ASSET_CLASS_MARKET_VALUE"),
        F.sum("COST_BASIS").alias("ASSET_CLASS_COST_BASIS"),
        F.sum("UNREALIZED_PNL").alias("ASSET_CLASS_UNREALIZED_PNL"),
        F.countDistinct("SECURITY_ID").alias("SECURITY_COUNT"),
        F.max("PORTFOLIO_TOTAL_MARKET_VALUE").alias("PORTFOLIO_AUM")
    )
    .withColumn(
        "ASSET_CLASS_EXPOSURE_PCT",
        F.when(
            F.col("PORTFOLIO_AUM") != 0,
            (F.col("ASSET_CLASS_MARKET_VALUE") / F.col("PORTFOLIO_AUM")) * 100
        )
    )
)

# COMMAND ----------

# Join the business-level asset class metrics back onto each position.
final_df = (
    portfolio_position_df.alias("p")
    .join(
        asset_class_exposure_df.alias("a"),
        on=["PORTFOLIO_ID", "ASSET_CLASS"],
        how="left"
    )
    .select(
        "p.HOLDING_ID",
        "p.PORTFOLIO_ID",
        "p.SECURITY_ID",
        "p.AS_OF_DATE",
        "p.SECURITY_CODE",
        "p.SECURITY_NAME",
        "p.SECURITY_TYPE",
        "p.ASSET_CLASS",
        "p.IS_ACTIVE",
        "p.QUANTITY",
        "p.UNIT_PRICE",
        "p.MARKET_VALUE",
        "p.COST_BASIS",
        "p.UNREALIZED_PNL",
        "p.UNREALIZED_PNL_PCT",
        "p.PORTFOLIO_TOTAL_MARKET_VALUE",
        "p.POSITION_ALLOCATION_PCT",
        "p.POSITION_RANK",
        "p.CONCENTRATION_BUCKET",
        "a.ASSET_CLASS_MARKET_VALUE",
        "a.ASSET_CLASS_COST_BASIS",
        "a.ASSET_CLASS_UNREALIZED_PNL",
        "a.ASSET_CLASS_EXPOSURE_PCT",
        "a.SECURITY_COUNT",
        "p.CURRENCY_CODE",
        "p.UPDATED_AT",
        "p.ingestion_timestamp",
        "p.source_system",
        "p.source_table"
    )
)

# COMMAND ----------

# Basic Gold validations.
assert final_df.select("PORTFOLIO_ID", "SECURITY_ID").distinct().count() == final_df.count(), \
    "Gold position grain must be unique by portfolio and security"

assert final_df.filter(F.col("SECURITY_ID").isNull()).count() == 0, \
    "Security enrichment failed for one or more positions"

# Asset-class exposure should reconcile to approximately 100% per portfolio.
exposure_check_df = (
    final_df
    .groupBy("PORTFOLIO_ID")
    .agg(F.sum("ASSET_CLASS_EXPOSURE_PCT").alias("TOTAL_EXPOSURE_PCT"))
)

assert exposure_check_df.filter(
    (F.col("TOTAL_EXPOSURE_PCT") < 99.99) | (F.col("TOTAL_EXPOSURE_PCT") > 100.01)
).count() == 0, \
    "Asset-class exposure does not reconcile to portfolio AUM"

# COMMAND ----------

# Persist the Gold business product.
(
    final_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.portfolio_position_exposure")
)

# COMMAND ----------

# Final inspection
spark.table("gold.portfolio_position_exposure") \
    .orderBy("PORTFOLIO_ID", "POSITION_RANK") \
    .show(50, truncate=False)
