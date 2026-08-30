# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Portfolio Transformation
# MAGIC
# MAGIC **Purpose**
# MAGIC - Read the Bronze `portfolio` dataset.
# MAGIC - Validate the portfolio business key and account relationship.
# MAGIC - Standardize identifiers and descriptive attributes.
# MAGIC - Enrich portfolios with `CLIENT_ID` through the Silver `account` relationship.
# MAGIC - Derive portfolio age and lifecycle attributes.
# MAGIC - Validate lifecycle timestamps and referential integrity.
# MAGIC - Persist the result as a Delta Silver table.
# MAGIC
# MAGIC **Business relationship**
# MAGIC
# MAGIC `CLIENT → ACCOUNT → PORTFOLIO`
# MAGIC
# MAGIC `PORTFOLIO` contains `ACCOUNT_ID`, so `CLIENT_ID` is conformed into Silver by resolving the account relationship.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports and source tables
# MAGIC
# MAGIC PySpark is used for the transformation logic. Spark SQL is used only for DDL where convenient.

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "`databricks-cata`.bronze.portfolio"
ACCOUNT_TABLE = "`databricks-cata`.silver.account"
TARGET_TABLE = "`databricks-cata`.silver.portfolio"

portfolio_df = spark.table(SOURCE_TABLE)
account_df = spark.table(ACCOUNT_TABLE)

display(portfolio_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source quality baseline
# MAGIC
# MAGIC Validate the source key before transformation. `PORTFOLIO_ID` is expected to identify one portfolio record.

# COMMAND ----------

portfolio_count = portfolio_df.count()
portfolio_id_count = portfolio_df.select("PORTFOLIO_ID").distinct().count()

print(f"Bronze portfolio rows: {portfolio_count}")
print(f"Distinct portfolio IDs: {portfolio_id_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check the ACCOUNT → PORTFOLIO relationship
# MAGIC
# MAGIC Every portfolio should resolve to a valid account. The account table also provides the owning `CLIENT_ID` used to conform the portfolio to the client hierarchy.

# COMMAND ----------

orphan_portfolios_df = (
    portfolio_df.alias("p")
    .join(
        account_df.select("ACCOUNT_ID", "CLIENT_ID").alias("a"),
        F.col("p.ACCOUNT_ID") == F.col("a.ACCOUNT_ID"),
        "left"
    )
    .filter(F.col("a.ACCOUNT_ID").isNull())
)

print(f"Orphan portfolios: {orphan_portfolios_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validate portfolio lifecycle timestamps
# MAGIC
# MAGIC The source does not contain an account-style `OPEN_DATE`/`CLOSE_DATE`. The available lifecycle timestamps are `INCEPTION_DATE`, `CREATED_AT`, and `UPDATED_AT`.
# MAGIC
# MAGIC We therefore check that required timestamps exist and that the chronology is sensible:
# MAGIC
# MAGIC `INCEPTION_DATE <= CREATED_AT <= UPDATED_AT`

# COMMAND ----------

missing_lifecycle_timestamps_df = (
    portfolio_df
    .filter(
        F.col("INCEPTION_DATE").isNull()
        | F.col("CREATED_AT").isNull()
        | F.col("UPDATED_AT").isNull()
    )
)

print(
    "Records with missing lifecycle timestamps: "
    f"{missing_lifecycle_timestamps_df.count()}"
)

# COMMAND ----------

invalid_timestamp_order_df = (
    portfolio_df
    .filter(
        (F.col("INCEPTION_DATE") > F.col("CREATED_AT"))
        | (F.col("CREATED_AT") > F.col("UPDATED_AT"))
    )
)

print(
    "Records with invalid timestamp ordering: "
    f"{invalid_timestamp_order_df.count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build the conformed Silver portfolio dataset
# MAGIC
# MAGIC Transformations performed:
# MAGIC - Cast Oracle numeric identifiers to `long`.
# MAGIC - Normalize portfolio code/name and categorical text.
# MAGIC - Enrich `CLIENT_ID` from the related Silver account record.
# MAGIC - Preserve source and ingestion metadata for lineage.

# COMMAND ----------

portfolio_silver_df = (
    portfolio_df.alias("p")
    .join(
        account_df.select(
            "ACCOUNT_ID",
            "CLIENT_ID"
        ).alias("a"),
        F.col("p.ACCOUNT_ID") == F.col("a.ACCOUNT_ID"),
        "left"
    )
    .select(
        F.col("p.PORTFOLIO_ID").cast("long").alias("PORTFOLIO_ID"),
        F.upper(F.trim(F.col("p.PORTFOLIO_CODE"))).alias("PORTFOLIO_CODE"),
        F.col("p.ACCOUNT_ID").cast("long").alias("ACCOUNT_ID"),
        F.col("a.CLIENT_ID").cast("long").alias("CLIENT_ID"),
        F.trim(F.col("p.PORTFOLIO_NAME")).alias("PORTFOLIO_NAME"),
        F.upper(F.trim(F.col("p.PORTFOLIO_TYPE"))).alias("PORTFOLIO_TYPE"),
        F.upper(F.trim(F.col("p.BASE_CURRENCY"))).alias("BASE_CURRENCY"),
        F.upper(F.trim(F.col("p.STATUS"))).alias("STATUS"),
        F.col("p.INCEPTION_DATE"),
        F.col("p.CREATED_AT"),
        F.col("p.UPDATED_AT"),
        F.col("p.ingestion_timestamp"),
        F.col("p.source_system"),
        F.col("p.source_table")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Derive portfolio age
# MAGIC
# MAGIC `PORTFOLIO_AGE_DAYS` measures elapsed time between the portfolio inception date and the current processing date.
# MAGIC
# MAGIC This is a derived business attribute and does not overwrite the source dates.

# COMMAND ----------

portfolio_silver_df = (
    portfolio_silver_df
    .withColumn(
        "PORTFOLIO_AGE_DAYS",
        F.datediff(
            F.current_date(),
            F.to_date("INCEPTION_DATE")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Derive portfolio lifecycle category
# MAGIC
# MAGIC Project-defined analytical classification:
# MAGIC - `< 365 days` → `NEW`
# MAGIC - `365–1824 days` → `ESTABLISHED`
# MAGIC - `>= 1825 days` → `MATURE`
# MAGIC
# MAGIC These thresholds are modeling choices for this project, not regulatory definitions.

# COMMAND ----------

portfolio_silver_df = (
    portfolio_silver_df
    .withColumn(
        "PORTFOLIO_LIFECYCLE",
        F.when(F.col("PORTFOLIO_AGE_DAYS") < 365, F.lit("NEW"))
         .when(F.col("PORTFOLIO_AGE_DAYS") < 1825, F.lit("ESTABLISHED"))
         .otherwise(F.lit("MATURE"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Review transformed portfolio records

# COMMAND ----------

display(
    portfolio_silver_df.select(
        "PORTFOLIO_ID",
        "PORTFOLIO_CODE",
        "ACCOUNT_ID",
        "CLIENT_ID",
        "PORTFOLIO_NAME",
        "PORTFOLIO_TYPE",
        "BASE_CURRENCY",
        "STATUS",
        "INCEPTION_DATE",
        "PORTFOLIO_AGE_DAYS",
        "PORTFOLIO_LIFECYCLE"
    ).orderBy("PORTFOLIO_ID")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate the Silver business key

# COMMAND ----------

duplicate_portfolios_df = (
    portfolio_silver_df
    .groupBy("PORTFOLIO_ID")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_portfolios_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate account/client enrichment

# COMMAND ----------

missing_client_df = portfolio_silver_df.filter(F.col("CLIENT_ID").isNull())

print(
    "Portfolios without resolved CLIENT_ID: "
    f"{missing_client_df.count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validate derived portfolio age

# COMMAND ----------

invalid_age_df = portfolio_silver_df.filter(
    F.col("PORTFOLIO_AGE_DAYS") < 0
)

print(
    "Portfolios with negative age: "
    f"{invalid_age_df.count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Persist Silver portfolio table
# MAGIC
# MAGIC The initial build uses overwrite to establish the managed Delta target. Production-style incremental dimension handling can later be implemented with Delta `MERGE` and SCD logic where required.

# COMMAND ----------

(
    portfolio_silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Final validation

# COMMAND ----------

silver_portfolio_df = spark.table(TARGET_TABLE)

print(f"Bronze portfolio rows: {portfolio_count}")
print(f"Silver portfolio rows: {silver_portfolio_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Outcome
# MAGIC
# MAGIC The Silver portfolio model now provides:
# MAGIC - Standardized identifiers and descriptive fields.
# MAGIC - `CLIENT_ID` conformed through the `ACCOUNT` relationship.
# MAGIC - Portfolio age as a derived business attribute.
# MAGIC - Portfolio lifecycle classification.
# MAGIC - Referential-integrity validation against Silver `ACCOUNT`.
# MAGIC - Lifecycle timestamp validation.
# MAGIC - Delta persistence with lineage metadata preserved.
