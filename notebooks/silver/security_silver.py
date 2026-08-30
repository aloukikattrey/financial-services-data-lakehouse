# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Security Transformation
# MAGIC
# MAGIC **Purpose**
# MAGIC - Read the Bronze `security` dataset.
# MAGIC - Validate the security master grain and critical identifiers.
# MAGIC - Standardize security identifiers and descriptive fields.
# MAGIC - Convert the source `ACTIVE_FLAG` into a boolean `IS_ACTIVE`.
# MAGIC - Normalize granular security types into canonical asset classes.
# MAGIC - Classify identifier quality for downstream data-quality handling.
# MAGIC - Persist the result as a Delta Silver table.
# MAGIC
# MAGIC **Architecture**
# MAGIC
# MAGIC `Oracle → Foreign Catalog → Bronze Delta → PySpark transformations → Silver Delta`
# MAGIC
# MAGIC **Why this is a Silver transformation:** `SECURITY` is reference/master data. The key Silver responsibility is to create a consistent, conformed security master that downstream holdings, transactions, and portfolio analytics can use without repeatedly re-implementing source-specific classification logic.
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC %md
# MAGIC ## 1. Imports and source table
# MAGIC
# MAGIC PySpark is the primary transformation framework. Spark SQL is not required for the transformation itself; the DataFrame API is used throughout.
# MAGIC
# MAGIC # COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "`databricks-cata`.bronze.security"
TARGET_TABLE = "`databricks-cata`.silver.security"

security_df = spark.table(SOURCE_TABLE)

display(security_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Source baseline
# MAGIC
# MAGIC `SECURITY_ID` is the source technical identifier and `SECURITY_CODE` is a second business identifier. The first step is to establish the current source grain before transforming the master data.

# COMMAND ----------

print(f"Total security records: {security_df.count()}")

print(
    f"Distinct SECURITY_IDs: "
    f"{security_df.select('SECURITY_ID').distinct().count()}"
)

print(
    f"Distinct SECURITY_CODEs: "
    f"{security_df.select('SECURITY_CODE').distinct().count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect source classifications
# MAGIC
# MAGIC These distributions are used to understand the source semantics before defining canonical Silver mappings.

# COMMAND ----------

display(
    security_df
    .groupBy("SECURITY_TYPE")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    security_df
    .groupBy("ACTIVE_FLAG")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    security_df
    .groupBy("CURRENCY_CODE")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Critical identifier quality checks
# MAGIC
# MAGIC A security master depends on stable identifiers. Missing `ISIN` is classified separately because it may be optional for some instruments, while missing `SECURITY_ID` or `SECURITY_CODE` represents a stronger master-data issue.

# COMMAND ----------

identifier_quality_df = (
    security_df
    .select(
        F.count("*").alias("total_records"),
        F.sum(
            F.when(F.col("SECURITY_ID").isNull(), 1).otherwise(0)
        ).alias("missing_security_id"),
        F.sum(
            F.when(F.col("SECURITY_CODE").isNull(), 1).otherwise(0)
        ).alias("missing_security_code"),
        F.sum(
            F.when(F.col("ISIN").isNull(), 1).otherwise(0)
        ).alias("missing_isin"),
        F.sum(
            F.when(F.col("SECURITY_NAME").isNull(), 1).otherwise(0)
        ).alias("missing_security_name")
    )
)

display(identifier_quality_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Duplicate master-data checks
# MAGIC
# MAGIC `SECURITY_ID` is expected to identify one security record. `ISIN` is checked separately, but duplicates are not automatically removed because the source model may legitimately represent multiple records around the same external identifier.

# COMMAND ----------

duplicate_security_ids = (
    security_df
    .groupBy("SECURITY_ID")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_security_ids)

# COMMAND ----------

duplicate_isins = (
    security_df
    .filter(F.col("ISIN").isNotNull())
    .groupBy("ISIN")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_isins)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Standardize the security master
# MAGIC
# MAGIC Source identifiers and descriptive attributes are normalized once in Silver so downstream consumers can rely on consistent representations.
# MAGIC
# MAGIC The transformation also maps source-specific `SECURITY_TYPE` values into a canonical `ASSET_CLASS` used for portfolio exposure analysis.

# COMMAND ----------

security_silver_df = (
    security_df
    .withColumn(
        "SECURITY_ID",
        F.col("SECURITY_ID").cast("long")
    )
    .withColumn(
        "SECURITY_CODE",
        F.upper(F.trim(F.col("SECURITY_CODE")))
    )
    .withColumn(
        "ISIN",
        F.upper(F.trim(F.col("ISIN")))
    )
    .withColumn(
        "TICKER",
        F.upper(F.trim(F.col("TICKER")))
    )
    .withColumn(
        "SECURITY_NAME",
        F.trim(F.col("SECURITY_NAME"))
    )
    .withColumn(
        "SECURITY_TYPE",
        F.upper(F.trim(F.col("SECURITY_TYPE")))
    )
    .withColumn(
        "CURRENCY_CODE",
        F.upper(F.trim(F.col("CURRENCY_CODE")))
    )
    .withColumn(
        "EXCHANGE_CODE",
        F.upper(F.trim(F.col("EXCHANGE_CODE")))
    )
    .withColumn(
        "COUNTRY_CODE",
        F.upper(F.trim(F.col("COUNTRY_CODE")))
    )
    .withColumn(
        "IS_ACTIVE",
        F.when(
            F.upper(F.trim(F.col("ACTIVE_FLAG"))) == "Y",
            F.lit(True)
        )
        .when(
            F.upper(F.trim(F.col("ACTIVE_FLAG"))) == "N",
            F.lit(False)
        )
        .otherwise(F.lit(None).cast("boolean"))
    )
    .withColumn(
        "ASSET_CLASS",
        F.when(
            F.col("SECURITY_TYPE").isin(
                "EQUITY",
                "ETF",
                "EQUITY_FUND",
                "REIT",
                "PREFERRED"
            ),
            F.lit("EQUITY")
        )
        .when(
            F.col("SECURITY_TYPE").isin(
                "BOND_FUND",
                "GOVERNMENT_BOND",
                "CORPORATE_BOND",
                "MUNICIPAL_BOND",
                "CONVERTIBLE_BOND",
                "MBS"
            ),
            F.lit("FIXED_INCOME")
        )
        .when(
            F.col("SECURITY_TYPE") == "MONEY_MARKET",
            F.lit("CASH_EQUIVALENT")
        )
        .otherwise(F.lit("OTHER"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Identifier quality classification
# MAGIC
# MAGIC Silver does not silently discard records. Instead, it records the identifier-quality outcome so a later quarantine or remediation process can act on it explicitly.

# COMMAND ----------

security_silver_df = (
    security_silver_df
    .withColumn(
        "IDENTIFIER_STATUS",
        F.when(
            F.col("SECURITY_ID").isNull(),
            F.lit("INVALID")
        )
        .when(
            F.col("SECURITY_CODE").isNull()
            | (F.length(F.col("SECURITY_CODE")) == 0),
            F.lit("INVALID")
        )
        .when(
            F.col("ISIN").isNull()
            | (F.length(F.col("ISIN")) == 0),
            F.lit("MISSING_ISIN")
        )
        .otherwise(F.lit("VALID"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Select the Silver contract
# MAGIC
# MAGIC The Bronze lineage fields are preserved so downstream users can trace records back to the originating source.

# COMMAND ----------

security_silver_df = security_silver_df.select(
    "SECURITY_ID",
    "SECURITY_CODE",
    "ISIN",
    "TICKER",
    "SECURITY_NAME",
    "SECURITY_TYPE",
    "ASSET_CLASS",
    "CURRENCY_CODE",
    "EXCHANGE_CODE",
    "COUNTRY_CODE",
    "IS_ACTIVE",
    "IDENTIFIER_STATUS",
    "CREATED_AT",
    "UPDATED_AT",
    "ingestion_timestamp",
    "source_system",
    "source_table"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validate canonical asset-class mapping
# MAGIC
# MAGIC This verifies that every source security type has been mapped into a downstream-consumable canonical class.

# COMMAND ----------

display(
    security_silver_df
    .groupBy("SECURITY_TYPE", "ASSET_CLASS")
    .count()
    .orderBy("ASSET_CLASS", "SECURITY_TYPE")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate Silver quality attributes

# COMMAND ----------

display(
    security_silver_df
    .groupBy("IS_ACTIVE")
    .count()
)

# COMMAND ----------

display(
    security_silver_df
    .groupBy("IDENTIFIER_STATUS")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Review transformed records

# COMMAND ----------

display(
    security_silver_df
    .select(
        "SECURITY_ID",
        "SECURITY_CODE",
        "ISIN",
        "TICKER",
        "SECURITY_NAME",
        "SECURITY_TYPE",
        "ASSET_CLASS",
        "CURRENCY_CODE",
        "EXCHANGE_CODE",
        "COUNTRY_CODE",
        "IS_ACTIVE",
        "IDENTIFIER_STATUS"
    )
    .orderBy("SECURITY_ID")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Persist Silver Security as Delta
# MAGIC
# MAGIC This initial implementation creates the Silver table with an overwrite. Incremental master-data processing can later use a controlled Delta upsert strategy once the pipeline is operationalized.

# COMMAND ----------

(
    security_silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Final validation

# COMMAND ----------

silver_security_df = spark.table(TARGET_TABLE)

print(
    f"Bronze security records: {security_df.count()}"
)
print(
    f"Silver security records: {silver_security_df.count()}"
)

# COMMAND ----------

final_duplicate_ids = (
    silver_security_df
    .groupBy("SECURITY_ID")
    .count()
    .filter(F.col("count") > 1)
)

display(final_duplicate_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Outcome
# MAGIC
# MAGIC The Security Silver layer now provides a conformed security master with:
# MAGIC
# MAGIC - Standardized identifiers and descriptive attributes
# MAGIC - `ACTIVE_FLAG` converted from source `Y/N` semantics into boolean `IS_ACTIVE`
# MAGIC - Canonical `ASSET_CLASS` mapping from granular source security types
# MAGIC - Identifier-quality classification
# MAGIC - Source lineage columns
# MAGIC - Delta persistence and validation
# MAGIC
# MAGIC `ASSET_CLASS` will be reused later for portfolio exposure and concentration analytics rather than recreating instrument classification logic in every downstream transformation.
