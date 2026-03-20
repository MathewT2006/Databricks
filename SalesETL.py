# Databricks notebook source
# MAGIC %md
# MAGIC # SalesETL — Employee CSV → Gold Table
# MAGIC **Source:** `/Volumes/salescatalog/default/myvolume/employees.csv`  
# MAGIC **Destination:** `salescatalog.gold.employees_from_csv`  
# MAGIC **Pipeline:** CSV (Volume) → Bronze (raw) → Silver (cleaned) → Gold (aggregated)

# COMMAND ----------

# MAGIC %md ## 0 · Configuration

# COMMAND ----------

CATALOG   = "salescatalog"
VOLUME_PATH = "/Volumes/salescatalog/default/myvolume"

BRONZE_TABLE = f"{CATALOG}.bronze.employees_csv_raw"
SILVER_TABLE = f"{CATALOG}.silver.employees_csv"
GOLD_TABLE   = f"{CATALOG}.gold.employees_from_csv"

print(f"Source : {VOLUME_PATH}")
print(f"Bronze : {BRONZE_TABLE}")
print(f"Silver : {SILVER_TABLE}")
print(f"Gold   : {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md ## 1 · Bronze — Load raw CSV from Volume

# COMMAND ----------

from pyspark.sql import functions as F

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")          # keep everything as string in bronze
    .option("mode", "PERMISSIVE")
    .csv(VOLUME_PATH)
)

df_raw = df_raw.withColumn("_source_file", F.lit(VOLUME_PATH)) \
               .withColumn("_loaded_at",   F.current_timestamp())

print(f"Rows loaded: {df_raw.count()}")
df_raw.printSchema()
display(df_raw)

# COMMAND ----------

# MAGIC %md ### Write Bronze table (overwrite)

# COMMAND ----------

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)
print(f"✓ Written to {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 2 · Silver — Clean & Enrich

# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)

df_silver = (
    df_bronze
    # Drop internal CSV rescue column if present
    .drop("_rescued_data")

    # Type casts
    .withColumn("salary",    F.col("salary").cast("decimal(12,2)"))
    .withColumn("hire_date", F.to_date("hire_date", "yyyy-MM-dd"))

    # Derived columns
    .withColumn("full_name",    F.concat_ws(" ", F.trim("first_name"), F.trim("last_name")))
    .withColumn("tenure_days",  F.datediff(F.current_date(), "hire_date"))
    .withColumn("tenure_years", F.round(F.col("tenure_days") / 365.25, 1))
    .withColumn("salary_band",
        F.when(F.col("salary") >= 130000, "Band 5 - Executive")
         .when(F.col("salary") >= 110000, "Band 4 - Senior Lead")
         .when(F.col("salary") >=  90000, "Band 3 - Lead")
         .when(F.col("salary") >=  70000, "Band 2 - Mid")
         .otherwise("Band 1 - Junior")
    )
    .withColumn("region",
        F.when(F.col("state").isin("CA", "OR", "WA"), "West")
         .when(F.col("state").isin("TX", "OK", "NM"), "South")
         .when(F.col("state").isin("NY", "MA", "PA", "NJ"), "Northeast")
         .when(F.col("state").isin("IL", "OH", "MI", "IN"), "Midwest")
         .otherwise("Other")
    )

    # Normalise strings
    .withColumn("email",      F.lower(F.trim("email")))
    .withColumn("status",     F.trim("status"))
    .withColumn("department", F.trim("department"))

    # Audit
    .withColumn("_updated_at", F.current_timestamp())

    # Drop bronze audit cols
    .drop("_source_file", "_loaded_at")
)

# Filter out completely empty rows
df_silver = df_silver.filter(F.col("employee_id").isNotNull())

print(f"Silver rows: {df_silver.count()}")
display(df_silver)

# COMMAND ----------

# MAGIC %md ### Write Silver table

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)
print(f"✓ Written to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md ## 3 · Gold — Aggregated Department & Region Metrics

# COMMAND ----------

df_gold = (
    spark.table(SILVER_TABLE)
    .filter(F.col("status") == "Active")
    .groupBy("department", "region")
    .agg(
        F.count("*")                          .alias("headcount"),
        F.round(F.avg("salary"), 2)           .alias("avg_salary"),
        F.min("salary")                       .alias("min_salary"),
        F.max("salary")                       .alias("max_salary"),
        F.round(F.avg("tenure_years"), 1)     .alias("avg_tenure_years"),
        F.countDistinct("city")               .alias("unique_cities"),
        F.collect_set("salary_band")          .alias("salary_bands_present"),
        F.current_timestamp()                 .alias("_updated_at")
    )
    .orderBy(F.desc("headcount"))
)

print(f"Gold rows: {df_gold.count()}")
display(df_gold)

# COMMAND ----------

# MAGIC %md ### Write Gold table

# COMMAND ----------

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)
print(f"✓ Written to {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md ## 4 · Gold — Salary Band Distribution

# COMMAND ----------

df_gold_bands = (
    spark.table(SILVER_TABLE)
    .filter(F.col("status") == "Active")
    .groupBy("salary_band", "department")
    .agg(
        F.count("*")                      .alias("employee_count"),
        F.round(F.avg("salary"), 2)       .alias("avg_salary"),
        F.round(F.avg("tenure_years"), 1) .alias("avg_tenure_years"),
        F.current_timestamp()             .alias("_updated_at")
    )
    .orderBy(F.desc("salary_band"), "department")
)

GOLD_BANDS_TABLE = f"{CATALOG}.gold.employees_salary_bands_csv"
(
    df_gold_bands.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_BANDS_TABLE)
)
print(f"✓ Written to {GOLD_BANDS_TABLE}")
display(df_gold_bands)

# COMMAND ----------

# MAGIC %md ## 5 · Validation Summary

# COMMAND ----------

print("=" * 55)
print("  ETL Pipeline Complete — Table Row Counts")
print("=" * 55)
for tbl in [BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE, GOLD_BANDS_TABLE]:
    cnt = spark.table(tbl).count()
    print(f"  {tbl.split('.')[-1]:35s}  {cnt:>5} rows")
print("=" * 55)
