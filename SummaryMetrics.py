# Databricks notebook source
# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

bronze_path     = f"{catalog}.{schema}.{Bronze_Table}"
quarantine_path = f"{catalog}.{schema}.{Quarantine_Table}"
silver_path     = f"{catalog}.{schema}.silver"

# COMMAND ----------

# DBTITLE 1,Bronze — current state
bronze_df = spark.read.table(bronze_path)
print(f"=== BRONZE — {bronze_df.count()} rows ===")
display(bronze_df.orderBy("id"))

# COMMAND ----------

# DBTITLE 1,Silver — SCD2 full history
silver_df = spark.read.table(silver_path)
active_df = silver_df.filter("__END_AT IS NULL")
closed_df = silver_df.filter("__END_AT IS NOT NULL")
print(f"=== SILVER — {active_df.count()} active | {closed_df.count()} closed ===")
display(silver_df.orderBy("id", "__START_AT"))

# COMMAND ----------

# DBTITLE 1,Quarantine — breakdown by rule
if spark.catalog.tableExists(quarantine_path):
    quarantine_df = spark.read.table(quarantine_path)
    print(f"=== QUARANTINE — {quarantine_df.count()} total errors ===")
    display(
        quarantine_df
        .groupBy("rule_name", "faulty_column")
        .agg(F.count("*").alias("error_count"))
        .orderBy(F.col("error_count").desc())
    )
else:
    quarantine_df = None
    print("=== QUARANTINE — no errors yet (table not created) ===")

# COMMAND ----------

# DBTITLE 1,Quarantine — breakdown by pipeline run
if quarantine_df is not None:
    display(
        quarantine_df
        .groupBy("pipeline_name", "run_id")
        .agg(
            F.count("*").alias("total_errors"),
            F.collect_set("rule_name").alias("rules_triggered"),
            F.min("quarantine_timestamp").alias("first_error_at"),
        )
        .orderBy("first_error_at")
    )

# COMMAND ----------

# DBTITLE 1,Silver — operation log (inserts / updates / deletes per run)
op_log_df = spark.read.table(f"{catalog}.{schema}.operation_log")
display(
    op_log_df
    .groupBy("source_batch")
    .pivot("operation", ["insert", "update_postimage", "delete"])
    .agg(F.count(F.lit(1)))
    .withColumnsRenamed({
        "insert":           "inserts",
        "update_postimage": "updates",
        "delete":           "deletes",
    })
    .orderBy("source_batch")
)

# COMMAND ----------

# DBTITLE 1,Pipeline health scorecard
q_total    = quarantine_df.count()                                if quarantine_df is not None else 0
q_distinct = quarantine_df.select("rule_name").distinct().count() if quarantine_df is not None else 0

display(spark.createDataFrame([
    ("Bronze active records",      bronze_df.count(), "Current live records in bronze"),
    ("Silver active records",      active_df.count(), "Current open SCD2 rows"),
    ("Silver closed (historical)", closed_df.count(), "SCD2 rows closed by updates/deletes"),
    ("Total quarantine errors",    q_total,           "Rows rejected by DQX across all runs"),
    ("Distinct rules triggered",   q_distinct,        "Unique DQX rules that fired"),
], ["metric", "value", "description"]))
