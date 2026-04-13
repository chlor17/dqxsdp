# Databricks notebook source
# DBTITLE 1,Install DQX
# MAGIC %pip install databricks-labs-dqx==0.13.0 -q
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_shared

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.dropdown("load_type", "full", ["full", "partial"], "Load Type")

load_type = dbutils.widgets.get("load_type")

assert load_type in ("full", "partial"), f"Invalid load_type: '{load_type}'. Must be 'full' or 'partial'."

print(f"load_type={load_type}")

# COMMAND ----------

# DBTITLE 1,Read from landing volume
vol_name   = Vol_Full if load_type == "full" else Vol_Partial
ensure_uc_resources(vol_name)
landing_ts = get_latest_folder(vol_name)
landing_df = spark.read.parquet(f"{vol_path(vol_name)}/{landing_ts}")
print(f"Landing batch_ts: {landing_ts}")

# COMMAND ----------

# DBTITLE 1,DQX — split landing into valid / quarantine
valid_df, invalid_df, valid_count, quarantine_count = apply_dqx(landing_df)

# COMMAND ----------

# DBTITLE 1,Write invalid records to quarantine (Delta) and GRS volume (parquet)
quarantine_vol_path = f"{vol_path(Vol_Clean, 'quarantine')}/{landing_ts}"
write_quarantine(invalid_df, load_type=load_type, batch_ts=landing_ts, quarantine_vol_path=quarantine_vol_path, quarantine_count=quarantine_count)

# COMMAND ----------

# DBTITLE 1,Quarantine rate check — abort if threshold exceeded
total_count     = valid_count + quarantine_count
quarantine_rate = quarantine_count / total_count if total_count > 0 else 0.0

print(f"Quarantine rate: {quarantine_rate:.1%}  |  threshold: {Quarantine_threshold:.1%}")

if quarantine_rate > Quarantine_threshold:
    raise ValueError(
        f"Quarantine rate {quarantine_rate:.1%} ({quarantine_count}/{total_count} rows) "
        f"exceeds the {Quarantine_threshold:.1%} threshold — silver update skipped."
    )

# COMMAND ----------

# DBTITLE 1,Write clean records to clean volume
ensure_uc_resources(Vol_Clean)
clean_path = f"{vol_path(Vol_Clean, 'clean')}/{landing_ts}"
valid_df.coalesce(1).write.mode("overwrite").parquet(clean_path)
print(f"Clean data written to: {clean_path}")

# COMMAND ----------

# DBTITLE 1,Write clean data to bronze
bronze_path = write_to_bronze(valid_df, load_type)
display(spark.read.table(bronze_path))
