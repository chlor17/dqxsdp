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

volume = Vol_Full if load_type == "full" else Vol_Partial
print(f"load_type={load_type} | volume={volume}")

# COMMAND ----------

# DBTITLE 1,Create UC resources
ensure_uc_resources(volume)

# COMMAND ----------

# DBTITLE 1,DQX — split landing into valid / quarantine
landing_folder = get_latest_folder(volume)
landing_ts     = landing_folder.removeprefix(f"{volume}_")   # e.g. "20260331153318"
print(f"Landing folder: {landing_folder} | batch_ts: {landing_ts}")

landing_df                                    = read_latest_parquet(volume)
valid_df, invalid_df, valid_count, quarantine_count = apply_dqx(landing_df)

# COMMAND ----------

# DBTITLE 1,Write invalid records to quarantine
write_quarantine(invalid_df, load_type=load_type, batch_ts=landing_ts)

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

# DBTITLE 1,Write clean data to volume and bronze
ensure_uc_resources(Vol_Clean)

clean_path = f"/Volumes/{catalog}/{schema}/{Vol_Clean}/{load_type}_{landing_ts}"
valid_df.write.mode("overwrite").parquet(clean_path)
print(f"Clean data written: {clean_path}")

bronze_path = f"{catalog}.{schema}.{Bronze_Table}"
clean_df    = spark.read.parquet(clean_path).withColumn("batch_ts", F.to_timestamp(F.lit(landing_ts), "yyyyMMddHHmmss"))

if load_type == "full":
    # Full overwrite — absent rows become CDF deletes → silver closes them
    (clean_df.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .saveAsTable(bronze_path)
    )
    print(f"Bronze overwritten: {bronze_path}")
else:
    # Partial merge — upsert only, no deletes
    (DeltaTable.forName(spark, bronze_path).alias("b")
        .merge(clean_df.alias("p"), "b.id = p.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Bronze merged: {bronze_path}")

display(spark.read.table(bronze_path))
