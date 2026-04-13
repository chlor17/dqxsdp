# Databricks notebook source
# DBTITLE 1,DR entry point — reads from replicated grsvolume, skips DQX
# MAGIC %run ./_config

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.dropdown("load_type", "full", ["full", "partial"], "Load Type")
dbutils.widgets.text("dr_catalog",   "", "DR catalog override (leave blank to use config.yaml)")
dbutils.widgets.text("dr_schema",    "", "DR schema override (leave blank to use config.yaml)")
dbutils.widgets.text("dr_vol_clean", "", "DR clean volume override (leave blank to use config.yaml)")
dbutils.widgets.text("dr_batch_ts",  "", "Specific batch folder to read (leave blank for latest)")

load_type = dbutils.widgets.get("load_type")
assert load_type in ("full", "partial"), f"Invalid load_type: '{load_type}'. Must be 'full' or 'partial'."

_dr_schema    = dbutils.widgets.get("dr_schema").strip()
_dr_vol_clean = dbutils.widgets.get("dr_vol_clean").strip()
if _dr_schema:    schema    = _dr_schema
if _dr_vol_clean: Vol_Clean = _dr_vol_clean

print(f"load_type={load_type} | schema={schema} | Vol_Clean={Vol_Clean}")

# COMMAND ----------

# DBTITLE 1,Read from replicated clean volume (already DQX-validated in primary)
ensure_uc_resources(Vol_Clean)
_dr_batch_ts = dbutils.widgets.get("dr_batch_ts").strip()
if _dr_batch_ts:
    batch_ts = _dr_batch_ts
else:
    batch_ts = get_latest_folder(Vol_Clean, subfolder="clean")
clean_df = spark.read.parquet(f"{vol_path(Vol_Clean, 'clean')}/{batch_ts}")
print(f"Reading from specific batch: {batch_ts}")
print(f"DR source | batch_ts={batch_ts} | rows={clean_df.count()}")

# COMMAND ----------

# DBTITLE 1,Write clean data to bronze
bronze_path = write_to_bronze(clean_df, load_type, label=" (DR)")
display(spark.read.table(bronze_path))

# COMMAND ----------

# DBTITLE 1,Append quarantine records from GRS volume to quarantine table
q_vol_path = f"{vol_path(Vol_Clean, 'quarantine')}/{batch_ts}"
if os.path.exists(q_vol_path):
    q_df = spark.read.parquet(q_vol_path)
    quarantine_path = tbl(Quarantine_Table)
    q_df.write.format("delta").mode("append").saveAsTable(quarantine_path)
    print(f"Quarantine appended (DR): {quarantine_path} | rows={q_df.count()}")
    display(spark.read.table(quarantine_path))
else:
    print(f"No quarantine records for batch {batch_ts} — skipping.")
