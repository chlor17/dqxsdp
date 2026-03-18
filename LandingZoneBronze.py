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
landing_df           = read_latest_parquet(volume)
valid_df, invalid_df = apply_dqx(landing_df)

# COMMAND ----------

# DBTITLE 1,Write to bronze
from delta.tables import DeltaTable

bronze_path = f"{catalog}.{schema}.{Bronze_Table}"

if load_type == "full":
    # Full overwrite — absent rows become CDF deletes → silver closes them
    (valid_df.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .saveAsTable(bronze_path)
    )
    print(f"Bronze overwritten: {bronze_path}")
else:
    # Partial merge — upsert only, no deletes
    (DeltaTable.forName(spark, bronze_path).alias("b")
        .merge(valid_df.alias("p"), "b.id = p.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Bronze merged: {bronze_path}")

display(spark.read.table(bronze_path))

# COMMAND ----------

# DBTITLE 1,Write invalid records to quarantine
write_quarantine(invalid_df, load_type=load_type)
