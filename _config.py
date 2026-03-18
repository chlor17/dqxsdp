# Databricks notebook source
# DBTITLE 1,Config & non-DQX helpers (sourced via %run — no DQX dependency)
import os
import yaml
from pyspark.sql import functions as F

# ── Config ────────────────────────────────────────────────────────────────────
_notebook_dir = "/".join(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    .notebookPath().get().split("/")[:-1]
)
_CONFIG_PATH = f"/Workspace{_notebook_dir}/config.yaml"

with open(_CONFIG_PATH) as f:
    _cfg = yaml.safe_load(f)

catalog          = _cfg["Catalog"]
schema           = _cfg["Schema"]
Bronze_Table     = _cfg["Bronze_table"]
Quarantine_Table = _cfg["Quarantine_table"]
Vol_Full         = _cfg["Vol_Full"]
Vol_Partial      = _cfg["Vol_Partial"]

# ── Helpers ───────────────────────────────────────────────────────────────────
def ensure_uc_resources(volume_name):
    """Idempotently create catalog, schema, and volume."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME  IF NOT EXISTS {catalog}.{schema}.{volume_name}")
    print("Unity Catalog resources verified.")


def read_latest_parquet(volume_name):
    """Return a DataFrame from the most recent parquet folder in a volume."""
    volume_dir = f"/Volumes/{catalog}/{schema}/{volume_name}"
    latest     = max(os.listdir(volume_dir))
    path       = os.path.join(volume_dir, latest)
    print(f"Reading from: {latest}")
    return spark.read.parquet(path)
