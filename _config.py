# Databricks notebook source
# DBTITLE 1,Config & non-DQX helpers (sourced via %run — no DQX dependency)
import os
import yaml
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# ── Config ────────────────────────────────────────────────────────────────────
_notebook_dir = "/".join(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    .notebookPath().get().split("/")[:-1]
)
_CONFIG_PATH = f"/Workspace{_notebook_dir}/config.yaml"

with open(_CONFIG_PATH) as f:
    _cfg = yaml.safe_load(f)

catalog               = _cfg["Catalog"]
schema                = _cfg["Schema"]
Bronze_Table          = _cfg["Bronze_table"]

# ── DR overrides — passed as notebook params in secondary-region jobs ─────────
# Primary runs don't pass these widgets; the try/except falls back silently.
try:
    _ov = dbutils.widgets.get("dr_catalog").strip()
    if _ov: catalog = _ov
except Exception:
    pass
try:
    _ov = dbutils.widgets.get("dr_schema").strip()
    if _ov: schema = _ov
except Exception:
    pass

# Switch active catalog so all SQL uses Unity Catalog (not spark_catalog / hive_metastore)
spark.sql(f"USE CATALOG `{catalog}`")
Quarantine_Table      = _cfg["Quarantine_table"]
Vol_Full              = _cfg["Vol_Full"]
Vol_Partial           = _cfg["Vol_Partial"]
Quarantine_threshold  = float(_cfg.get("Quarantine_threshold", 0.10))
Vol_Clean             = _cfg["Vol_Clean"]

# ── Helpers ───────────────────────────────────────────────────────────────────
def tbl(table_name):
    """Return a backtick-quoted three-part table reference (safe for hyphens in names)."""
    return f"`{catalog}`.`{schema}`.`{table_name}`"


def ensure_uc_resources(volume_name):
    """Idempotently create schema and volume (catalog must already exist)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{schema}`.`{volume_name}`")
    print("Unity Catalog resources verified.")


def vol_path(volume_name, subfolder=None):
    """Return the absolute Volume path, optionally appending a subfolder."""
    base = f"/Volumes/{catalog}/{schema}/{volume_name}"
    return f"{base}/{subfolder}" if subfolder else base


def get_latest_folder(volume_name, subfolder=None):
    """Return the folder name (not full path) of the most recent timestamp entry in a volume.

    Only considers entries whose names are pure digits (yyyyMMddHHmmss format),
    so non-timestamp directories (e.g. clean, quarantine) are ignored.
    Pass subfolder to search within a subdirectory (e.g. subfolder="clean").
    """
    return max(e for e in os.listdir(vol_path(volume_name, subfolder)) if e.isdigit())


def read_latest_parquet(volume_name, subfolder=None):
    """Return a DataFrame from the most recent parquet folder in a volume.

    Pass subfolder to search within a subdirectory (e.g. subfolder="clean").
    """
    latest = get_latest_folder(volume_name, subfolder)
    path   = f"{vol_path(volume_name, subfolder)}/{latest}"
    print(f"Reading from: {latest}")
    return spark.read.parquet(path)


def write_to_bronze(df, load_type, label=""):
    """Write df to bronze using full overwrite or partial merge.

    Returns the bronze table path. label is appended to print messages (e.g. " (DR)").
    """
    bronze_path = tbl(Bronze_Table)
    if load_type == "full":
        (df.write
            .format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("overwriteSchema", "true")
            .mode("overwrite")
            .saveAsTable(bronze_path)
        )
        print(f"Bronze overwritten{label}: {bronze_path}")
    else:
        (DeltaTable.forName(spark, bronze_path).alias("b")
            .merge(df.alias("p"), "b.id = p.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Bronze merged{label}: {bronze_path}")
    return bronze_path
