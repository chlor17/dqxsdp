from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, col, date_format

catalog = spark.conf.get("Catalog")
schema = spark.conf.get("Schema")
bronze_table = spark.conf.get("Bronze_table")

source_path = f"`{catalog}`.`{schema}`.`{bronze_table}`"


@dp.view(name="bronze_full")
def bronze_full():
    """Read change data feed from bronze table."""
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(source_path)
    )


dp.create_streaming_table(
    name="silver", comment="Silver layer — SCD Type 2 from bronze CDF"
)

dp.create_auto_cdc_flow(
    flow_name="silver_full",
    target="silver",
    source="bronze_full",
    keys=["id"],
    sequence_by="batch_ts",
    stored_as_scd_type=2,
    except_column_list=["_change_type", "_commit_timestamp", "_commit_version", "batch_ts"],
    apply_as_deletes=expr(
        "_change_type = 'delete' or _change_type = 'update_preimage'"
    ),
    track_history_except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
        "batch_ts",
    ],
)


# ── Operation log ─────────────────────────────────────────────────────────────
# @dp.table with a streaming source and stateful groupBy runs in COMPLETE mode:
# SDP recomputes the full aggregation from accumulated checkpoint state each run,
# then rewrites the table. This naturally produces correct per-batch counts
# whether silver processes one bronze commit or many (e.g. DR catchup), with no
# watermark needed and no risk of count inflation across runs.


@dp.table(
    name="operation_log",
    comment="Per-batch count of inserts, updates, and deletes flowing into silver",
)
def log_silver_operations():
    """Aggregate bronze CDF events into per-batch operation counts.

    Groups by (_commit_version, source_batch) so each atomic bronze commit
    produces exactly one row. _commit_version is dropped from the output.
    """
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(source_path)
        .groupBy(
            col("_commit_version"),
            date_format(col("batch_ts"), "yyyyMMddHHmmss").alias("source_batch"),
        )
        .agg(
            F.sum(F.when(col("_change_type") == "insert",           1).otherwise(0)).alias("inserts"),
            F.sum(F.when(col("_change_type") == "update_postimage", 1).otherwise(0)).alias("updates"),
            F.sum(F.when(col("_change_type") == "delete",           1).otherwise(0)).alias("deletes"),
            F.max("_commit_timestamp").alias("logged_at"),
        )
        .drop("_commit_version")
    )
