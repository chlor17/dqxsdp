from pyspark import pipelines as dp
from pyspark.sql.functions import expr, col, current_timestamp, date_format

catalog = spark.conf.get("Catalog")
schema = spark.conf.get("Schema")
bronze_table = spark.conf.get("Bronze_table")

source_path = f"{catalog}.{schema}.{bronze_table}"


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
dp.create_streaming_table(
    name="operation_log",
    comment="Row-level log of every CDF event (insert / update / delete) flowing into silver",
)


@dp.append_flow(target="operation_log")
def log_silver_operations():
    """Capture every bronze CDF event as it flows into silver."""
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(source_path)
        .select(
            col("id"),
            col("_change_type").alias("operation"),
            date_format(col("batch_ts"), "yyyyMMddHHmmss").alias("source_batch"),
            current_timestamp().alias("logged_at"),
        )
    )
