# Databricks notebook source
# DBTITLE 1,Shared DQX helpers — requires DQX installed (pip install in calling notebook)
# MAGIC %run ./_config

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx.check_funcs import is_not_null, regex_match

# ── DQX checks (defined once) ─────────────────────────────────────────────────
_ORIGINAL_COLS = ["id", "name", "age"]

_CHECKS = [
    DQRowRule(name="id_not_null",   criticality="error", column="id",   check_func=is_not_null),
    DQRowRule(name="name_not_null", criticality="error", column="name", check_func=is_not_null),
    DQRowRule(name="age_not_null",  criticality="error", column="age",  check_func=is_not_null),
    DQRowRule(name="name_format",   criticality="error", column="name", check_func=regex_match,
              check_func_kwargs={"regex": r"^[A-Za-z\s]+$"}),
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def apply_dqx(landing_df):
    """Run DQX checks and split into valid / invalid DataFrames.

    Logs a GE-style result block per check rule, then returns
    (valid_df, invalid_df, valid_count, quarantine_count).
    """
    dq = DQEngine(WorkspaceClient(), spark=spark)

    total_count = landing_df.count()

    # Split using the official API (handles null vs empty _errors correctly)
    valid_df, invalid_df = dq.apply_checks_and_split(landing_df, _CHECKS)
    valid_count      = valid_df.count()
    quarantine_count = total_count - valid_count

    # Per-check GE-style result log — derived from invalid_df which carries _errors
    for check in _CHECKS:
        col_name   = check.column
        check_name = check.name
        func_name  = check.check_func.__name__

        failed_df        = invalid_df.filter(F.expr(f"exists(_errors, e -> e.name = '{check_name}')"))
        unexpected_count = failed_df.count()
        pct              = round(unexpected_count / total_count * 100, 4) if total_count > 0 else 0.0

        bad_vals   = [r[col_name] for r in failed_df.select(col_name).limit(20).collect()]
        val_counts = (
            [{"value": str(r[col_name]), "count": r["count"]}
             for r in failed_df.groupBy(col_name).count()
                                .orderBy(F.col("count").desc()).limit(5).collect()]
            if unexpected_count > 0 else []
        )

        if func_name == "regex_match":
            regex     = (check.check_func_kwargs or {}).get("regex", "")
            idx_query = f"df.filter(~F.col('{col_name}').rlike(r'{regex}'))"
        else:
            idx_query = f"df.filter(F.col('{col_name}').isNull())"

        print(json.dumps({
            "check":  check_name,
            "column": col_name,
            "result": {
                "element_count":             total_count,
                "unexpected_count":          unexpected_count,
                "unexpected_percent":        pct,
                "partial_unexpected_list":   bad_vals[:5],
                "partial_unexpected_counts": val_counts,
                "unexpected_list":           bad_vals,
                "unexpected_index_query":    idx_query,
            },
        }, indent=2, default=str))

    print(f"Valid: {valid_count} | Quarantined: {quarantine_count}")
    return valid_df, invalid_df, valid_count, quarantine_count


def write_quarantine(invalid_df, load_type="unknown", batch_ts=None, quarantine_vol_path=None, quarantine_count=None):
    """Explode DQX errors into one row per error and write to quarantine.

    If quarantine_vol_path is provided: writes parquet to GRS volume first, then
    appends to the Delta quarantine table from that parquet (GRS is the primary store).
    Without a path: appends directly from memory.
    Pass quarantine_count to skip the guard .count() when the caller already has it.
    """
    _count = quarantine_count if quarantine_count is not None else invalid_df.count()
    if _count == 0:
        print("No quarantine records — all rows passed DQX checks.")
        return

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    def _get(opt):
        try:    return opt.get()
        except: return "N/A"

    run_id = str(_get(ctx.jobRunId()))

    quarantine_df = (
        invalid_df
        .withColumn("row_payload", F.to_json(F.struct(*[F.col(c) for c in _ORIGINAL_COLS])))
        .withColumn("error", F.explode("_errors"))
        .select(
            F.col("error.name").alias("rule_name"),
            F.col("error.message").alias("error_message"),
            F.col("error.columns")[0].alias("faulty_column"),
            F.col("row_payload"),
            (F.to_timestamp(F.lit(batch_ts), "yyyyMMddHHmmss") if batch_ts else F.current_timestamp()).alias("quarantine_timestamp"),
            F.lit(_get(ctx.notebookPath()).split("/")[-1]).alias("pipeline_name"),
            F.lit(_get(ctx.jobId())).alias("pipeline_id"),
            F.lit(run_id).alias("run_id"),
            F.lit(load_type).alias("load_type"),
        )
    )

    if quarantine_vol_path:
        quarantine_df.coalesce(1).write.mode("overwrite").parquet(quarantine_vol_path)
        print(f"Quarantine parquet written to GRS volume: {quarantine_vol_path}")
        source_df = spark.read.parquet(quarantine_vol_path)
    else:
        source_df = quarantine_df

    path = tbl(Quarantine_Table)
    source_df.write.format("delta").mode("append").saveAsTable(path)
    print(f"Quarantine written: {path}")

    display(spark.read.table(path))
