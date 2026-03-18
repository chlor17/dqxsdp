# Databricks notebook source
# DBTITLE 1,Shared DQX helpers — requires DQX installed (pip install in calling notebook)
# MAGIC %run ./_config

# COMMAND ----------

import uuid
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
    """Run DQX checks and split into valid / invalid DataFrames."""
    dq = DQEngine(WorkspaceClient(), spark=spark)
    valid_df, invalid_df = dq.apply_checks_and_split(landing_df, _CHECKS)
    print(f"Valid: {valid_df.count()} | Quarantined: {invalid_df.count()}")
    return valid_df, invalid_df


def write_quarantine(invalid_df, load_type="unknown"):
    """Explode DQX errors into one row per error and append to quarantine table."""
    if invalid_df.count() == 0:
        print("No quarantine records — all rows passed DQX checks.")
        return

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    def _get(opt):
        try:    return opt.get()
        except: return "N/A"

    run_id = str(uuid.uuid4())  # unique per write_quarantine() call (each task run)

    quarantine_df = (
        invalid_df
        .withColumn("row_payload", F.to_json(F.struct(*[F.col(c) for c in _ORIGINAL_COLS])))
        .withColumn("error", F.explode("_errors"))
        .select(
            F.col("error.name").alias("rule_name"),
            F.col("error.message").alias("error_message"),
            F.col("error.columns")[0].alias("faulty_column"),
            F.col("row_payload"),
            F.current_timestamp().alias("quarantine_timestamp"),
            F.lit(_get(ctx.notebookPath()).split("/")[-1]).alias("pipeline_name"),
            F.lit(_get(ctx.jobId())).alias("pipeline_id"),
            F.lit(run_id).alias("run_id"),  # unique per task (UUID generated once per call)
            F.lit(load_type).alias("load_type"),
        )
    )

    path = f"{catalog}.{schema}.{Quarantine_Table}"
    quarantine_df.write.format("delta").mode("append").saveAsTable(path)
    print(f"Quarantine written: {path}")
    display(spark.read.table(path))
