# Databricks notebook source
# MAGIC %run ./_config

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("step", "1", "Step (1-7)")
step = int(dbutils.widgets.get("step"))

assert 1 <= step <= 7, f"step must be between 1 and 7, got {step}"

# COMMAND ----------

# DBTITLE 1,Write test data for this step
from datetime import datetime

# step → (load_type, rows)
_STEPS = {
    # Step 1 — full baseline, all clean
    1: ("full",    [(1, "Roger",    30), (2, "Alice",   29), (3, "Bob",    35)]),

    # Step 2 — partial: new clean records + 1 null age → quarantine (age_not_null)
    2: ("partial", [(4, "Diana",    28), (5, "Eve",     32), (6, "Frank",  40),
                    (11, "Oscar",  None)]),                                                  # ✗ null age

    # Step 3 — partial: valid updates + 2 bad names → quarantine (name_format, name_not_null)
    3: ("partial", [(2, "Alice",    99), (9, "Grace",   27),
                    (12, "B0b2.0",  44),                                                     # ✗ name fails regex
                    (13, None,      31)]),                                                   # ✗ null name

    # Step 4 — full: delete Alice, Roger+Bob age update + 1 null id → quarantine (id_not_null)
    4: ("full",    [(1, "Roger",    31), (3, "Bob",     36),
                    (None, "Ghost", 50)]),                                                   # ✗ null id

    # Step 5 — partial: update Eve + 1 regex + 1 null age → quarantine (name_format, age_not_null)
    5: ("partial", [(5, "Eve",      33),
                    (14, "J4ne!",   27),                                                     # ✗ name fails regex
                    (15, "Paul",   None)]),                                                  # ✗ null age

    # Step 6 — partial: update Frank + new Henry + 1 fully null row → quarantine (id, name, age nulls)
    6: ("partial", [(6, "Frank",    41), (10, "Henry",  25),
                    (None, None,   None)]),                                                  # ✗ all nulls

    # Step 7 — full refresh: only clean records survive
    7: ("full",    [(1, "Roger",    31), (3, "Bob",     36), (4, "Diana",  28),
                    (5, "Eve",      33), (6, "Frank",   41), (9, "Grace",  27),
                    (10, "Henry",   25)]),
}

load_type, rows = _STEPS[step]
volume = Vol_Full if load_type == "full" else Vol_Partial

ensure_uc_resources(volume)

volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{volume}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
spark.createDataFrame(rows, ["id", "name", "age"]).write.mode("overwrite").parquet(volume_path)

print(f"Step {step} | load_type={load_type} | {len(rows)} rows written to: {volume_path}")
