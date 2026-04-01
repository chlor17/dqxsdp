# Databricks notebook source
# MAGIC %run ./_config

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("step",      "1",    "Step (1-7)")
dbutils.widgets.dropdown("load_type", "full", ["full", "partial"], "Load Type")

step      = int(dbutils.widgets.get("step"))
load_type = dbutils.widgets.get("load_type")

assert 1 <= step <= 8,            f"step must be 1–8, got {step}"
assert load_type in ("full", "partial"), f"load_type must be full or partial, got {load_type}"

# COMMAND ----------

# DBTITLE 1,Write test data for this step
from datetime import datetime

# step → rows  (load_type is now a job parameter, not baked in here)
_STEPS = {
    # Step 1 — full baseline, all clean
    1: [(1, "Roger",    30), (2, "Alice",   29), (3, "Bob",    35)],

    # Step 2 — new clean records + 1 null age → quarantine (age_not_null)
    # 11 rows total: 1 bad → 9.1% quarantine rate (below 10% threshold)
    2: [(4, "Diana",    28), (5, "Eve",     32), (6, "Frank",  40),
        (11, "Oscar",  None),                                           # ✗ null age
        (16, "Liam",   25),  (17, "Emma",   30), (18, "Noah",  28),
        (19, "Olivia", 35),  (20, "William",42), (21, "Ava",   27),
        (22, "James",  33)],

    # Step 3 — valid updates + 2 bad names → quarantine (name_format, name_not_null)
    # 22 rows total: 2 bad → 9.1% quarantine rate (below 10% threshold)
    3: [(2, "Alice",    99), (9, "Grace",   27),
        (12, "B0b2.0",  44),                                            # ✗ name fails regex
        (13, None,      31),                                            # ✗ null name
        (23, "Sophia",  29), (24, "Ethan",  34), (25, "Mia",   26),
        (26, "Mason",   38), (27, "Charlotte",31),(28, "Logan", 45),
        (29, "Amelia",  27), (30, "Lucas",  39), (31, "Harper",22),
        (32, "Jackson", 41), (33, "Evelyn", 28), (34, "Sebastian",36),
        (35, "Abigail", 24), (36, "Mateo",  32), (37, "Emily", 29),
        (38, "Jack",    47), (39, "Ella",   31), (40, "Owen",  25)],

    # Step 4 — age updates + 1 null id → quarantine (id_not_null)
    # 11 rows total: 1 bad → 9.1% quarantine rate (below 10% threshold)
    4: [(1, "Roger",    31), (3, "Bob",     36),
        (None, "Ghost", 50),                                            # ✗ null id
        (41, "Theodore",28),(42, "Camila",  33), (43, "Aiden", 27),
        (44, "Luna",    35), (45, "Samuel", 40), (46, "Sofia", 29),
        (47, "Avery",   31), (48, "Joseph", 37)],

    # Step 5 — update Eve + 1 regex + 1 null age → quarantine (name_format, age_not_null)
    # 22 rows total: 2 bad → 9.1% quarantine rate (below 10% threshold)
    5: [(5, "Eve",      33),
        (14, "J4ne!",   27),                                            # ✗ name fails regex
        (15, "Paul",   None),                                           # ✗ null age
        (49, "David",   44), (50, "Lily",   26), (51, "Wyatt", 38),
        (52, "Eleanor", 31), (53, "Gabriel",27), (54, "Nora",  35),
        (55, "Carter",  29), (56, "Zoey",   42), (57, "Isaac", 33),
        (58, "Hannah",  28), (59, "Anthony",36), (60, "Addison",24),
        (61, "Lincoln", 41), (62, "Aurora", 30), (63, "Grayson",38),
        (64, "Natalie", 27), (65, "Eli",    34), (66, "Savannah",29),
        (67, "Ryan",    43)],

    # Step 6 — update Frank + new Henry + 1 fully null row → quarantine (id, name, age nulls)
    # 11 rows total: 1 bad → 9.1% quarantine rate (below 10% threshold)
    6: [(6, "Frank",    41), (10, "Henry",  25),
        (None, None,   None),                                           # ✗ all nulls
        (68, "Penelope",32), (69, "Levi",   28), (70, "Layla", 35),
        (71, "Julian",  40), (72, "Riley",  26), (73, "Elijah",33),
        (74, "Scarlett",29), (75, "Landon", 37)],

    # Step 7 — full refresh: surviving records + 1 null id → quarantine (id_not_null)
    # 11 rows total: 1 bad → 9.1% quarantine rate (below 10% threshold)
    # Records absent from bronze after overwrite get SCD2-closed (deletes)
    7: [(1, "Roger",    31), (3, "Bob",     36), (4, "Diana",  28),
        (5, "Eve",      33), (6, "Frank",   41), (9, "Grace",  27),
        (10, "Henry",   25),
        (None, "Ghost", 50),                                            # ✗ null id → quarantine
        (100, "Piper",  28), (101, "Quinn", 33), (102, "Reed", 41)],

    # Step 8 — partial update: Roger age 30 → 33
    8: [(1, "Roger",    33)],
}

rows   = _STEPS[step]
volume = Vol_Full if load_type == "full" else Vol_Partial

ensure_uc_resources(volume)

volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{volume}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
spark.createDataFrame(rows, ["id", "name", "age"]).write.mode("overwrite").parquet(volume_path)

print(f"Step {step} | load_type={load_type} | {len(rows)} rows written to: {volume_path}")
