# DQX Pipeline Demo

A end-to-end Databricks data quality demo using [Databricks Labs DQX](https://github.com/databrickslabs/dqx) with a medallion architecture (Landing → Bronze → Silver) and a quarantine pattern for invalid records.

## What it does

- Applies DQX row-level quality checks at ingestion time (landing → bronze)
- Routes invalid rows to a **quarantine table** with full lineage: which rule fired, which column, the original row payload, and which pipeline run produced it
- Promotes clean rows through to a **Silver SCD Type 2 table** via a Lakeflow Spark Declarative Pipeline using Change Data Feed
- Supports both **full** (overwrite) and **partial** (merge/upsert) load modes
- Replicates clean and quarantine data to a **DR workspace** via a GRS-replicated volume — the secondary region skips DQX and reads pre-validated parquet directly
- Ships an 8-step simulation scenario covering inserts, updates, deletes, and bad data of every kind

## Architecture

### Primary region

```
Landing Volume (Parquet)
        │
        ▼
  LandingZoneBronze.py
        │ DQX checks
        ├──────────────────────────────────────┐
        │ valid rows                           │ invalid rows
        ▼                                      ▼
  [quarantine rate check]        GRS Volume ─── quarantine/{batch_ts}/
  abort if rate > threshold              │ (GRS replication to DR)
        │                                      │
        ▼                                      ▼
  GRS Volume ─── clean/{batch_ts}/    Quarantine table (Delta)
        │
        ▼
  Bronze Delta table (CDF enabled)
        │
        ▼
  SilverTableDP.py (Lakeflow SDP pipeline)
        ├──────────────────────────────────────┐
        ▼                                      ▼
  Silver SCD Type 2                    operation_log
        │                                      │
        └──────────────┬───────────────────────┘
                       │
                       ▼
               SummaryMetrics.py
```

### DR (secondary) region

```
GRS Volume (replicated from primary)
        │
        ├── clean/{batch_ts}/           ── quarantine/{batch_ts}/
        │                                          │
        ▼                                          ▼
  LandingZoneBronzeDR.py ────────────────► Quarantine table (Delta, append)
        │  (skips DQX — data already validated)
        ▼
  Bronze Delta table (CDF enabled)
        │
        ▼
  SilverTableDP.py (same pipeline definition)
        ├──────────────────────────────────────┐
        ▼                                      ▼
  Silver SCD Type 2                    operation_log
        │                                      │
        └──────────────┬───────────────────────┘
                       │
                       ▼
               SummaryMetrics.py
```

## Repository structure

| File                     | Purpose                                                                                                |
| ------------------------ | ------------------------------------------------------------------------------------------------------ |
| `config.yaml`            | Catalog / schema / table / volume names                                                                |
| `_config.py`             | Loads config, shared Spark helpers (`tbl`, `vol_path`, `ensure_uc_resources`, `get_latest_folder`, `read_latest_parquet`, `write_to_bronze`) |
| `_shared.py`             | DQX rule definitions and `apply_dqx` / `write_quarantine` helpers (also writes quarantine parquet to GRS volume) |
| `WriteTestData.py`       | Generates synthetic test data for steps 1-8 and writes to a landing volume                             |
| `LandingZoneBronze.py`   | Reads landing parquet, runs DQX, writes valid rows to bronze and GRS volume, quarantines invalid rows  |
| `LandingZoneBronzeDR.py` | DR entry point — reads pre-validated clean and quarantine parquet from GRS volume, skips DQX           |
| `SilverTableDP.py`       | Lakeflow Spark Declarative Pipeline — SCD Type 2 from bronze CDF + operation_log per commit            |
| `SummaryMetrics.py`      | Reads bronze / silver / quarantine and prints a pipeline health scorecard                              |
| `databricks.yml`         | Databricks Asset Bundle — `dev` (primary) and `dr` targets, pipeline, and all jobs                    |

## DQX checks

Defined in `_shared.py`, applied to every landing batch:

| Rule            | Column | Condition                  |
| --------------- | ------ | -------------------------- |
| `id_not_null`   | `id`   | Must not be null           |
| `name_not_null` | `name` | Must not be null           |
| `age_not_null`  | `age`  | Must not be null           |
| `name_format`   | `name` | Must match `^[A-Za-z\s]+$` |

All rules have criticality `error` — invalid rows are routed to quarantine and never reach bronze.

## Quarantine table schema

| Column                 | Description                             |
| ---------------------- | --------------------------------------- |
| `rule_name`            | DQX rule that fired                     |
| `error_message`        | Human-readable error from DQX           |
| `faulty_column`        | Column that failed                      |
| `row_payload`          | Full original row as JSON               |
| `quarantine_timestamp` | When the row was quarantined            |
| `pipeline_name`        | Notebook name that produced the batch   |
| `pipeline_id`          | Databricks Job ID                       |
| `run_id`               | UUID unique per pipeline task execution |
| `load_type`            | `"full"` or `"partial"`                 |

## Jobs

Defined in `databricks.yml`. Primary jobs use `--profile primary`; DR jobs use `--target dr --profile dr`.

### Primary (`dev` target)

| Job                | Schedule intent | What it does                                                |
| ------------------ | --------------- | ----------------------------------------------------------- |
| `dqx-full-load`    | Weekly          | Full overwrite of bronze, then silver pipeline update       |
| `dqx-partial-load` | Every 6 hours   | Merge/upsert into bronze, then silver pipeline update       |
| `dqx-scenario-run` | Demo / ad-hoc   | Single parameterised run: write test data → bronze → silver |

### DR (`dr` target)

| Job                    | When to run                                         | What it does                                                                           |
| ---------------------- | --------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `dqx-dr-full-load`     | After primary full_load replicates GRS volume       | Reads clean + quarantine parquet from GRS volume, overwrites bronze, runs silver       |
| `dqx-dr-partial-load`  | After primary partial_load replicates GRS volume    | Reads clean + quarantine parquet from GRS volume, merges into bronze, runs silver      |
| `dqx-dr-partial-catchup` | Manual — catch up N missed partial batches        | 3 sequential bronze merges (each with quarantine), single silver run, then summary     |

Run a scenario step from the CLI:

```bash
# Primary — step 1 (full baseline)
databricks bundle run scenario_run --profile primary -p step=1 -p load_type=full

# Primary — step 2 (partial with bad data)
databricks bundle run scenario_run --profile primary -p step=2 -p load_type=partial

# DR — full load (specify the batch_ts written to GRS volume)
databricks bundle run dr_full_load --target dr --profile dr -p dr_batch_ts=<batch_ts>

# DR — partial load
databricks bundle run dr_partial_load --target dr --profile dr -p dr_batch_ts=<batch_ts>

# DR — catchup 3 missed partial batches in one run
databricks bundle run dr_partial_catchup --target dr --profile dr \
  -p batch_ts_1=<ts1> -p batch_ts_2=<ts2> -p batch_ts_3=<ts3>
```

## 8-step simulation scenario

| Step | Load type | Data description                                              | Expected quarantine                                       |
| ---- | --------- | ------------------------------------------------------------- | --------------------------------------------------------- |
| 1    | full      | 3 clean baseline rows (Roger, Alice, Bob)                     | none                                                      |
| 2    | partial   | 3 new clean rows + Oscar with null age                        | `age_not_null` (1 error)                                  |
| 3    | partial   | Valid updates + `B0b2.0` (bad name) + null name               | `name_format`, `name_not_null` (2 errors)                 |
| 4    | full      | Alice SCD2-closed, ages updated + Ghost with null id          | `id_not_null` (1 error)                                   |
| 5    | partial   | Eve updated + `J4ne!` (bad name) + Paul with null age         | `name_format`, `age_not_null` (2 errors)                  |
| 6    | partial   | Frank updated + Henry added + fully null row                  | `id_not_null`, `name_not_null`, `age_not_null` (3 errors) |
| 7    | full      | Full refresh: absent records SCD2-closed + Ghost with null id | `id_not_null` (1 error)                                   |
| 8    | partial   | Roger age updated (30 → 33)                                   | none                                                      |

Total: 10 quarantine errors across 6 steps, 4 distinct rules triggered.

## Getting started

### Prerequisites

- Databricks workspace with Unity Catalog
- Databricks CLI installed and configured
- A profile in `~/.databrickscfg` for your workspace

### Setup

1. Clone this repo:

   ```bash
   git clone <repo-url>
   cd dqx
   ```

2. Edit `config.yaml` to set your catalog and schema:

   ```yaml
   Catalog: your_catalog
   Schema: your_schema
   ```

3. Update the `_CONFIG_PATH` in `_config.py` and the `file_path` in `databricks.yml` to match your workspace user path.

4. Deploy the bundle:

   ```bash
   databricks bundle deploy --profile <your-profile>
   ```

5. Run a demo scenario step:

   ```bash
   databricks bundle run scenario_run --profile <your-profile> -p step=1 -p load_type=full
   ```

6. After the run completes, open and run `SummaryMetrics.py` in your workspace to see the health scorecard.

### Running individual jobs

```bash
# Full load (overwrite)
databricks bundle run full_load --profile <your-profile>

# Partial load (merge)
databricks bundle run partial_load --profile <your-profile>
```

## Key design decisions

**CDF-based silver pipeline** — Bronze has Change Data Feed enabled. The Lakeflow pipeline reads CDF and applies `APPLY CHANGES INTO` with SCD Type 2, so every update and delete is preserved as history in silver.

**Full vs partial load semantics** — A `full` load overwrites bronze entirely; rows absent from the new batch become CDF delete events, which close their SCD2 rows in silver. A `partial` load is a merge/upsert — no rows are deleted.

**GRS volume as replication boundary** — `LandingZoneBronze.py` writes two artifacts per batch to the GRS-replicated volume: clean parquet under `clean/{batch_ts}/` and quarantine parquet under `quarantine/{batch_ts}/`. For invalid rows, the GRS volume is the primary store — the quarantine parquet is written first, then the Delta quarantine table is appended from it. Azure GRS replicates the volume to the DR workspace. `LandingZoneBronzeDR.py` reads both and skips DQX entirely — validation already happened in primary.

**Quarantine parquet schema** — The quarantine file written to the GRS volume is the fully-processed (exploded) quarantine DataFrame, identical in schema to the quarantine Delta table. DR can append it directly with no DQX dependency.

**operation_log uses `@dp.table` (COMPLETE mode)** — The operation_log aggregates bronze CDF events by `(_commit_version, source_batch)`. Using `@dp.table` with a streaming source and stateful `groupBy` runs in COMPLETE mode: SDP recomputes the full aggregation from accumulated checkpoint state each run and rewrites the table. This produces correct per-batch counts whether silver processes one bronze commit or many (DR catchup), with no watermark needed.

**Job run `run_id`** — Each call to `write_quarantine()` captures the Databricks job run ID (`ctx.jobRunId()`). This ties every quarantine record back to the exact job run that produced it, enabling per-run lineage queries in the quarantine table.

**Checkpoint management** — If you drop and recreate the bronze table, you must run a full refresh on the silver pipeline to clear its CDF checkpoint before the next job run:

```bash
databricks bundle run silver_scd2 --full-refresh-all --profile <your-profile>
```
