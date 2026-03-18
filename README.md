# DQX Pipeline Demo

A end-to-end Databricks data quality demo using [Databricks Labs DQX](https://github.com/databrickslabs/dqx) with a medallion architecture (Landing → Bronze → Silver) and a quarantine pattern for invalid records.

## What it does

- Applies DQX row-level quality checks at ingestion time (landing → bronze)
- Routes invalid rows to a **quarantine table** with full lineage: which rule fired, which column, the original row payload, and which pipeline run produced it
- Promotes clean rows through to a **Silver SCD Type 2 table** via a Lakeflow Spark Declarative Pipeline using Change Data Feed
- Supports both **full** (overwrite) and **partial** (merge/upsert) load modes
- Ships a 7-step simulation scenario covering inserts, updates, deletes, and bad data of every kind

## Architecture

```
Landing Volume (Parquet)
        │
        ▼
  LandingZoneBronze.py  ──── DQX checks ────┐
        │                                    │
        │ valid rows                         │ invalid rows
        ▼                                    ▼
Bronze Delta table               Quarantine table  (dead end)
  (CDF enabled)
        │
        ▼
SilverTableDP.py (Lakeflow SDP pipeline)
        │
        ▼
  Silver SCD Type 2
```

## Repository structure

| File | Purpose |
|---|---|
| `config.yaml` | Catalog / schema / table / volume names |
| `_config.py` | Loads config, shared Spark helpers (`ensure_uc_resources`, `read_latest_parquet`) |
| `_shared.py` | DQX rule definitions and `apply_dqx` / `write_quarantine` helpers |
| `WriteTestData.py` | Generates synthetic test data for steps 1-7 and writes to a landing volume |
| `LandingZoneBronze.py` | Reads landing parquet, runs DQX, writes valid rows to bronze, quarantines invalid rows |
| `SilverTableDP.py` | Lakeflow Spark Declarative Pipeline — SCD Type 2 from bronze CDF |
| `SummaryMetrics.py` | Reads bronze / silver / quarantine and prints a pipeline health scorecard |
| `databricks.yml` | Databricks Asset Bundle — defines the pipeline and three jobs |

## DQX checks

Defined in `_shared.py`, applied to every landing batch:

| Rule | Column | Condition |
|---|---|---|
| `id_not_null` | `id` | Must not be null |
| `name_not_null` | `name` | Must not be null |
| `age_not_null` | `age` | Must not be null |
| `name_format` | `name` | Must match `^[A-Za-z\s]+$` |

All rules have criticality `error` — invalid rows are routed to quarantine and never reach bronze.

## Quarantine table schema

| Column | Description |
|---|---|
| `rule_name` | DQX rule that fired |
| `error_message` | Human-readable error from DQX |
| `faulty_column` | Column that failed |
| `row_payload` | Full original row as JSON |
| `quarantine_timestamp` | When the row was quarantined |
| `pipeline_name` | Notebook name that produced the batch |
| `pipeline_id` | Databricks Job ID |
| `run_id` | UUID unique per pipeline task execution |
| `load_type` | `"full"` or `"partial"` |

## Jobs

Defined in `databricks.yml`:

| Job | Schedule intent | What it does |
|---|---|---|
| `dqx-full-load` | Weekly | Full overwrite of bronze, then silver pipeline update |
| `dqx-partial-load` | Every 6 hours | Merge/upsert into bronze, then silver pipeline update |
| `dqx-full-scenario` | Demo / ad-hoc | Runs all 7 simulation steps end-to-end |

## 7-step simulation scenario

| Step | Load type | Data description | Expected quarantine |
|---|---|---|---|
| 1 | full | 3 clean baseline rows (Roger, Alice, Bob) | none |
| 2 | partial | 3 new clean rows + Oscar with null age | `age_not_null` (1 error) |
| 3 | partial | Valid updates + `B0b2.0` (bad name) + null name | `name_format`, `name_not_null` (2 errors) |
| 4 | full | Alice deleted, ages updated + Ghost with null id | `id_not_null` (1 error) |
| 5 | partial | Eve updated + `J4ne!` (bad name) + Paul with null age | `name_format`, `age_not_null` (2 errors) |
| 6 | partial | Frank updated + Henry added + fully null row | `id_not_null`, `name_not_null`, `age_not_null` (3 errors) |
| 7 | full | Clean full refresh — only valid records survive | none |

Total: 10 quarantine errors across 5 steps, 4 distinct rules triggered.

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
   Schema:  your_schema
   ```

3. Update the `_CONFIG_PATH` in `_config.py` and the `file_path` in `databricks.yml` to match your workspace user path.

4. Deploy the bundle:
   ```bash
   databricks bundle deploy --profile <your-profile>
   ```

5. Run the full demo scenario:
   ```bash
   databricks bundle run full_scenario --profile <your-profile>
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

**UUID `run_id`** — Each call to `write_quarantine()` generates a fresh UUID. This gives every task execution in a multi-task job a distinct identifier, even on Databricks Serverless where JVM-level run IDs are unavailable.

**Checkpoint management** — If you drop and recreate the bronze table, you must run a full refresh on the silver pipeline to clear its CDF checkpoint before the next job run:
```bash
databricks pipelines start-update <pipeline-id> --full-refresh --profile <your-profile>
```
