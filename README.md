# edetl-workshop

Hands-on Databricks data engineering workshop repo. Generates synthetic legal-domain JSON files, ingests them with Auto Loader through a Spark Declarative Pipeline (SDP), and ships everything as a Databricks Asset Bundle (DAB) with two targets and GitHub Actions CI/CD.

This is the canonical reference structure your internal `edetl` repo can mirror.

## What's in here

```
edetl-workshop/
├── databricks.yml                  # Bundle config + dev/stg targets
├── resources/
│   ├── ingestion_pipeline.yml      # Serverless SDP pipeline resource
│   └── ingestion_job.yml           # Lakeflow Job that runs the pipeline
├── src/
│   ├── pipelines/
│   │   ├── bronze.py               # Auto Loader streaming tables
│   │   ├── silver.py               # Typed + expectations
│   │   ├── gold.py                 # Aggregates (materialized views)
│   │   └── _transforms.py          # Pure-Python helpers (tested)
│   └── generator/
│       └── generate_files.py       # Synthetic JSON generator
├── tests/
│   └── test_transforms.py          # pytest unit tests
└── .github/workflows/
    ├── pr-check.yml                # Validate + tests on PR
    └── deploy.yml                  # Deploy to stg on push to main
```

## Prerequisites

1. **Unity Catalog** with a catalog you can `USE_CATALOG`, `CREATE_SCHEMA`, and `CREATE_VOLUME` on.
2. **Databricks CLI** v0.240+ ([install](https://docs.databricks.com/dev-tools/cli/install.html)).
3. **Serverless** enabled on your workspace (terms accepted).
4. **Python 3.10+** locally for running the generator and tests.

## Setup

```bash
# 1. Authenticate the CLI
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# 2. Install generator + test dependencies locally
pip install databricks-sdk faker pytest
```

## Run the workshop end-to-end

Replace `<CATALOG>` with your UC catalog. The generator and the DAB both create per-user schemas in dev so attendees don't collide.

### 1. Generate synthetic files

```bash
python src/generator/generate_files.py --catalog <CATALOG>
```

This creates:
- Schema `<CATALOG>.edetl_workshop_<your_short_username>`
- Volume `<CATALOG>.edetl_workshop_<your_short_username>.raw_landing`
- ~5 JSON files per source under `/Volumes/<CATALOG>/edetl_workshop_<you>/raw_landing/{matters,time_entries,doc_audit}/`

Re-run any time to drop a fresh batch — Auto Loader picks them up incrementally.

### 2. Validate and deploy the bundle

```bash
databricks bundle validate -t dev --var "catalog=<CATALOG>"
databricks bundle deploy   -t dev --var "catalog=<CATALOG>"
```

### 3. Run the pipeline

```bash
databricks bundle run ingestion_job -t dev --var "catalog=<CATALOG>"
```

Open the SDP UI to watch bronze → silver → gold tables populate. Re-run the generator, then re-run the job: only new files are processed.

### 4. Promote to stg (optional)

```bash
databricks bundle deploy -t stg --var "catalog=<CATALOG>"
databricks bundle run    ingestion_job -t stg --var "catalog=<CATALOG>"
```

## Targets

| Target | Mode | Schema | When to use |
|---|---|---|---|
| `dev` | development | `edetl_workshop_<user>` | Local iteration. Schedules paused, names dev-prefixed, runs as you. |
| `stg` | production | `edetl_stg` | CI/CD-driven. Schedule active, runs as service principal, locked names. |

## CI/CD

GitHub Actions workflows expect these repo secrets:

| Secret | Purpose |
|---|---|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_CLIENT_ID` | Service principal application ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal secret |
| `WORKSHOP_CATALOG` | UC catalog name (passed via `--var`) |

- **PR to main** → `pr-check.yml` runs unit tests + `bundle validate -t dev`
- **Push to main** → `deploy.yml` runs `bundle deploy -t stg` then triggers the job

## Run tests locally

```bash
pytest tests/ -v
```

## Pipeline data model

```
raw JSON files (Volume)
  └── matters/          → bronze_matter_events     → silver_matter_events     → gold_matter_lifecycle
  └── time_entries/     → bronze_time_entries      → silver_time_entries      → gold_billable_hours_by_matter_month
  └── doc_audit/        → bronze_doc_audit         → silver_doc_audit         → gold_doc_activity_by_matter
```

All silver tables apply `@dlt.expect_or_drop` — rows that fail data quality drop, the rest flow downstream.

## Customizing for your workspace

In `databricks.yml`, change the `host:` under `targets.dev.workspace` and `targets.stg.workspace` to your Azure / AWS Databricks workspace URL. Everything else lives behind the `catalog` variable.
