# edetl-workshop

Hands-on Databricks data engineering workshop repo. Generates synthetic legal-domain JSON files, ingests them with Auto Loader through a Spark Declarative Pipeline (SDP), produces a star-schema gold layer, and ships the whole thing as a Databricks Asset Bundle (DAB) with GitHub Actions CI/CD.

The intended workflow:
- **Dev** — clone this repo into a Databricks Git folder, create your own pipeline directly from the SDP editor, iterate. No DAB.
- **Stg** — `databricks bundle deploy -t stg` ships the same code as a managed pipeline + scheduled job.

## What's in here

```
edetl-workshop/
├── databricks.yml                  # Bundle config (single target: stg)
├── resources/
│   ├── storage.yml                 # stg schema + raw_landing volume
│   ├── ingestion_pipeline.yml      # Serverless SDP pipeline resource
│   └── ingestion_job.yml           # Lakeflow Job that runs the pipeline
├── src/
│   ├── pipelines/
│   │   ├── bronze.py               # Auto Loader streaming tables (4 sources)
│   │   ├── silver.py               # Typed + expectations
│   │   ├── gold.py                 # Aggregates + star-schema join
│   │   └── _transforms.py          # Pure-Python helpers (tested)
│   ├── generator/
│   │   └── generate_files.py       # Synthetic JSON generator (with bad data)
│   ├── dashboards/
│   │   ├── edetl_overview.lvdash.json   # Importable AI/BI dashboard
│   │   └── SETUP.md                # How to import it
│   └── genie/
│       └── SETUP.md                # How to spin up a Genie Space
├── tests/
│   └── test_transforms.py          # pytest unit tests
└── .github/workflows/
    ├── pr-check.yml                # Validate + tests on PR
    └── deploy.yml                  # Deploy to stg on push to main
```

## Prerequisites

1. A Unity Catalog you can `USE_CATALOG`, `CREATE_SCHEMA`, and `CREATE_VOLUME` on
2. **Databricks CLI** v0.240+ — [install](https://docs.databricks.com/dev-tools/cli/install.html)
3. **Serverless** enabled on your workspace (terms accepted)
4. **Python 3.10+** locally for the generator and tests

```bash
pip install databricks-sdk faker pytest
databricks auth login --host https://<your-workspace>
```

## Workshop flow

### Block A — DE foundations (30 min, hands-on)

#### 1. Clone the repo into your workspace

In the Databricks UI:
- **Sidebar → Repos → Add repo** → URL: `https://github.com/fabienv-vibes/edetl-workshop`
- Now you have `/Repos/<you>/edetl-workshop/`.

#### 2. Generate raw files

Locally:
```bash
python src/generator/generate_files.py --catalog <CATALOG>
```

This creates a per-user schema `<CATALOG>.edetl_workshop_<your_short_username>`, a `raw_landing` volume inside it, and drops:
- `matters_master/all_matters.json` (200-row dimension; written once, skipped on re-runs)
- 5 fact files per source × 3 sources (matter_events, time_entries, doc_audit)
- ~5% of fact rows are deliberately malformed so silver expectations actually drop something

Re-run any time to drop a fresh batch. Auto Loader picks them up incrementally.

#### 3. Create your dev pipeline in the SDP editor

Still in the workspace UI:
- **Sidebar → Pipelines (Lakeflow) → Create pipeline**
- **Name:** `edetl-workshop-<your_username>`
- **Source code:**
  - Type: `Notebooks/SQL files`
  - Add each file: `/Repos/<you>/edetl-workshop/src/pipelines/bronze.py`, `silver.py`, `gold.py`
- **Compute:** Serverless
- **Catalog:** `<CATALOG>`
- **Target schema:** `edetl_workshop_<your_short_username>` (matches what the generator created)
- **Configuration:**
  - Key: `workshop.raw_path`
  - Value: `/Volumes/<CATALOG>/edetl_workshop_<your_short_username>/raw_landing`
- Click **Create** → **Run**

The pipeline runs serverless, ~3-5 min cold start.

#### 4. Re-run the generator, re-run the pipeline

Drop another batch of files — only the new ones are processed (Auto Loader checkpoint). Watch the SDP UI: bronze grows by exactly the new file count, silver applies expectations and drops the bad rows, gold materialized views recompute.

The DAG should look like a star: `silver_matters_master` + 3 silver fact aggregates feed `gold_matter_summary`.

### Block B — CI/CD working session (45 min)

The same code can be deployed as a managed pipeline via the DAB.

#### 1. Walk the DAB structure

```
databricks.yml          # one target (stg), required catalog variable
resources/storage.yml   # creates edetl_stg schema + raw_landing volume
resources/ingestion_pipeline.yml  # serverless SDP pipeline resource
resources/ingestion_job.yml       # Lakeflow Job, hourly (paused) schedule
```

The `mode: production` target makes the pipeline SP-owned, locks names, runs as the deployer.

#### 2. Validate, deploy, run

```bash
databricks bundle validate -t stg --var "catalog=<CATALOG>"
databricks bundle deploy   -t stg --var "catalog=<CATALOG>"

# Drop files into the new stg volume
python src/generator/generate_files.py --catalog <CATALOG> --schema edetl_stg

databricks bundle run ingestion_job -t stg --var "catalog=<CATALOG>"
```

After the run, `<CATALOG>.edetl_stg` has the same bronze/silver/gold tables, populated.

#### 3. Walk the GitHub Actions

- `.github/workflows/pr-check.yml` — on PR: pytest + `bundle validate -t stg`
- `.github/workflows/deploy.yml` — on push to main: `bundle deploy -t stg`, then optional `bundle run`

Repo secrets needed: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `WORKSHOP_CATALOG`.

#### 4. Live-stub your own `edetl` repo

Following the same skeleton, scaffold an `edetl` directory in your real repo: `databricks.yml`, a `resources/` folder, `src/pipelines/`. We do this together as a working session.

### Block C — Demo extras (10-15 min)

Once stg has data:

- **Dashboard:** import `src/dashboards/edetl_overview.lvdash.json` — see [src/dashboards/SETUP.md](src/dashboards/SETUP.md). KPIs, monthly revenue, practice-area breakdown, doc-action distribution, top-25 matters.
- **Genie Space:** point a Genie Space at the silver + gold tables — see [src/genie/SETUP.md](src/genie/SETUP.md). Sample questions like "Which 10 matters generated the most billable revenue?" and "What's the breakdown of revenue by practice area?" come pre-loaded.

## Pipeline data model

```
matters_master.json     → bronze_matters_master    → silver_matters_master   ─┐
matter_events JSON      → bronze_matter_events     → silver_matter_events    ─┤
time_entries JSON       → bronze_time_entries      → silver_time_entries     ─┼─→ gold_matter_summary
doc_audit JSON          → bronze_doc_audit         → silver_doc_audit        ─┘

                                                  → gold_matter_lifecycle
                                                  → gold_billable_hours_by_matter_month
                                                  → gold_doc_activity_by_matter
```

All silver tables apply `@dlt.expect_or_drop`. The generator's `--bad-data-pct` flag (default 5%) deliberately injects rows that fail those checks (negative hours, unknown event types, missing matter_id) so the SDP UI shows real expectation drops.

## Run tests

```bash
pytest tests/ -v
```

## Customizing for your workspace

In `databricks.yml`, replace the `host:` under `targets.stg.workspace` with your Azure / AWS Databricks workspace URL. Everything else lives behind the `catalog` variable.
