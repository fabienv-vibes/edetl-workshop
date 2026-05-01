# edetl-workshop

Hands-on Databricks data engineering workshop repo. Generates synthetic legal-domain JSON files, ingests them with Auto Loader through a Spark Declarative Pipeline (SDP), produces a star-schema gold layer, and ships the whole thing as a Databricks Asset Bundle (DAB) with GitHub Actions CI/CD.

The intended workflow:
- **Dev** — clone this repo into a Databricks Git folder, run `setup_dev.py` once to provision pipeline + job + dashboard + Genie space, then iterate in the SDP editor.
- **Stg** — promote to a managed environment via DABs. The workshop demos authoring the bundle YAML from the workspace UI ("Edit as YAML"), then deploying via CLI or the workspace bundle editor.

## What's in here

```
edetl-workshop/
├── databricks.yml                  # Bundle config (single target: stg)
├── resources/
│   ├── storage.yml                 # stg schema + raw_landing volume
│   ├── ingestion_pipeline.yml      # Serverless SDP pipeline resource
│   └── ingestion_job.yml           # Lakeflow Job that runs the pipeline
├── src/
│   ├── setup_dev.py                # One-shot dev setup: schema + volume + files + pipeline + job + dashboard + genie
│   ├── pipelines/
│   │   ├── bronze.py               # Auto Loader streaming tables (4 sources)
│   │   ├── silver.py               # Typed + expectations
│   │   ├── gold.py                 # Aggregates + star-schema join
│   │   └── _transforms.py          # Pure-Python helpers (tested)
│   ├── generator/
│   │   └── generate_files.py       # Synthetic JSON generator (with bad data)
│   ├── dashboards/
│   │   ├── edetl_overview.lvdash.json   # AI/BI dashboard JSON (importable)
│   │   └── SETUP.md                # Manual import path (preferred: setup_dev.py)
│   └── genie/
│       ├── space_template.json     # Genie space serialized payload (placeholders for catalog/schema)
│       └── SETUP.md                # Manual setup path (preferred: setup_dev.py)
├── tests/
│   └── test_transforms.py          # pytest unit tests
└── .github/workflows/
    ├── pr-check.yml                # Validate + tests on PR
    └── deploy.yml                  # Deploy to stg on push to main
```

## Prerequisites

1. A Unity Catalog you can `USE_CATALOG`, `CREATE_SCHEMA`, and `CREATE_VOLUME` on
2. **Databricks CLI** v0.240+ — [install](https://docs.databricks.com/dev-tools/cli/install.html)
3. **Python 3.10+** on your laptop for the generator, setup script, and tests

```bash
pip install databricks-sdk faker pytest
databricks auth login --host https://<your-workspace>
```

`setup_dev.py` and `generate_files.py` run from **your local terminal** — they use the Databricks SDK with whatever profile `databricks auth login` set up. (They also work inside a Databricks notebook if you'd rather.)

## Workshop flow

### Block A — DE foundations (30 min, hands-on)

#### 1. Clone the repo into your workspace

In the Databricks UI:
- **Sidebar → Repos → Add repo** → URL: `https://github.com/fabienv-vibes/edetl-workshop`
- You'll land at `/Workspace/Repos/<your_email>/edetl-workshop/`.

#### 2. Run the dev setup script

```bash
python src/setup_dev.py --catalog <CATALOG>
```

That single command creates everything for dev:
- Per-user schema `<CATALOG>.edetl_workshop_<your_short_username>` and a `raw_landing` volume
- An initial batch of synthetic JSON files (~5 per fact source, ~5% deliberately malformed)
- A serverless SDP pipeline `edetl-workshop-<your_short_username>` pointing at the .py files in your Repos clone
- A Lakeflow Job `edetl-workshop-<your_short_username>` with one task that runs the pipeline (used for the "Edit as YAML" demo in Block B)
- An AI/BI dashboard with KPIs, monthly revenue, practice-area breakdown, top-25 matters table
- A Genie space with 8 sample questions, wired to the silver/gold tables

The script triggers an initial pipeline run and waits for it to finish (~3-5 min cold start), so the dashboard and Genie space have data to query when it returns. URLs for all three are printed at the end.

It's idempotent — re-run any time and it upgrades existing assets in place.

#### 3. Iterate in the SDP editor

Open the pipeline URL the script printed. The SDP editor lets you walk the bronze/silver/gold code, edit any of the .py files (they live in your Repos clone), and hit Run to see your changes. Auto Loader's checkpoint makes re-runs incremental.

#### 4. Drop more files, re-run

```bash
python src/generator/generate_files.py --catalog <CATALOG>
```

Then hit Run on the pipeline again. Bronze grows by exactly the new file count, silver applies expectations and drops the bad rows, gold materialized views recompute. The DAG looks like a star: `silver_matters_master` + 3 silver fact aggregates feed `gold_matter_summary`.

### Block B — CI/CD working session (45 min)

The same pipeline can be promoted to a managed environment via a **Databricks Asset Bundle (DAB)**. We let the workspace UI generate the bundle YAML from the dev job we already have, then use that as the source of truth for CI/CD.

#### 1. Generate bundle YAML from your dev job — the "Edit as YAML" demo (~5 min)

In the workspace UI:
- Open the job `setup_dev.py` created: **Jobs & Pipelines → `edetl-workshop-<short>`**
- Click the kebab menu (⋮) next to **Run now** → **Edit as YAML**
- Bundle-shaped YAML appears, ready to drop into a `databricks.yml` resources file

This is the GA "Collaborate on bundles in the workspace" feature. See [Migrate existing resources to a bundle](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/migrate-resources) for the canonical doc.

Compare the generated YAML against `resources/ingestion_job.yml` already in this repo — same shape. The bundle is the codified version of what you've been clicking on.

#### 2. Walk the bundle in the workspace bundle editor (~10 min)

- Create a Git folder pointing at this repo (Workspace → **Git folders → Add folder**)
- Open `databricks.yml` in the workspace bundle editor
- Walk the structure together:
  - `databricks.yml` — single target `stg`, required `catalog` variable
  - `resources/storage.yml` — schema + volume
  - `resources/ingestion_pipeline.yml` — serverless SDP pipeline
  - `resources/ingestion_job.yml` — Lakeflow Job (matches the YAML you exported in step 1)

The bundle editor lints, validates, and offers schema-aware IntelliSense — no need to memorise the YAML structure. The `mode: production` target makes the pipeline service-principal-owned, locks names, and runs as the deployer.

See [Collaborate on bundles in the workspace](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/workspace) for the full feature overview.

#### 3. Promote to stg (~10 min)

Two equivalent paths produce the same `[stg] edetl-edetl_stg` pipeline + hourly job. Pick one for the demo.

**(a) From the workspace bundle editor**: deploy button on the bundle, target `stg`.

**(b) From the CLI**:

```bash
databricks bundle validate -t stg --var "catalog=<CATALOG>"
databricks bundle deploy   -t stg --var "catalog=<CATALOG>"

# Drop files into the new stg volume
python src/generator/generate_files.py --catalog <CATALOG> --schema edetl_stg

databricks bundle run ingestion_job -t stg --var "catalog=<CATALOG>"
```

After the run, `<CATALOG>.edetl_stg` has the same bronze/silver/gold tables, populated.

#### 4. Walk the GitHub Actions (~10 min)

- `.github/workflows/pr-check.yml` — on PR: pytest + `bundle validate -t stg`
- `.github/workflows/deploy.yml` — on push to main: `bundle deploy -t stg`, then optional `bundle run`

Repo secrets needed: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `WORKSHOP_CATALOG`.

The full pattern: **workspace UI authors → Git is source of truth → Actions deploy to production**.

#### 5. Live-stub your own `edetl` repo (~10 min)

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
