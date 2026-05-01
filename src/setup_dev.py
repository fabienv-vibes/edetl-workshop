"""One-shot dev environment setup for the edetl-workshop.

Creates everything an attendee needs to start iterating in their workspace:
  - schema `<catalog>.edetl_workshop_<your_short_username>`
  - volume `raw_landing` inside it
  - initial batch of synthetic JSON files (with bad data)
  - serverless SDP pipeline pointing at the Repos clone of this repo
  - Lakeflow Job that runs the pipeline (used for the "Edit as YAML" demo in Block B)
  - AI/BI dashboard wired to the gold tables
  - Genie space wired to the silver/gold tables

Re-running upgrades existing assets in place (idempotent by name).

Prerequisite: clone this repo into Workspace -> Repos first. Setup needs the
.py files at `/Workspace/Repos/<your_email>/edetl-workshop/src/pipelines/`.

Usage:
    python src/setup_dev.py --catalog <UC_CATALOG>
    python src/setup_dev.py --catalog <UC_CATALOG> --skip-pipeline-run

Requires: pip install databricks-sdk faker
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.dashboards import Dashboard, LifecycleState
from databricks.sdk.service.jobs import CronSchedule, JobSettings, PauseStatus, PipelineTask, Task
from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary
from faker import Faker

# Reuse the generator's helpers — same module, same module-level constants.
sys.path.insert(0, str(Path(__file__).parent / "generator"))
from generate_files import (  # noqa: E402
    GeneratorConfig,
    SCHEMA_PREFIX,
    VOLUME_NAME,
    build_id_pools,
    ensure_schema,
    ensure_volume,
    resolve_user_short_name,
    write_fact_batch,
    write_matters_master_once,
)


REPO_NAME = "edetl-workshop"
PIPELINE_FILES = ["bronze.py", "silver.py", "gold.py"]
DASHBOARD_TEMPLATE = Path(__file__).parent / "dashboards" / "edetl_overview.lvdash.json"
GENIE_TEMPLATE = Path(__file__).parent / "genie" / "space_template.json"
GENIE_CATALOG_TOKEN = "__CATALOG__"
GENIE_SCHEMA_TOKEN = "__SCHEMA__"


def pick_warehouse_id(w: WorkspaceClient) -> str:
    """Prefer a running warehouse; fall back to Serverless Starter; else first listed."""
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise SystemExit("No SQL warehouses available in this workspace")

    def sort_key(wh):
        running = 0 if (wh.state and wh.state.value == "RUNNING") else 1
        starter = 0 if (wh.name == "Serverless Starter Warehouse") else 1
        return (running, starter, wh.name or "")

    return sorted(warehouses, key=sort_key)[0].id


def workspace_path_exists(w: WorkspaceClient, path: str) -> bool:
    try:
        w.workspace.get_status(path)
        return True
    except NotFound:
        return False


def repos_pipeline_paths(email: str) -> list[str]:
    base = f"/Workspace/Repos/{email}/{REPO_NAME}/src/pipelines"
    return [f"{base}/{f}" for f in PIPELINE_FILES]


def find_pipeline_id(w: WorkspaceClient, name: str) -> str | None:
    for p in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if p.name == name:
            return p.pipeline_id
    return None


def create_or_update_pipeline(
    w: WorkspaceClient,
    *,
    name: str,
    catalog: str,
    schema: str,
    raw_path: str,
    library_paths: list[str],
) -> str:
    libraries = [PipelineLibrary(file=FileLibrary(path=p)) for p in library_paths]
    config = {"workshop.raw_path": raw_path}

    existing = find_pipeline_id(w, name)
    if existing:
        print(f"  updating existing pipeline {existing}")
        w.pipelines.update(
            pipeline_id=existing,
            name=name,
            catalog=catalog,
            target=schema,
            libraries=libraries,
            configuration=config,
            serverless=True,
            photon=True,
            channel="CURRENT",
            continuous=False,
            development=True,
        )
        return existing

    print(f"  creating new pipeline {name}")
    resp = w.pipelines.create(
        name=name,
        catalog=catalog,
        target=schema,
        libraries=libraries,
        configuration=config,
        serverless=True,
        photon=True,
        channel="CURRENT",
        continuous=False,
        development=True,
    )
    return resp.pipeline_id


def wait_for_pipeline_idle(w: WorkspaceClient, pipeline_id: str, timeout_s: int = 60) -> None:
    """Wait for any in-flight update to settle so we can safely (re)start."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        p = w.pipelines.get(pipeline_id)
        latest = (p.latest_updates or [None])[0]
        if not latest:
            return
        state = latest.state.value if latest.state else "UNKNOWN"
        if state in {"COMPLETED", "FAILED", "CANCELED"}:
            return
        time.sleep(3)


def run_pipeline_and_wait(w: WorkspaceClient, pipeline_id: str, timeout_s: int = 600) -> str:
    wait_for_pipeline_idle(w, pipeline_id, timeout_s=30)
    update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
    update_id = update.update_id
    print(f"  triggered update {update_id}; waiting up to {timeout_s}s")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        u = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
        state = u.update.state.value if u.update.state else "UNKNOWN"
        if state in {"COMPLETED", "FAILED", "CANCELED"}:
            print(f"  update finished: {state}")
            return state
        time.sleep(8)
    raise SystemExit(f"Pipeline update {update_id} did not finish within {timeout_s}s")


def find_job_id(w: WorkspaceClient, name: str) -> int | None:
    for j in w.jobs.list(name=name):
        if j.settings and j.settings.name == name:
            return j.job_id
    return None


def create_or_update_job(w: WorkspaceClient, *, name: str, pipeline_id: str) -> int:
    """Create a Lakeflow Job that runs the dev pipeline.

    Mirrors the bundle's `resources/ingestion_job.yml` shape so that "Edit as
    YAML" in the workspace UI produces output recognisable to attendees.
    """
    task = Task(
        task_key="run_ingestion_pipeline",
        pipeline_task=PipelineTask(pipeline_id=pipeline_id, full_refresh=False),
    )
    schedule = CronSchedule(
        quartz_cron_expression="0 0 * * * ?",
        timezone_id="UTC",
        pause_status=PauseStatus.PAUSED,
    )

    existing = find_job_id(w, name)
    if existing:
        print(f"  updating existing job {existing}")
        w.jobs.reset(
            job_id=existing,
            new_settings=JobSettings(
                name=name,
                tasks=[task],
                schedule=schedule,
                max_concurrent_runs=1,
            ),
        )
        return existing

    print(f"  creating new job {name}")
    resp = w.jobs.create(
        name=name,
        tasks=[task],
        schedule=schedule,
        max_concurrent_runs=1,
    )
    return resp.job_id


def find_dashboard_id(w: WorkspaceClient, display_name: str) -> str | None:
    for d in w.lakeview.list():
        if d.display_name == display_name and d.lifecycle_state != LifecycleState.TRASHED:
            return d.dashboard_id
    return None


def create_or_update_dashboard(
    w: WorkspaceClient,
    *,
    display_name: str,
    parent_path: str,
    catalog: str,
    schema: str,
    warehouse_id: str,
) -> tuple[str, str]:
    serialized = DASHBOARD_TEMPLATE.read_text()
    dashboard = Dashboard(
        display_name=display_name,
        parent_path=parent_path,
        serialized_dashboard=serialized,
        warehouse_id=warehouse_id,
    )
    existing = find_dashboard_id(w, display_name)
    if existing:
        print(f"  updating existing dashboard {existing}")
        result = w.lakeview.update(
            dashboard_id=existing,
            dashboard=dashboard,
            dataset_catalog=catalog,
            dataset_schema=schema,
        )
    else:
        print(f"  creating new dashboard {display_name}")
        result = w.lakeview.create(
            dashboard=dashboard,
            dataset_catalog=catalog,
            dataset_schema=schema,
        )

    print("  publishing dashboard")
    w.lakeview.publish(dashboard_id=result.dashboard_id, warehouse_id=warehouse_id, embed_credentials=True)
    return result.dashboard_id, result.path or ""


def find_genie_space_id(w: WorkspaceClient, title: str) -> str | None:
    resp = w.genie.list_spaces()
    for s in (resp.spaces or []):
        if s.title == title:
            return s.space_id
    return None


def create_or_update_genie_space(
    w: WorkspaceClient,
    *,
    title: str,
    description: str,
    catalog: str,
    schema: str,
    warehouse_id: str,
) -> str:
    template = GENIE_TEMPLATE.read_text()
    serialized = template.replace(GENIE_CATALOG_TOKEN, catalog).replace(GENIE_SCHEMA_TOKEN, schema)
    # Validate it's still parseable JSON after substitution
    json.loads(serialized)

    existing = find_genie_space_id(w, title)
    if existing:
        print(f"  updating existing Genie space {existing}")
        w.genie.update_space(
            space_id=existing,
            title=title,
            description=description,
            warehouse_id=warehouse_id,
            serialized_space=serialized,
        )
        return existing

    print(f"  creating new Genie space {title}")
    result = w.genie.create_space(
        warehouse_id=warehouse_id,
        serialized_space=serialized,
        title=title,
        description=description,
    )
    return result.space_id


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", required=True, help="Unity Catalog name (must exist; need CREATE_SCHEMA + CREATE_VOLUME)")
    p.add_argument("--profile", default=None, help="Databricks CLI profile to use")
    p.add_argument("--skip-pipeline-run", action="store_true", help="Skip the initial pipeline run (default: run once)")
    p.add_argument("--files-per-source", type=int, default=5, help="Files to write per fact source (default: 5)")
    p.add_argument("--bad-data-pct", type=float, default=0.05, help="Fraction of fact rows with bad data (default: 0.05)")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    me = w.current_user.me()
    if not me.user_name:
        raise SystemExit("Could not resolve current user")
    email = me.user_name
    short = resolve_user_short_name(w)
    schema = f"{SCHEMA_PREFIX}_{short}"
    raw_path = f"/Volumes/{args.catalog}/{schema}/{VOLUME_NAME}"
    asset_name = f"edetl-workshop-{short}"

    print(f"workspace: {w.config.host}")
    print(f"user:      {email}  (short: {short})")
    print(f"target:    {args.catalog}.{schema}.{VOLUME_NAME}")
    print(f"asset name: {asset_name}")

    # ── Schema + volume ────────────────────────────────────────────────────
    print("\n[1/7] Schema + volume")
    ensure_schema(w, args.catalog, schema)
    ensure_volume(w, args.catalog, schema, VOLUME_NAME)

    # ── Initial files ──────────────────────────────────────────────────────
    print("\n[2/7] Initial synthetic files")
    cfg = GeneratorConfig(
        catalog=args.catalog,
        schema=schema,
        volume=VOLUME_NAME,
        files_per_source=args.files_per_source,
        seed=args.seed,
        bad_data_pct=args.bad_data_pct,
    )
    fake = Faker()
    Faker.seed(args.seed)
    matter_ids, user_ids, doc_ids = build_id_pools(args.seed)
    write_matters_master_once(w, cfg, fake, matter_ids, user_ids)
    write_fact_batch(w, cfg, fake, matter_ids, user_ids, doc_ids)

    # ── Verify Repos clone ─────────────────────────────────────────────────
    print("\n[3/7] Verify Repos clone")
    library_paths = repos_pipeline_paths(email)
    missing = [p for p in library_paths if not workspace_path_exists(w, p)]
    if missing:
        print("  ERROR: required pipeline files are missing in your Workspace Repos.")
        for p in missing:
            print(f"    {p}")
        print("  Clone the repo first via Workspace -> Repos -> Add repo:")
        print(f"    https://github.com/fabienv-vibes/{REPO_NAME}")
        print(f"  Expected location: /Workspace/Repos/{email}/{REPO_NAME}/")
        return 1
    print(f"  found all 3 pipeline files under /Workspace/Repos/{email}/{REPO_NAME}/")

    # ── SDP pipeline ───────────────────────────────────────────────────────
    print("\n[4/7] SDP pipeline")
    pipeline_id = create_or_update_pipeline(
        w,
        name=asset_name,
        catalog=args.catalog,
        schema=schema,
        raw_path=raw_path,
        library_paths=library_paths,
    )

    if args.skip_pipeline_run:
        print("  skipping initial pipeline run (--skip-pipeline-run)")
    else:
        run_pipeline_and_wait(w, pipeline_id, timeout_s=600)

    # ── Job ────────────────────────────────────────────────────────────────
    print("\n[5/7] Lakeflow Job (for the 'Edit as YAML' demo)")
    job_id = create_or_update_job(w, name=asset_name, pipeline_id=pipeline_id)

    # ── Dashboard ──────────────────────────────────────────────────────────
    print("\n[6/7] Dashboard")
    warehouse_id = pick_warehouse_id(w)
    dashboard_id, dashboard_path = create_or_update_dashboard(
        w,
        display_name=asset_name,
        parent_path=f"/Workspace/Users/{email}",
        catalog=args.catalog,
        schema=schema,
        warehouse_id=warehouse_id,
    )

    # ── Genie space ────────────────────────────────────────────────────────
    print("\n[7/7] Genie space")
    description = (
        "Natural-language exploration of legal practice data: matters, billable hours, "
        "and document activity. Backed by the edetl-workshop SDP gold layer."
    )
    space_id = create_or_update_genie_space(
        w,
        title=asset_name,
        description=description,
        catalog=args.catalog,
        schema=schema,
        warehouse_id=warehouse_id,
    )

    # ── Summary ────────────────────────────────────────────────────────────
    host = w.config.host.rstrip("/")
    print("\n" + "=" * 70)
    print("done. open these in your workspace:")
    print(f"  pipeline:  {host}/pipelines/{pipeline_id}")
    print(f"  job:       {host}/jobs/{job_id}")
    print(f"  dashboard: {host}/sql/dashboardsv3/{dashboard_id}")
    print(f"  genie:     {host}/genie/rooms/{space_id}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
