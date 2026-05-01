"""Optional Genie space setup for the edetl-workshop.

Run after `setup_dev.py` has provisioned the schema, volume, pipeline, and job,
and the pipeline has produced gold tables.

Creates a Genie space `edetl-workshop-<short>` wired to the silver/gold tables,
pre-loaded with sample questions about matters, billable hours, and doc activity.

The dashboard is intentionally NOT scripted here — Block C of the workshop demos
creating it via Genie Code instead, to showcase that natural-language workflow.
See the README for the prompt.

Re-running upgrades an existing space in place (idempotent by name).

Usage:
    python src/setup_extras.py --catalog <UC_CATALOG>

Or from the `02_extras.py` notebook in this repo's root.

Requires: pip install -U databricks-sdk faker
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

# Reuse the generator's helpers for consistent schema-name derivation.
sys.path.insert(0, str(Path(__file__).parent / "generator"))
from generate_files import (  # noqa: E402
    SCHEMA_PREFIX,
    resolve_user_short_name,
)


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
    json.loads(serialized)  # validate

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
    p.add_argument("--catalog", required=True, help="Unity Catalog name (must exist; same one used for setup_dev)")
    p.add_argument("--profile", default=None, help="Databricks CLI profile to use")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    me = w.current_user.me()
    if not me.user_name:
        raise SystemExit("Could not resolve current user")
    short = resolve_user_short_name(w)
    schema = f"{SCHEMA_PREFIX}_{short}"
    asset_name = f"edetl-workshop-{short}"

    print(f"workspace:  {w.config.host}")
    print(f"target:     {args.catalog}.{schema}")
    print(f"asset name: {asset_name}")

    print("\n[1/1] Genie space")
    warehouse_id = pick_warehouse_id(w)
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

    host = w.config.host.rstrip("/")
    print("\n" + "=" * 70)
    print("done. open this in your workspace:")
    print(f"  genie:     {host}/genie/rooms/{space_id}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
