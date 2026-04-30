"""Synthetic file generator for the edetl-workshop SDP demo.

Drops legal-domain JSON files (matters, time entries, doc audit) into a UC Volume.
Each invocation writes a fresh batch — re-running simulates new files arriving so
Auto Loader's incremental processing is visible.

Usage:
    python generate_files.py --catalog <uc_catalog>
    python generate_files.py --catalog ed_dev --files-per-source 3 --seed 7

Requires:
    pip install databricks-sdk faker

Auth: uses the active Databricks profile (DEFAULT) or env vars
(DATABRICKS_HOST + DATABRICKS_TOKEN, or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET).
When run inside a Databricks notebook, no auth setup is needed.
"""

from __future__ import annotations

import argparse
import io
import json
import random
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists
from faker import Faker


SCHEMA_PREFIX = "edetl_workshop"
VOLUME_NAME = "raw_landing"

MATTER_POOL_SIZE = 200
USER_POOL_SIZE = 50
DOC_POOL_SIZE = 600

EVENT_TYPES = ["opened", "status_change", "note_added", "closed"]
DOC_ACTIONS = ["view", "edit", "share", "download", "delete"]


@dataclass
class GeneratorConfig:
    catalog: str
    schema: str
    volume: str
    files_per_source: int
    seed: int


def short_name_from_email(email: str) -> str:
    return email.split("@")[0].replace(".", "_").replace("-", "_").lower()


def resolve_user_short_name(w: WorkspaceClient) -> str:
    me = w.current_user.me()
    if not me.user_name:
        raise RuntimeError("Could not resolve current user from Databricks workspace")
    return short_name_from_email(me.user_name)


def ensure_schema(w: WorkspaceClient, catalog: str, schema: str) -> None:
    full = f"{catalog}.{schema}"
    try:
        w.schemas.get(full)
        print(f"  schema {full} already exists")
    except NotFound:
        w.schemas.create(name=schema, catalog_name=catalog)
        print(f"  created schema {full}")


def ensure_volume(w: WorkspaceClient, catalog: str, schema: str, volume: str) -> None:
    full = f"{catalog}.{schema}.{volume}"
    try:
        w.volumes.read(full)
        print(f"  volume {full} already exists")
    except NotFound:
        from databricks.sdk.service.catalog import VolumeType

        w.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume,
            volume_type=VolumeType.MANAGED,
        )
        print(f"  created volume {full}")


def generate_matter_events(rng: random.Random, fake: Faker, matter_ids: list[str], user_ids: list[str]) -> list[dict]:
    n = rng.randint(50, 500)
    now = datetime.now(timezone.utc)
    rows = []
    for _ in range(n):
        days_ago = rng.randint(0, 90)
        ts = now - timedelta(days=days_ago, seconds=rng.randint(0, 86400))
        rows.append(
            {
                "event_id": str(uuid.uuid4()),
                "matter_id": rng.choice(matter_ids),
                "event_type": rng.choices(EVENT_TYPES, weights=[1, 5, 4, 1], k=1)[0],
                "timestamp": ts.isoformat(),
                "user_id": rng.choice(user_ids),
                "office": rng.choice(["NYC", "LON", "CHI", "SFO", "BLR"]),
            }
        )
    return rows


def generate_time_entries(rng: random.Random, fake: Faker, matter_ids: list[str], user_ids: list[str]) -> list[dict]:
    n = rng.randint(80, 500)
    today = datetime.now(timezone.utc).date()
    rows = []
    for _ in range(n):
        days_ago = rng.randint(0, 60)
        rows.append(
            {
                "entry_id": str(uuid.uuid4()),
                "matter_id": rng.choice(matter_ids),
                "user_id": rng.choice(user_ids),
                "hours": round(rng.uniform(0.1, 8.0), 2),
                "billable_flag": rng.random() < 0.85,
                "rate_usd": rng.choice([250, 350, 450, 600, 850, 1100]),
                "date": (today - timedelta(days=days_ago)).isoformat(),
                "narrative": fake.sentence(nb_words=8),
            }
        )
    return rows


def generate_doc_audit(rng: random.Random, fake: Faker, doc_ids: list[str], matter_ids: list[str], user_ids: list[str]) -> list[dict]:
    n = rng.randint(100, 500)
    now = datetime.now(timezone.utc)
    rows = []
    for _ in range(n):
        seconds_ago = rng.randint(0, 90 * 86400)
        ts = now - timedelta(seconds=seconds_ago)
        rows.append(
            {
                "audit_id": str(uuid.uuid4()),
                "doc_id": rng.choice(doc_ids),
                "matter_id": rng.choice(matter_ids),
                "action": rng.choices(DOC_ACTIONS, weights=[60, 20, 8, 10, 2], k=1)[0],
                "user_id": rng.choice(user_ids),
                "timestamp": ts.isoformat(),
                "client_ip": fake.ipv4_public(),
            }
        )
    return rows


def build_id_pools(seed: int) -> tuple[list[str], list[str], list[str]]:
    rng = random.Random(seed)
    matter_ids = [f"M-{rng.randint(10000, 99999)}-{i:04d}" for i in range(MATTER_POOL_SIZE)]
    user_ids = [f"U-{i:04d}" for i in range(USER_POOL_SIZE)]
    doc_ids = [f"D-{rng.randint(100000, 999999)}-{i:05d}" for i in range(DOC_POOL_SIZE)]
    return matter_ids, user_ids, doc_ids


def write_jsonl(w: WorkspaceClient, volume_path: str, filename: str, rows: list[dict]) -> int:
    """Write JSON-lines file to the volume. Returns bytes written."""
    buf = io.BytesIO()
    for row in rows:
        buf.write((json.dumps(row, default=str) + "\n").encode("utf-8"))
    payload = buf.getvalue()
    full_path = f"{volume_path}/{filename}"
    w.files.upload(full_path, contents=io.BytesIO(payload), overwrite=True)
    return len(payload)


def write_batch(
    w: WorkspaceClient,
    cfg: GeneratorConfig,
    fake: Faker,
    matter_ids: list[str],
    user_ids: list[str],
    doc_ids: list[str],
) -> None:
    base = f"/Volumes/{cfg.catalog}/{cfg.schema}/{cfg.volume}"
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    sources = {
        "matters": lambda r: generate_matter_events(r, fake, matter_ids, user_ids),
        "time_entries": lambda r: generate_time_entries(r, fake, matter_ids, user_ids),
        "doc_audit": lambda r: generate_doc_audit(r, fake, doc_ids, matter_ids, user_ids),
    }

    for source, gen_fn in sources.items():
        print(f"  writing {cfg.files_per_source} files to {base}/{source}/")
        for i in range(cfg.files_per_source):
            rng = random.Random(f"{cfg.seed}-{source}-{run_ts}-{i}")
            rows = gen_fn(rng)
            filename = f"{source}/{run_ts}_{i:02d}_{uuid.uuid4().hex[:8]}.json"
            size = write_jsonl(w, base, filename, rows)
            print(f"    {filename}  ({len(rows)} rows, {size:,} bytes)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", required=True, help="Unity Catalog name (must exist, you must have CREATE_SCHEMA + CREATE_VOLUME)")
    p.add_argument("--files-per-source", type=int, default=5, help="Files to write per source per invocation (default: 5)")
    p.add_argument("--seed", type=int, default=42, help="RNG seed for ID pools (default: 42)")
    p.add_argument("--profile", default=None, help="Databricks CLI profile to use (default: env or DEFAULT)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    short = resolve_user_short_name(w)
    schema = f"{SCHEMA_PREFIX}_{short}"

    cfg = GeneratorConfig(
        catalog=args.catalog,
        schema=schema,
        volume=VOLUME_NAME,
        files_per_source=args.files_per_source,
        seed=args.seed,
    )

    print(f"workspace: {w.config.host}")
    print(f"target: {cfg.catalog}.{cfg.schema}.{cfg.volume}")

    ensure_schema(w, cfg.catalog, cfg.schema)
    ensure_volume(w, cfg.catalog, cfg.schema, cfg.volume)

    fake = Faker()
    Faker.seed(args.seed)
    matter_ids, user_ids, doc_ids = build_id_pools(args.seed)

    write_batch(w, cfg, fake, matter_ids, user_ids, doc_ids)
    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
