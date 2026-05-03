# Databricks notebook source
# MAGIC %md
# MAGIC # edetl-workshop — dev setup
# MAGIC
# MAGIC Provisions the data engineering core in one shot:
# MAGIC
# MAGIC - per-user schema and `raw_landing` UC volume
# MAGIC - initial batch of synthetic JSON files (with ~5% deliberately bad rows)
# MAGIC - serverless SDP pipeline pointing at `src/pipelines/` in this Git folder
# MAGIC - Lakeflow Job that runs the pipeline (used for the "Edit as YAML" demo in Block B)
# MAGIC
# MAGIC **Optional Block C extras** (not run here, to keep this notebook fast):
# MAGIC - Dashboard — provisioned via a Genie Code prompt (see README)
# MAGIC - Genie space — provisioned by `02_extras.py`
# MAGIC
# MAGIC **How to run:** run cell 1 below to create the `catalog` widget, set it to a Unity Catalog you have
# MAGIC `USE_CATALOG`, `CREATE_SCHEMA`, and `CREATE_VOLUME` on, then **Run all** from the top.
# MAGIC
# MAGIC The names of resources are derived from your email — schema `edetl_workshop_<short_username>`,
# MAGIC and pipeline / job both named `edetl-workshop-<short_username>`.
# MAGIC The preview cell below prints the exact names before anything is created.
# MAGIC
# MAGIC Idempotent — re-run any time and it upgrades existing assets in place.

# COMMAND ----------

# Cell 1: create the widget (run this first so the widget appears at the top of the notebook).
dbutils.widgets.text("catalog", "", "Unity Catalog")

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Preview the resources this run will create / update.
import os
import sys

catalog = dbutils.widgets.get("catalog")
assert catalog, "Set the `catalog` widget at the top of the notebook before running."

sys.path.insert(0, os.path.join(os.getcwd(), "src"))
sys.path.insert(0, os.path.join(os.getcwd(), "src", "generator"))

from databricks.sdk import WorkspaceClient
from generate_files import SCHEMA_PREFIX, VOLUME_NAME, resolve_user_short_name

w = WorkspaceClient()
short = resolve_user_short_name(w)
schema = f"{SCHEMA_PREFIX}_{short}"
asset_name = f"edetl-workshop-{short}"

print("This run will create / update the following in your workspace:")
print(f"  Workspace: {w.config.host}")
print(f"  Schema:    {catalog}.{schema}")
print(f"  Volume:    {catalog}.{schema}.{VOLUME_NAME}")
print(f"  Pipeline:  {asset_name}")
print(f"  Job:       {asset_name}")
print()
print(f"It will also write `.databricks/bundle/stg/variable-overrides.json` so")
print(f"Block B's bundle deploy reuses `catalog={catalog}` automatically.")

# COMMAND ----------

# Run the setup. The script also prints the same summary plus the resource URLs at the end.
from setup_dev import main

main(["--catalog", catalog])
