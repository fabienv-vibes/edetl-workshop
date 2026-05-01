# Databricks notebook source
# MAGIC %md
# MAGIC # edetl-workshop — drop more files
# MAGIC
# MAGIC Re-runs the synthetic file generator to drop more JSON files into your `raw_landing` volume.
# MAGIC
# MAGIC After this finishes, click **Run** on your SDP pipeline to watch Auto Loader pick up the new files —
# MAGIC bronze grows by exactly the file count, silver applies expectations and drops bad rows, gold
# MAGIC materialized views recompute.
# MAGIC
# MAGIC **How to run:** run cell 1 to create the widgets, set `catalog` (and optionally `schema` —
# MAGIC leave blank for your dev schema, set to `edetl_stg` for the staging volume after Block B's deploy),
# MAGIC then **Run all** from the top.

# COMMAND ----------

# Cell 1: create widgets (run this first so they appear at the top of the notebook).
dbutils.widgets.text("catalog", "", "Unity Catalog")
dbutils.widgets.text("schema", "", "Schema (blank = your dev schema)")

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Preview the target volume this run will write to.
import os
import sys

catalog = dbutils.widgets.get("catalog")
schema_override = dbutils.widgets.get("schema") or None
assert catalog, "Set the `catalog` widget at the top of the notebook before running."

sys.path.insert(0, os.path.join(os.getcwd(), "src", "generator"))

from databricks.sdk import WorkspaceClient
from generate_files import SCHEMA_PREFIX, VOLUME_NAME, resolve_user_short_name

w = WorkspaceClient()
short = resolve_user_short_name(w)
schema = schema_override or f"{SCHEMA_PREFIX}_{short}"

print("This run will write JSON files into:")
print(f"  Workspace: {w.config.host}")
print(f"  Volume:    {catalog}.{schema}.{VOLUME_NAME}")

# COMMAND ----------

# Run the generator.
from generate_files import main

args = ["--catalog", catalog]
if schema_override:
    args.extend(["--schema", schema_override])
main(args)
