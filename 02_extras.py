# Databricks notebook source
# MAGIC %md
# MAGIC # edetl-workshop — optional Genie space
# MAGIC
# MAGIC Provisions a Genie space wired to your dev silver/gold tables, pre-loaded with sample questions
# MAGIC about matters, billable hours, and doc activity.
# MAGIC
# MAGIC **Optional** — the workshop's data engineering story doesn't depend on this. Run after
# MAGIC `00_setup.py` has provisioned the pipeline and the gold tables have data.
# MAGIC
# MAGIC The dashboard isn't here on purpose: Block C creates it via a Genie Code prompt instead, to
# MAGIC showcase the natural-language-to-dashboard workflow. See the README.
# MAGIC
# MAGIC **How to run:** run cell 1 to create the `catalog` widget, set it to your Unity Catalog,
# MAGIC then **Run all** from the top.

# COMMAND ----------

# Cell 1: create the widget (run this first so the widget appears at the top of the notebook).
dbutils.widgets.text("catalog", "", "Unity Catalog")

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Preview the Genie space this run will create / update.
import os
import sys

catalog = dbutils.widgets.get("catalog")
assert catalog, "Set the `catalog` widget at the top of the notebook before running."

sys.path.insert(0, os.path.join(os.getcwd(), "src"))
sys.path.insert(0, os.path.join(os.getcwd(), "src", "generator"))

from databricks.sdk import WorkspaceClient
from generate_files import SCHEMA_PREFIX, resolve_user_short_name

w = WorkspaceClient()
short = resolve_user_short_name(w)
schema = f"{SCHEMA_PREFIX}_{short}"
asset_name = f"edetl-workshop-{short}"

print("This run will create / update:")
print(f"  Workspace: {w.config.host}")
print(f"  Genie:     {asset_name} (wired to {catalog}.{schema})")

# COMMAND ----------

from setup_extras import main

main(["--catalog", catalog])
