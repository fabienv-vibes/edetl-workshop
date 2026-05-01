# Databricks notebook source
# MAGIC %md
# MAGIC # edetl-workshop — dev setup
# MAGIC
# MAGIC Provisions everything you need to start iterating, in one shot:
# MAGIC
# MAGIC - per-user schema and `raw_landing` UC volume
# MAGIC - initial batch of synthetic JSON files (with ~5% deliberately bad rows)
# MAGIC - serverless SDP pipeline pointing at `src/pipelines/` in this Git folder
# MAGIC - Lakeflow Job that runs the pipeline (used for the "Edit as YAML" demo in Block B)
# MAGIC - AI/BI dashboard
# MAGIC - Genie space
# MAGIC
# MAGIC **Before running:** set the `catalog` widget below to a Unity Catalog you have
# MAGIC `USE_CATALOG`, `CREATE_SCHEMA`, and `CREATE_VOLUME` on.
# MAGIC
# MAGIC Idempotent — re-run any time and it upgrades existing assets in place.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog")
catalog = dbutils.widgets.get("catalog")
assert catalog, "Set the `catalog` widget at the top of the notebook before running."

# COMMAND ----------

import os
import sys

# This notebook lives at the root of the workshop Git folder; src/ is one level down.
sys.path.insert(0, os.path.join(os.getcwd(), "src"))

from setup_dev import main

main(["--catalog", catalog])
