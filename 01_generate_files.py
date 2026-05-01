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
# MAGIC Leave `schema` blank for your dev schema. Set it to `edetl_stg` after Block B's bundle deploy to
# MAGIC seed the staging volume instead.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog")
dbutils.widgets.text("schema", "", "Schema (blank = your dev schema)")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema") or None
assert catalog, "Set the `catalog` widget at the top of the notebook before running."

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "src", "generator"))

from generate_files import main

args = ["--catalog", catalog]
if schema:
    args.extend(["--schema", schema])
main(args)
