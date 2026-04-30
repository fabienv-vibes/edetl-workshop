"""Bronze layer — Auto Loader ingestion of raw JSON files into streaming tables.

One streaming table per source. Each reads from its own subfolder under the volume
path defined in the pipeline configuration (`workshop.raw_path`). Schema inference
is on; bad rows land in `_rescued_data` rather than failing the pipeline.
"""

import dlt
from pyspark.sql import functions as F


RAW_PATH = spark.conf.get("workshop.raw_path")


def _read_stream(subfolder: str):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{RAW_PATH}/{subfolder}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dlt.table(
    name="bronze_matter_events",
    comment="Raw matter events from JSON files. Append-only.",
    table_properties={"quality": "bronze"},
)
def bronze_matter_events():
    return _read_stream("matters")


@dlt.table(
    name="bronze_time_entries",
    comment="Raw time entries from JSON files. Append-only.",
    table_properties={"quality": "bronze"},
)
def bronze_time_entries():
    return _read_stream("time_entries")


@dlt.table(
    name="bronze_doc_audit",
    comment="Raw document audit events from JSON files. Append-only.",
    table_properties={"quality": "bronze"},
)
def bronze_doc_audit():
    return _read_stream("doc_audit")


@dlt.table(
    name="bronze_matters_master",
    comment="Raw matter master/dimension records. One file expected, written once at bootstrap.",
    table_properties={"quality": "bronze"},
)
def bronze_matters_master():
    return _read_stream("matters_master")
