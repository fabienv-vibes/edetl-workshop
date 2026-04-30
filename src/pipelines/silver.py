"""Silver layer — typed, validated, append-only streaming tables.

Reads bronze streams, casts timestamps, derives `entry_date`, computes
`billable_amount_usd`, and applies expectations (`@dlt.expect_or_drop`)
that drop rows failing data quality checks.

Note: rate_tier categorization is done with native Spark `when().otherwise()`
rather than a Python UDF — same logic as `_transforms.categorize_rate_tier`,
but avoids UDF serialization overhead. The pure-Python version in
`_transforms.py` exists so the same rule can be unit-tested in CI.
"""

import dlt
from pyspark.sql import functions as F


def _rate_tier_expr(col):
    """Native Spark expression that mirrors _transforms.categorize_rate_tier."""
    return (
        F.when(col.isNull(), "unknown")
        .when(col < 300, "junior")
        .when(col < 600, "associate")
        .when(col < 900, "senior")
        .otherwise("partner")
    )


@dlt.table(
    name="silver_matter_events",
    comment="Cleaned matter events: typed timestamp, recognized event types only.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_event_type", "event_type IN ('opened', 'status_change', 'note_added', 'closed')")
@dlt.expect_or_drop("has_matter_id", "matter_id IS NOT NULL AND matter_id != ''")
@dlt.expect_or_drop("has_event_id", "event_id IS NOT NULL")
def silver_matter_events():
    return (
        dlt.read_stream("bronze_matter_events")
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .select(
            "event_id",
            "matter_id",
            "event_type",
            "event_ts",
            "event_date",
            "user_id",
            "office",
            "_source_file",
            "_ingested_at",
        )
    )


@dlt.table(
    name="silver_time_entries",
    comment="Cleaned time entries with derived billable_amount_usd and rate_tier.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("positive_hours", "hours > 0 AND hours <= 24")
@dlt.expect_or_drop("has_user", "user_id IS NOT NULL")
@dlt.expect_or_drop("has_matter", "matter_id IS NOT NULL")
def silver_time_entries():
    return (
        dlt.read_stream("bronze_time_entries")
        .withColumn("entry_date", F.to_date(F.col("date")))
        .withColumn("hours_decimal", F.col("hours").cast("decimal(5,2)"))
        .withColumn("rate_decimal", F.col("rate_usd").cast("decimal(10,2)"))
        .withColumn("billable_amount_usd", (F.col("hours_decimal") * F.col("rate_decimal")).cast("decimal(12,2)"))
        .withColumn("rate_tier", _rate_tier_expr(F.col("rate_usd")))
        .select(
            "entry_id",
            "matter_id",
            "user_id",
            F.col("hours_decimal").alias("hours"),
            "billable_flag",
            F.col("rate_decimal").alias("rate_usd"),
            "billable_amount_usd",
            "rate_tier",
            "entry_date",
            "narrative",
            "_source_file",
            "_ingested_at",
        )
    )


@dlt.table(
    name="silver_doc_audit",
    comment="Cleaned doc audit events with parsed timestamp.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_action", "action IN ('view', 'edit', 'share', 'download', 'delete')")
@dlt.expect_or_drop("has_doc", "doc_id IS NOT NULL")
def silver_doc_audit():
    return (
        dlt.read_stream("bronze_doc_audit")
        .withColumn("audit_ts", F.to_timestamp(F.col("timestamp")))
        .withColumn("audit_date", F.to_date(F.col("audit_ts")))
        .select(
            "audit_id",
            "doc_id",
            "matter_id",
            "action",
            "user_id",
            "audit_ts",
            "audit_date",
            "client_ip",
            "_source_file",
            "_ingested_at",
        )
    )


@dlt.table(
    name="silver_matters_master",
    comment="Matter dimension: typed columns, one row per matter_id.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("has_matter_id", "matter_id IS NOT NULL")
@dlt.expect("known_practice_area", "practice_area IN ('M&A', 'Litigation', 'IP', 'Real Estate', 'Employment', 'Tax', 'Regulatory')")
def silver_matters_master():
    return (
        dlt.read_stream("bronze_matters_master")
        .withColumn("opened_ts", F.to_timestamp(F.col("opened_at")))
        .withColumn("opened_date", F.to_date(F.col("opened_ts")))
        .select(
            "matter_id",
            "client_name",
            "practice_area",
            "region",
            "opened_ts",
            "opened_date",
            "partner_lead_user_id",
            "billing_arrangement",
            "_source_file",
            "_ingested_at",
        )
    )
