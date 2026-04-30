"""Gold layer — business-facing aggregates as materialized views.

Materialized views recompute fully on each refresh, which keeps the workshop
example simple. For a real workload with high cardinality you would consider
streaming aggregates with `dlt.read_stream(...) + window` instead.

`gold_matter_summary` is the centerpiece of the medallion DAG: it joins the
matters dimension with three aggregated fact streams (events, time entries,
doc audit) into a single per-matter rollup.
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_billable_hours_by_matter_month",
    comment="Aggregate billable hours and revenue by matter and calendar month.",
    table_properties={"quality": "gold"},
)
def gold_billable_hours_by_matter_month():
    return (
        dlt.read("silver_time_entries")
        .filter(F.col("billable_flag") == True)  # noqa: E712 - Spark requires explicit ==
        .groupBy(
            "matter_id",
            F.trunc(F.col("entry_date"), "month").alias("entry_month"),
        )
        .agg(
            F.sum("hours").alias("total_hours"),
            F.sum("billable_amount_usd").alias("total_revenue_usd"),
            F.countDistinct("user_id").alias("distinct_users"),
            F.countDistinct("entry_id").alias("entry_count"),
        )
    )


@dlt.table(
    name="gold_doc_activity_by_matter",
    comment="Per-matter doc activity counts, broken out by action type.",
    table_properties={"quality": "gold"},
)
def gold_doc_activity_by_matter():
    return (
        dlt.read("silver_doc_audit")
        .groupBy("matter_id", "action")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("doc_id").alias("distinct_docs"),
            F.countDistinct("user_id").alias("distinct_users"),
            F.min("audit_ts").alias("first_seen"),
            F.max("audit_ts").alias("last_seen"),
        )
    )


@dlt.table(
    name="gold_matter_lifecycle",
    comment="Per-matter lifecycle summary: first/last event, total events, current state.",
    table_properties={"quality": "gold"},
)
def gold_matter_lifecycle():
    events = dlt.read("silver_matter_events")
    return events.groupBy("matter_id").agg(
        F.min("event_ts").alias("first_event_ts"),
        F.max("event_ts").alias("last_event_ts"),
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("event_type") == "opened", 1).otherwise(0)).alias("opened_events"),
        F.sum(F.when(F.col("event_type") == "closed", 1).otherwise(0)).alias("closed_events"),
        F.collect_set("office").alias("offices_touched"),
    )


@dlt.table(
    name="gold_matter_summary",
    comment="Star-schema rollup: matter dimension + revenue + lifecycle + doc activity, one row per matter.",
    table_properties={"quality": "gold"},
)
def gold_matter_summary():
    matters = dlt.read("silver_matters_master")

    revenue = (
        dlt.read("silver_time_entries")
        .filter(F.col("billable_flag") == True)  # noqa: E712
        .groupBy("matter_id")
        .agg(
            F.sum("hours").alias("total_billable_hours"),
            F.sum("billable_amount_usd").alias("total_revenue_usd"),
            F.countDistinct("user_id").alias("distinct_timekeepers"),
            F.max("entry_date").alias("last_billed_date"),
        )
    )

    lifecycle = (
        dlt.read("silver_matter_events")
        .groupBy("matter_id")
        .agg(
            F.min("event_ts").alias("first_event_ts"),
            F.max("event_ts").alias("last_event_ts"),
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "closed", 1).otherwise(0)).alias("close_count"),
        )
    )

    doc_activity = (
        dlt.read("silver_doc_audit")
        .groupBy("matter_id")
        .agg(
            F.count("*").alias("total_doc_events"),
            F.countDistinct("doc_id").alias("distinct_docs"),
        )
    )

    return (
        matters.alias("m")
        .join(revenue, "matter_id", "left")
        .join(lifecycle, "matter_id", "left")
        .join(doc_activity, "matter_id", "left")
        .withColumn("is_closed", F.coalesce(F.col("close_count") > 0, F.lit(False)))
        .select(
            "matter_id",
            "client_name",
            "practice_area",
            "region",
            "billing_arrangement",
            "partner_lead_user_id",
            "opened_ts",
            "first_event_ts",
            "last_event_ts",
            "is_closed",
            F.coalesce(F.col("total_billable_hours"), F.lit(0).cast("decimal(15,2)")).alias("total_billable_hours"),
            F.coalesce(F.col("total_revenue_usd"), F.lit(0).cast("decimal(22,2)")).alias("total_revenue_usd"),
            F.coalesce(F.col("distinct_timekeepers"), F.lit(0)).alias("distinct_timekeepers"),
            "last_billed_date",
            F.coalesce(F.col("total_doc_events"), F.lit(0)).alias("total_doc_events"),
            F.coalesce(F.col("distinct_docs"), F.lit(0)).alias("distinct_docs"),
            F.coalesce(F.col("total_events"), F.lit(0)).alias("total_lifecycle_events"),
        )
    )
