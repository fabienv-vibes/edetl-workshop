"""Gold layer — business-facing aggregates as materialized views.

Materialized views recompute fully on each refresh, which keeps the workshop
example simple. For a real workload with high cardinality you would consider
streaming aggregates with `dlt.read_stream(...) + window` instead.
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
