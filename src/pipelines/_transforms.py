"""Pure-Python helpers used by the silver layer.

Kept separate from Spark code so they can be unit-tested without a SparkSession.
The silver pipeline imports these and wraps them in UDFs.
"""

from __future__ import annotations


def categorize_rate_tier(rate_usd: float | int | None) -> str:
    """Map a billable rate (USD/hour) to a coarse seniority tier.

    Used in silver layer to enrich time_entries with a tier label.
    """
    if rate_usd is None:
        return "unknown"
    if rate_usd < 300:
        return "junior"
    if rate_usd < 600:
        return "associate"
    if rate_usd < 900:
        return "senior"
    return "partner"


def is_valid_doc_action(action: str | None) -> bool:
    """True if action is one of the recognized doc audit actions."""
    return action in {"view", "edit", "share", "download", "delete"}


def is_valid_event_type(event_type: str | None) -> bool:
    """True if event_type is one of the recognized matter event types."""
    return event_type in {"opened", "status_change", "note_added", "closed"}
