"""Unit tests for src.pipelines._transforms.

These tests don't require Spark — they exercise the pure-Python helpers that
encode the same business rules used by the silver layer's native Spark
expressions. The point is to demonstrate the test-then-deploy pattern.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make `src/pipelines/_transforms.py` importable without installing the package.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src" / "pipelines"))

from _transforms import (  # noqa: E402
    categorize_rate_tier,
    is_valid_doc_action,
    is_valid_event_type,
)


class TestCategorizeRateTier:
    @pytest.mark.parametrize(
        "rate,expected",
        [
            (None, "unknown"),
            (0, "junior"),
            (100, "junior"),
            (299, "junior"),
            (300, "associate"),
            (500, "associate"),
            (599, "associate"),
            (600, "senior"),
            (800, "senior"),
            (899, "senior"),
            (900, "partner"),
            (1500, "partner"),
        ],
    )
    def test_buckets(self, rate, expected):
        assert categorize_rate_tier(rate) == expected

    def test_accepts_float(self):
        assert categorize_rate_tier(450.5) == "associate"


class TestIsValidDocAction:
    @pytest.mark.parametrize("action", ["view", "edit", "share", "download", "delete"])
    def test_valid(self, action):
        assert is_valid_doc_action(action) is True

    @pytest.mark.parametrize("action", ["", "VIEW", "print", None, "create"])
    def test_invalid(self, action):
        assert is_valid_doc_action(action) is False


class TestIsValidEventType:
    @pytest.mark.parametrize("event_type", ["opened", "status_change", "note_added", "closed"])
    def test_valid(self, event_type):
        assert is_valid_event_type(event_type) is True

    @pytest.mark.parametrize("event_type", ["", "OPENED", "deleted", None])
    def test_invalid(self, event_type):
        assert is_valid_event_type(event_type) is False
