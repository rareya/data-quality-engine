"""
tests/test_rules.py
--------------------
Unit tests for backend/dq_engine/rules.py

Each rule is tested against a minimal fake `profile` dict — no need to
run the full pipeline or load real files. This keeps tests fast and
isolates rule logic from the loader/profiler.

Run with:
    pytest tests/test_rules.py -v
"""

import pytest

from backend.dq_engine.rules import (
    MissingValueRule,
    DuplicateRowRule,
    ErrorRateRule,
    TrafficVolumeRule,
    ConstantColumnRule,
)


# ---------------- MissingValueRule ----------------

def test_missing_value_rule_passes_when_within_limit():
    profile = {
        "columns": {
            "age": {"missing_pct": 5.0}
        }
    }
    rule = MissingValueRule(column="age", max_missing_pct=10.0)
    result = rule.evaluate(profile)

    assert result.passed is True
    assert "within limit" in result.message


def test_missing_value_rule_fails_when_over_limit():
    profile = {
        "columns": {
            "age": {"missing_pct": 45.0}
        }
    }
    rule = MissingValueRule(column="age", max_missing_pct=10.0)
    result = rule.evaluate(profile)

    assert result.passed is False
    assert "45.0" in result.message


def test_missing_value_rule_fails_when_column_not_found():
    profile = {"columns": {}}
    rule = MissingValueRule(column="ghost_column", max_missing_pct=10.0)
    result = rule.evaluate(profile)

    assert result.passed is False
    assert "not found" in result.message


# ---------------- DuplicateRowRule ----------------

def test_duplicate_row_rule_passes_when_within_limit():
    profile = {"row_count": 1000, "duplicate_rows": 10}  # 1%
    rule = DuplicateRowRule(max_duplicate_pct=5.0)
    result = rule.evaluate(profile)

    assert result.passed is True


def test_duplicate_row_rule_fails_when_over_limit():
    profile = {"row_count": 1000, "duplicate_rows": 200}  # 20%
    rule = DuplicateRowRule(max_duplicate_pct=5.0)
    result = rule.evaluate(profile)

    assert result.passed is False
    assert "20.00%" in result.message


def test_duplicate_row_rule_handles_zero_rows_without_crashing():
    profile = {"row_count": 0, "duplicate_rows": 0}
    rule = DuplicateRowRule(max_duplicate_pct=5.0)
    result = rule.evaluate(profile)

    # Should not raise ZeroDivisionError, and 0% is within any limit
    assert result.passed is True


# ---------------- ErrorRateRule ----------------

def test_error_rate_rule_passes_when_within_limit():
    profile = {"log_metrics": {"error_rate_pct": 1.5}}
    rule = ErrorRateRule(max_error_rate_pct=5.0)
    result = rule.evaluate(profile)

    assert result.passed is True


def test_error_rate_rule_fails_when_over_limit():
    profile = {"log_metrics": {"error_rate_pct": 25.0}}
    rule = ErrorRateRule(max_error_rate_pct=5.0)
    result = rule.evaluate(profile)

    assert result.passed is False


def test_error_rate_rule_fails_when_metric_missing():
    profile = {"log_metrics": {}}
    rule = ErrorRateRule(max_error_rate_pct=5.0)
    result = rule.evaluate(profile)

    assert result.passed is False
    assert "missing" in result.message.lower()


# ---------------- TrafficVolumeRule ----------------

def test_traffic_volume_rule_passes_when_enough_rows():
    profile = {"row_count": 500}
    rule = TrafficVolumeRule(min_rows=100)
    result = rule.evaluate(profile)

    assert result.passed is True


def test_traffic_volume_rule_fails_when_too_few_rows():
    profile = {"row_count": 3}
    rule = TrafficVolumeRule(min_rows=100)
    result = rule.evaluate(profile)

    assert result.passed is False
    assert "3 rows" in result.message


# ---------------- ConstantColumnRule ----------------

def test_constant_column_rule_passes_when_column_varies():
    profile = {
        "columns": {
            "status": {"unique_count": 4}
        }
    }
    rule = ConstantColumnRule(column="status")
    result = rule.evaluate(profile)

    assert result.passed is True


def test_constant_column_rule_fails_when_column_is_constant():
    profile = {
        "columns": {
            "status": {"unique_count": 1}
        }
    }
    rule = ConstantColumnRule(column="status")
    result = rule.evaluate(profile)

    assert result.passed is False
    assert "constant" in result.message


def test_constant_column_rule_fails_when_column_not_found():
    profile = {"columns": {}}
    rule = ConstantColumnRule(column="ghost_column")
    result = rule.evaluate(profile)

    assert result.passed is False
    assert "not found" in result.message