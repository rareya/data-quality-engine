"""
tests/test_scorer.py
---------------------
Unit tests for backend/dq_engine/scorer.py

Uses lightweight fake rule-result objects instead of real RuleResult
instances, since QualityScorer only reads .weight/.passed/.rule_name/
.message off whatever it's given (duck typing). Keeps tests independent
of rules.py changes.

Run with:
    pytest tests/test_scorer.py -v
"""

import pytest

from backend.dq_engine.scorer import QualityScorer


class FakeResult:
    """Minimal stand-in for RuleResult, only carrying what the scorer reads."""
    def __init__(self, rule_name, passed, weight, message="test message"):
        self.rule_name = rule_name
        self.passed = passed
        self.weight = weight
        self.message = message


# ---------------- Score calculation ----------------

def test_all_rules_passing_gives_score_100():
    results = [
        FakeResult("RuleA", True, 20),
        FakeResult("RuleB", True, 15),
        FakeResult("RuleC", True, 10),
    ]
    report = QualityScorer(results).evaluate()

    assert report["score"] == 100.0
    assert report["status"] == "PASS"


def test_all_rules_failing_gives_score_0():
    results = [
        FakeResult("RuleA", False, 20),
        FakeResult("RuleB", False, 15),
    ]
    report = QualityScorer(results).evaluate()

    assert report["score"] == 0.0
    assert report["status"] == "FAIL"


def test_partial_pass_gives_weighted_score():
    # 20 passed out of 40 total weight = 50%
    results = [
        FakeResult("RuleA", True, 20),
        FakeResult("RuleB", False, 20),
    ]
    report = QualityScorer(results).evaluate()

    assert report["score"] == 50.0
    assert report["status"] == "FAIL"  # below 70 threshold


def test_score_between_70_and_99_is_warning_status():
    # 80 passed out of 100 total weight = 80%
    results = [
        FakeResult("RuleA", True, 80),
        FakeResult("RuleB", False, 20),
    ]
    report = QualityScorer(results).evaluate()

    assert report["score"] == 80.0
    assert report["status"] == "WARNING"


def test_empty_rule_list_returns_no_rules_status():
    report = QualityScorer([]).evaluate()

    assert report["score"] == 0.0
    assert report["status"] == "NO_RULES"
    assert report["details"] == []


# ---------------- Details/report structure ----------------

def test_details_list_matches_input_rules():
    results = [
        FakeResult("MissingValueRule", True, 20, "age: missing within limit"),
        FakeResult("DuplicateRowRule", False, 15, "12.00% duplicate rows"),
    ]
    report = QualityScorer(results).evaluate()

    assert len(report["details"]) == 2
    assert report["details"][0]["rule"] == "MissingValueRule"
    assert report["details"][0]["passed"] is True
    assert report["details"][1]["rule"] == "DuplicateRowRule"
    assert report["details"][1]["message"] == "12.00% duplicate rows"


def test_total_and_passed_weight_are_reported_correctly():
    results = [
        FakeResult("RuleA", True, 30),
        FakeResult("RuleB", False, 10),
        FakeResult("RuleC", True, 10),
    ]
    report = QualityScorer(results).evaluate()

    assert report["total_weight"] == 50
    assert report["passed_weight"] == 40