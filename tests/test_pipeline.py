"""
tests/test_pipeline.py
------------------------
Real pytest coverage for DataQualityPipeline end-to-end orchestration.
Verified against actual report.py / QualityReport.generate() structure:

report = {
    "rows": int,
    "columns": [list of column names],
    "column_count": int,
    "quality_score": {score, status, total_weight, passed_weight, details},
    "failed_rules": [{"rule":..., "message":...}, ...],
    "column_issues": [{"column":..., "issue":...}, ...],
    # added on top by pipeline.py:
    "parse_report": ...,
    "eda": {...},
    "recommendations": ...,
    "summary": str,
}
"""
import pytest
from backend.dq_engine.pipeline import DataQualityPipeline


@pytest.fixture(scope="module")
def report():
    pipeline = DataQualityPipeline("data/ongc_access.csv")
    return pipeline.run()


def test_pipeline_runs_without_error(report):
    assert report is not None


def test_report_has_row_and_column_counts(report):
    assert report["rows"] > 0
    assert report["column_count"] == len(report["columns"])
    assert isinstance(report["columns"], list)


def test_report_quality_score_structure(report):
    qs = report["quality_score"]
    assert "score" in qs
    assert 0 <= qs["score"] <= 100
    assert qs["status"] in {"PASS", "WARNING", "FAIL", "NO_RULES"}


def test_report_failed_rules_structure(report):
    for item in report["failed_rules"]:
        assert "rule" in item
        assert "message" in item


def test_report_column_issues_structure(report):
    for item in report["column_issues"]:
        assert "column" in item
        assert "issue" in item
        assert item["column"] in report["columns"]


def test_report_includes_eda_section(report):
    assert "eda" in report
    assert "numeric_summary" in report["eda"] or "correlation_matrix" in report["eda"]


def test_report_includes_recommendations(report):
    assert "recommendations" in report


def test_report_includes_natural_language_summary(report):
    assert "summary" in report
    assert isinstance(report["summary"], str)


def test_report_includes_parse_report(report):
    assert "parse_report" in report


def test_pipeline_raises_on_missing_file():
    with pytest.raises(Exception):
        DataQualityPipeline("nonexistent_file_xyz.csv").run()