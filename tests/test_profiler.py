"""
tests/test_profiler.py
------------------------
Real pytest coverage for DataProfiler. Replaces the old print-script
version, which also imported the now-deleted loader.py/DataLoader —
using in-memory DataFrames here instead so this doesn't depend on
data/students.csv existing or SmartLoader's exact return signature.
"""
import pandas as pd
from backend.dq_engine.profiler import DataProfiler


def test_profile_returns_row_count():
    df = pd.DataFrame({"a": [1, 2, 3, 4]})
    profile = DataProfiler(df).profile()
    assert profile["row_count"] == 4


def test_profile_detects_duplicate_rows():
    df = pd.DataFrame({"a": [1, 1, 2, 3], "b": ["x", "x", "y", "z"]})
    profile = DataProfiler(df).profile()
    assert profile["duplicate_rows"] == 1


def test_profile_has_zero_duplicates_when_none_exist():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    profile = DataProfiler(df).profile()
    assert profile["duplicate_rows"] == 0


def test_profile_contains_all_columns():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"], "c": [True, False]})
    profile = DataProfiler(df).profile()
    assert set(profile["columns"].keys()) == {"a", "b", "c"}


def test_profile_reports_missing_pct_per_column():
    df = pd.DataFrame({"a": [1, None, 3, None]})
    profile = DataProfiler(df).profile()
    assert profile["columns"]["a"]["missing_pct"] == 50.0


def test_profile_reports_unique_count_per_column():
    df = pd.DataFrame({"a": [1, 1, 2, 3]})
    profile = DataProfiler(df).profile()
    assert profile["columns"]["a"]["unique_count"] == 3


def test_profile_handles_empty_dataframe():
    df = pd.DataFrame({"a": []})
    profile = DataProfiler(df).profile()
    assert profile["row_count"] == 0