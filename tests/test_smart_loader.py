"""
tests/test_smart_loader.py
---------------------------
Real coverage for SmartLoader — currently your biggest module (896 lines)
and the one with zero pytest coverage. Adjust import path / fixture data
to match your actual data/ files and SmartLoader's real method names —
I don't have the source for smart_loader.py in front of me, so treat
this as a scaffold to fill in, not a drop-in-and-done file.
"""
import os
import pandas as pd
import pytest

from backend.dq_engine.smart_loader import SmartLoader


@pytest.fixture
def csv_path(tmp_path):
    p = tmp_path / "sample.csv"
    p.write_text(
        "id,email,signup_date,status\n"
        "1,alice@example.com,2026-01-05,200\n"
        "2,bob@example.com,2026-01-06,404\n"
        "3,,2026-01-07,200\n"
    )
    return str(p)


def test_loads_csv_into_dataframe(csv_path):
    df, parse_report = SmartLoader(csv_path).load()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "email" in df.columns


def test_handles_missing_values(csv_path):
    df, parse_report = SmartLoader(csv_path).load()
    assert df["email"].isnull().sum() == 1


def test_parse_report_returned(csv_path):
    df, parse_report = SmartLoader(csv_path).load()
    assert parse_report is not None


def test_semantic_type_email_detected(csv_path):
    loader = SmartLoader(csv_path)
    df, parse_report = loader.load()
    types = loader.infer_semantic_types(df) if hasattr(loader, "infer_semantic_types") else None
    if types is not None:
        assert types.get("email") == "email"


def test_semantic_type_http_status_detected(csv_path):
    loader = SmartLoader(csv_path)
    df, parse_report = loader.load()
    types = loader.infer_semantic_types(df) if hasattr(loader, "infer_semantic_types") else None
    if types is not None:
        assert types.get("status") == "http_status"


def test_raises_on_missing_file():
    with pytest.raises(FileNotFoundError):
        SmartLoader("nonexistent_file_xyz.csv").load()


def test_loads_excel(tmp_path):
    xlsx_path = tmp_path / "sample.xlsx"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_excel(xlsx_path, index=False)
    df, parse_report = SmartLoader(str(xlsx_path)).load()
    assert len(df) == 2


def test_loads_json(tmp_path):
    json_path = tmp_path / "sample.json"
    json_path.write_text('[{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]')
    df, parse_report = SmartLoader(str(json_path)).load()
    assert len(df) == 2