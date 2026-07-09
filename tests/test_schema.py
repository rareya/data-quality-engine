"""
tests/test_schema.py
---------------------
Real pytest coverage for SchemaDetector. Replaces the old print-script
version that imported the now-deleted loader.py.
"""
import pandas as pd
from backend.dq_engine.schema import SchemaDetector


def test_infers_numeric_semantic_type():
    df = pd.DataFrame({"age": [25, 30, 35, 40]})
    schema = SchemaDetector(df).infer_schema()
    assert schema["age"]["semantic_type"] == "numeric"
    assert schema["age"]["is_constant"] is False


def test_infers_categorical_semantic_type():
    df = pd.DataFrame({"city": ["Mumbai", "Delhi", "Mumbai", "Pune"]})
    schema = SchemaDetector(df).infer_schema()
    assert schema["city"]["semantic_type"] == "categorical"


def test_infers_boolean_semantic_type():
    df = pd.DataFrame({"active": [True, False, True]})
    schema = SchemaDetector(df).infer_schema()
    assert schema["active"]["semantic_type"] == "boolean"


def test_detects_constant_column():
    df = pd.DataFrame({"country": ["IN", "IN", "IN"]})
    schema = SchemaDetector(df).infer_schema()
    assert schema["country"]["is_constant"] is True
    assert schema["country"]["unique_count"] == 1


def test_missing_pct_calculated_correctly():
    df = pd.DataFrame({"col": [1, None, 3, None]})
    schema = SchemaDetector(df).infer_schema()
    assert schema["col"]["missing_pct"] == 50.0
    assert schema["col"]["missing_count"] == 2


def test_numeric_enrichment_min_max_mean():
    df = pd.DataFrame({"score": [10, 20, 30, 40]})
    schema = SchemaDetector(df).infer_schema()
    assert schema["score"]["min"] == 10.0
    assert schema["score"]["max"] == 40.0
    assert schema["score"]["mean"] == 25.0


def test_log_specific_column_named_ip_gets_semantic_type():
    df = pd.DataFrame({"ip": ["192.168.1.1", "10.0.0.1"]})
    schema = SchemaDetector(df).infer_schema()
    assert schema["ip"]["semantic_type"] == "ip_address"
    assert "invalid_ip_pct" in schema["ip"]


def test_log_specific_column_named_status_validates_http_range():
    df = pd.DataFrame({"status": [200, 404, 999, "abc"]})
    schema = SchemaDetector(df).infer_schema()
    assert schema["status"]["semantic_type"] == "http_status"
    # 999 and "abc" are both outside valid 100-599 range -> 2/4 = 50% invalid
    assert schema["status"]["invalid_status_pct"] == 50.0


def test_sample_values_capped_at_three():
    df = pd.DataFrame({"col": [1, 2, 3, 4, 5]})
    schema = SchemaDetector(df).infer_schema()
    assert len(schema["col"]["sample_values"]) == 3