"""
main.py
-------
FastAPI server for the Data Quality Engine.

Endpoints:
- POST /analyze        — upload CSV/Excel/JSON file → get full quality report
- POST /analyze-sql    — connect to SQLite DB → analyze a table
- GET  /health         — health check
- GET  /demo           — run analysis on built-in demo dataset
"""

import os
import math
import shutil
import tempfile
import uuid
import numpy as np

from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.dq_engine.pipeline import DataQualityPipeline, DataQualityPipelineFromDataFrame
from backend.dq_engine.sql_loader import SQLiteLoader

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Data Quality Engine API",
    description="Automated data quality profiling, validation, scoring and EDA.",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── JSON sanitizer ────────────────────────────────────────────────────────────

def clean_for_json(obj):
    """
    Recursively sanitizes a report dict for JSON serialization.
    Handles NaN, Infinity, numpy types — all of which break JSON.
    Industry standard: replace non-serializable values with None.
    """
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, np.floating):
        val = float(obj)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.ndarray):
        return clean_for_json(obj.tolist())
    elif isinstance(obj, np.bool_):
        return bool(obj)
    return obj


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    return {"status": "ok", "version": "2.0.0"}


@app.post("/analyze")
async def analyze_file(file: UploadFile = File(...)):
    """
    Upload a CSV, Excel, or JSON file.
    Returns full data quality report.
    """
    allowed_extensions = {".csv", ".xlsx", ".xls", ".json", ".log", ".txt"}
    file_ext = os.path.splitext(file.filename)[1].lower()

    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{file_ext}'. "
                   f"Allowed: {', '.join(allowed_extensions)}"
        )

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name

        pipeline = DataQualityPipeline(tmp_path)
        report = pipeline.run()

        return JSONResponse(content=clean_for_json(report))

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


@app.post("/analyze-sql")
async def analyze_sql(
    db_path: str = Form(...),
    table_name: str = Form(...)
):
    """
    Connect to a SQLite database and analyze a specific table.
    """
    try:
        loader = SQLiteLoader(db_path)
        loader.connect()

        tables = loader.list_tables()
        if table_name not in tables:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{table_name}' not found. Available: {tables}"
            )

        df = loader.load_table(table_name)
        pipeline = DataQualityPipelineFromDataFrame(df)
        report = pipeline.run()

        report["source"] = {
            "type": "sqlite",
            "db_path": db_path,
            "table_name": table_name
        }

        return JSONResponse(content=clean_for_json(report))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL analysis failed: {str(e)}")


@app.get("/analyze-sql/tables")
async def list_tables(db_path: str):
    """Returns all table names in a SQLite database."""
    try:
        loader = SQLiteLoader(db_path)
        loader.connect()
        tables = loader.list_tables()
        return {"tables": tables, "db_path": db_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")


@app.get("/demo")
async def demo():
    """
    Runs pipeline on built-in demo dataset with intentional quality issues.
    """
    demo_db_path = None
    try:
        # WINDOWS FIX: previously used tempfile.NamedTemporaryFile(...) here,
        # which opens and holds an OS-level file handle on Windows even after
        # the `with` block exits. sqlite3.connect() inside
        # create_demo_database() would then fail with WinError 32
        # ("process cannot access the file") because Windows still considered
        # it locked. Generating a unique path in the temp directory WITHOUT
        # ever opening a handle on it avoids the lock entirely — nothing is
        # holding the file, so SQLite can create and write it freely.
        # This works identically on Linux/Mac, where the original code never
        # actually failed (Windows allows the OS to be stricter about locks).
        demo_db_path = os.path.join(
            tempfile.gettempdir(), f"demo_{uuid.uuid4().hex}.db"
        )

        loader = SQLiteLoader.create_demo_database(demo_db_path)
        loader.connect()
        df = loader.load_table("sales")

        # WINDOWS FIX: SQLAlchemy's engine keeps a pooled connection open to
        # the file even after load_table() returns. On Windows this holds
        # the same kind of OS-level lock as before — os.unlink() below would
        # fail with WinError 32 unless the engine is explicitly disposed
        # first, releasing all pooled connections.
        if loader.engine is not None:
            loader.engine.dispose()

        pipeline = DataQualityPipelineFromDataFrame(df)
        report = pipeline.run()

        report["source"] = {
            "type": "demo",
            "description": "Built-in sales dataset with intentional quality issues"
        }

        return JSONResponse(content=clean_for_json(report))

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Demo failed: {str(e)}")
    finally:
        if demo_db_path and os.path.exists(demo_db_path):
            try:
                os.unlink(demo_db_path)
            except PermissionError:
                # Best-effort cleanup — if the OS still hasn't released the
                # handle, leave the temp file behind rather than crash the
                # request. It's in the OS temp dir and will be cleaned up
                # eventually; not worth failing the response over.
                pass


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)