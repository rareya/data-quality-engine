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
import shutil
import tempfile

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

# Allow React frontend to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health_check():
    """Simple health check endpoint."""
    return {"status": "ok", "version": "2.0.0"}


@app.post("/analyze")
async def analyze_file(file: UploadFile = File(...)):
    """
    Upload a CSV, Excel, or JSON file.
    Returns full data quality report including:
    - Quality score (0-100)
    - Rule validation results
    - EDA (correlations, distributions, outliers)
    - Actionable recommendations
    - Natural language summary
    """
    # Validate file type
    allowed_extensions = {".csv", ".xlsx", ".xls", ".json"}
    file_ext = os.path.splitext(file.filename)[1].lower()

    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{file_ext}'. "
                   f"Allowed types: {', '.join(allowed_extensions)}"
        )

    # Save uploaded file to temp location
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            delete=False,
            suffix=file_ext
        ) as tmp:
            shutil.copyfileobj(file.file, tmp)
            tmp_path = tmp.name

        # Run pipeline
        pipeline = DataQualityPipeline(tmp_path)
        report = pipeline.run()

        return JSONResponse(content=report)

    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}"
        )

    finally:
        # Always clean up temp file
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


@app.post("/analyze-sql")
async def analyze_sql(
    db_path: str = Form(...),
    table_name: str = Form(...)
):
    """
    Connect to a SQLite database and analyze a specific table.
    
    Parameters:
    - db_path: path to the .db file
    - table_name: name of the table to analyze
    
    Returns full data quality report for the specified table.
    """
    try:
        loader = SQLiteLoader(db_path)
        loader.connect()

        # Validate table exists
        tables = loader.list_tables()
        if table_name not in tables:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{table_name}' not found. "
                       f"Available tables: {tables}"
            )

        df = loader.load_table(table_name)

        pipeline = DataQualityPipelineFromDataFrame(df)
        report = pipeline.run()

        # Add SQL metadata to report
        report["source"] = {
            "type": "sqlite",
            "db_path": db_path,
            "table_name": table_name
        }

        return JSONResponse(content=report)

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"SQL analysis failed: {str(e)}"
        )


@app.get("/analyze-sql/tables")
async def list_tables(db_path: str):
    """
    Returns all table names in a SQLite database.
    Useful for populating a table selector dropdown in the frontend.
    """
    try:
        loader = SQLiteLoader(db_path)
        loader.connect()
        tables = loader.list_tables()
        return {"tables": tables, "db_path": db_path}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list tables: {str(e)}"
        )


@app.get("/demo")
async def demo():
    """
    Runs the pipeline on a built-in demo dataset.
    Great for testing the frontend without uploading a file.
    Creates a demo SQLite database with intentional quality issues.
    """
    try:
        import tempfile

        # Create demo database in temp location
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".db"
        ) as tmp:
            demo_db_path = tmp.name

        loader = SQLiteLoader.create_demo_database(demo_db_path)
        loader.connect()
        df = loader.load_table("sales")

        pipeline = DataQualityPipelineFromDataFrame(df)
        report = pipeline.run()

        report["source"] = {
            "type": "demo",
            "description": "Built-in sales dataset with intentional quality issues for demonstration"
        }

        return JSONResponse(content=report)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Demo failed: {str(e)}"
        )

    finally:
        if os.path.exists(demo_db_path):
            os.unlink(demo_db_path)


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)