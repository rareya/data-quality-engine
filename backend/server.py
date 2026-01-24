from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os

from backend.dq_engine.pipeline import DataQualityPipeline

app = FastAPI(title="Data Quality Engine API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "Backend running"}

@app.get("/test")
def test():
    return {"test": "ok"}

@app.post("/analyze")
def analyze(file: UploadFile = File(...)):
    # temp filename
    file_path = f"temp_{file.filename}"

    # save uploaded file
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        pipeline = DataQualityPipeline(file_path)
        report = pipeline.run()

        # FastAPI will auto-serialize dict/list JSON
        return report

    finally:
        # cleanup temp file
        if os.path.exists(file_path):
            os.remove(file_path)
