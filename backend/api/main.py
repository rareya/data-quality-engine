from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os

from backend.dq_engine.pipeline import DataQualityPipeline

app = FastAPI(title="Data Quality Engine API")
@app.get("/")
def root(): 
    return {"status" : "Backend is alive"}


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "Data Quality Engine API running"}

@app.post("/analyze")
def analyze_dataset(file: UploadFile = File(...)):
    file_path = f"temp_{file.filename}"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        pipeline = DataQualityPipeline(file_path)
        report = pipeline.run()
        return report
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
