print("THIS FILE IS RUNNING ####!!!!")

from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import shutil
import os

from backend.dq_engine.pipeline import DataQualityPipeline

app = FastAPI(title="Data Quality Engine API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "API running"}

@app.get("/test")
def test():
    return {"test": "OK"}

@app.post("/analyze")
def analyze_dataset(file: UploadFile = File(...)):
    file_path = f"temp_{file.filename}"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        pipeline = DataQualityPipeline(file_path)
        return pipeline.run()
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
