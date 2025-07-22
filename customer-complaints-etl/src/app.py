from fastapi import FastAPI, Query
from .main import ComplaintETLPipeline
from typing import Optional
import numpy as np

# Helper to convert numpy types to native Python types for FastAPI responses
def convert_numpy_types(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(v) for v in obj]
    elif isinstance(obj, np.generic):
        return obj.item()
    else:
        return obj

app = FastAPI()

pipeline = ComplaintETLPipeline()

@app.post("/run-full")
def run_full_pipeline(records: Optional[int] = Query(None, description="Number of records to generate"), setup_db: bool = Query(True, description="Whether to setup DB schema")):
    """Run the full ETL pipeline (default behaviour of main.py)"""
    result = pipeline.run_full_pipeline(num_records=records, setup_db=setup_db)
    return convert_numpy_types(result)

@app.post("/extract")
def extract(records: Optional[int] = Query(100, description="Number of records to generate")):
    result = pipeline.run_extract_only(num_records=records)
    return convert_numpy_types(result)

@app.post("/transform")
def transform(input_file: str = Query(..., description="Path to input JSON file")):
    result = pipeline.run_transform_only(input_file)
    return convert_numpy_types(result)

@app.post("/load")
def load(
    customers_file: str = Query(..., description="Path to customers CSV file"),
    complaints_file: str = Query(..., description="Path to complaints CSV file"),
    setup_db: bool = Query(True, description="Whether to setup DB schema")
):
    result = pipeline.run_load_only(customers_file, complaints_file, setup_db)
    return convert_numpy_types(result)
