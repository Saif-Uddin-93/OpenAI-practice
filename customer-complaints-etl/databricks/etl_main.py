# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Complaints ETL Pipeline - Main Orchestrator
# MAGIC 
# MAGIC This notebook orchestrates the complete ETL pipeline for customer complaints data processing.
# MAGIC It coordinates the extract, transform, and load phases using Azure services.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import os
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerComplaintsETL").getOrCreate()

# Configuration from Terraform template
STORAGE_ACCOUNT_NAME = "${storage_account_name}"
CONTAINER_NAME = "${container_name}"
KEY_VAULT_SCOPE = "${key_vault_scope}"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_secret(secret_name):
    """Get secret from Databricks secret scope"""
    return dbutils.secrets.get(scope=KEY_VAULT_SCOPE, key=secret_name)

def log_phase_start(phase_name):
    """Log the start of an ETL phase"""
    timestamp = datetime.now().isoformat()
    logger.info(f"=== {phase_name.upper()} PHASE STARTED at {timestamp} ===")
    return timestamp

def log_phase_end(phase_name, start_time, metadata=None):
    """Log the end of an ETL phase"""
    end_time = datetime.now().isoformat()
    logger.info(f"=== {phase_name.upper()} PHASE COMPLETED at {end_time} ===")
    if metadata:
        logger.info(f"Phase metadata: {metadata}")
    return end_time

def save_metadata(phase, metadata, path_suffix=""):
    """Save phase metadata to storage"""
    try:
        storage_key = get_secret("storage-account-key")
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=storage_key
        )
        
        metadata_json = json.dumps(metadata, indent=2)
        blob_name = f"metadata/{phase}_metadata{path_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, 
            blob=blob_name
        )
        blob_client.upload_blob(metadata_json, overwrite=True)
        
        logger.info(f"Metadata saved to: {blob_name}")
        return blob_name
    except Exception as e:
        logger.error(f"Failed to save metadata: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Pipeline Orchestration

# COMMAND ----------

def run_etl_pipeline(num_records=1000):
    """
    Run the complete ETL pipeline
    """
    pipeline_start_time = datetime.now()
    logger.info(f"=== STARTING CUSTOMER COMPLAINTS ETL PIPELINE at {pipeline_start_time} ===")
    
    pipeline_metadata = {
        "pipeline_start_time": pipeline_start_time.isoformat(),
        "num_records": num_records,
        "phases": {}
    }
    
    try:
        # Phase 1: Extract
        extract_start = log_phase_start("extract")
        
        # Run extract notebook
        extract_result = dbutils.notebook.run(
            "/etl/extract", 
            timeout_seconds=3600, 
            arguments={"num_records": str(num_records)}
        )
        extract_metadata = json.loads(extract_result)
        
        extract_end = log_phase_end("extract", extract_start, extract_metadata)
        pipeline_metadata["phases"]["extract"] = {
            "start_time": extract_start,
            "end_time": extract_end,
            "metadata": extract_metadata
        }
        
        # Phase 2: Transform
        transform_start = log_phase_start("transform")
        
        # Run transform notebook
        transform_result = dbutils.notebook.run(
            "/etl/transform", 
            timeout_seconds=3600,
            arguments={"input_path": extract_metadata.get("output_path", "")}
        )
        transform_metadata = json.loads(transform_result)
        
        transform_end = log_phase_end("transform", transform_start, transform_metadata)
        pipeline_metadata["phases"]["transform"] = {
            "start_time": transform_start,
            "end_time": transform_end,
            "metadata": transform_metadata
        }
        
        # Phase 3: Load
        load_start = log_phase_start("load")
        
        # Run load notebook
        load_result = dbutils.notebook.run(
            "/etl/load", 
            timeout_seconds=3600,
            arguments={
                "customers_path": transform_metadata.get("customers_output_path", ""),
                "complaints_path": transform_metadata.get("complaints_output_path", "")
            }
        )
        load_metadata = json.loads(load_result)
        
        load_end = log_phase_end("load", load_start, load_metadata)
        pipeline_metadata["phases"]["load"] = {
            "start_time": load_start,
            "end_time": load_end,
            "metadata": load_metadata
        }
        
        # Pipeline completion
        pipeline_end_time = datetime.now()
        pipeline_duration = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        pipeline_metadata.update({
            "pipeline_end_time": pipeline_end_time.isoformat(),
            "pipeline_duration_seconds": pipeline_duration,
            "pipeline_status": "SUCCESS",
            "final_statistics": load_metadata.get("final_statistics", {})
        })
        
        # Save pipeline metadata
        save_metadata("pipeline", pipeline_metadata)
        
        logger.info(f"=== PIPELINE COMPLETED SUCCESSFULLY in {pipeline_duration:.2f} seconds ===")
        logger.info(f"Final statistics: {pipeline_metadata['final_statistics']}")
        
        return pipeline_metadata
        
    except Exception as e:
        pipeline_end_time = datetime.now()
        pipeline_duration = (pipeline_end_time - pipeline_start_time).total_seconds()
        
        error_metadata = {
            "pipeline_start_time": pipeline_start_time.isoformat(),
            "pipeline_end_time": pipeline_end_time.isoformat(),
            "pipeline_duration_seconds": pipeline_duration,
            "pipeline_status": "FAILED",
            "error": str(e),
            "phases": pipeline_metadata.get("phases", {})
        }
        
        # Save error metadata
        save_metadata("pipeline_error", error_metadata)
        
        logger.error(f"=== PIPELINE FAILED after {pipeline_duration:.2f} seconds ===")
        logger.error(f"Error: {e}")
        
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline

# COMMAND ----------

# Get parameters from job or use defaults
num_records = int(dbutils.widgets.get("num_records") if dbutils.widgets.get("num_records") else "1000")

# Run the ETL pipeline
try:
    result = run_etl_pipeline(num_records=num_records)
    
    # Display summary
    print("="*60)
    print("ETL PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Status: {result['pipeline_status']}")
    print(f"Duration: {result['pipeline_duration_seconds']:.2f} seconds")
    print(f"Records Processed: {num_records}")
    
    if 'final_statistics' in result:
        stats = result['final_statistics']
        print(f"\nFinal Data Counts:")
        for table, count_info in stats.items():
            if isinstance(count_info, dict) and 'total' in count_info:
                print(f"  - {table.title()}: {count_info['total']}")
    
    print("="*60)
    
    # Return result for downstream processing
    dbutils.notebook.exit(json.dumps(result))
    
except Exception as e:
    error_msg = f"Pipeline execution failed: {str(e)}"
    print(f"ERROR: {error_msg}")
    
    # Return error result
    error_result = {
        "pipeline_status": "FAILED",
        "error": error_msg,
        "timestamp": datetime.now().isoformat()
    }
    
    dbutils.notebook.exit(json.dumps(error_result))
