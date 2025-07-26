# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Complaints ETL Pipeline - Transform Phase
# MAGIC 
# MAGIC This notebook handles the data transformation phase, cleaning and enriching
# MAGIC the extracted customer complaint data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import os
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
import re

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerComplaintsETL-Transform").getOrCreate()

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

def load_data_from_storage(blob_path):
    """Load data from Azure Storage"""
    try:
        storage_key = get_secret("storage-account-key")
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=storage_key
        )
        
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, 
            blob=blob_path
        )
        
        blob_data = blob_client.download_blob().readall()
        return json.loads(blob_data.decode('utf-8'))
        
    except Exception as e:
        logger.error(f"Failed to load data from storage: {e}")
        raise

def save_transformed_data(data, file_prefix):
    """Save transformed data to Azure Storage"""
    try:
        storage_key = get_secret("storage-account-key")
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=storage_key
        )
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        blob_name = f"transformed/{file_prefix}_{timestamp}.csv"
        
        # Convert to CSV
        csv_data = data.to_csv(index=False)
        
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, 
            blob=blob_name
        )
        blob_client.upload_blob(csv_data, overwrite=True)
        
        logger.info(f"Transformed data saved to: {blob_name}")
        return blob_name
        
    except Exception as e:
        logger.error(f"Failed to save transformed data: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Functions

# COMMAND ----------

def clean_customer_data(customers_data):
    """Clean and transform customer data"""
    logger.info("Cleaning customer data...")
    
    # Convert to DataFrame
    customers_df = pd.DataFrame(customers_data)
    
    # Data cleaning
    customers_df['email'] = customers_df['email'].str.lower().str.strip()
    customers_df['phone'] = customers_df['phone'].str.replace(r'[^\d+\-\(\)\s]', '', regex=True)
    customers_df['first_name'] = customers_df['first_name'].str.title().str.strip()
    customers_df['last_name'] = customers_df['last_name'].str.title().str.strip()
    
    # Add derived fields
    customers_df['full_name'] = customers_df['first_name'] + ' ' + customers_df['last_name']
    customers_df['email_domain'] = customers_df['email'].str.split('@').str[1]
    
    # Convert dates
    customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date'])
    customers_df['created_at'] = pd.to_datetime(customers_df['created_at'])
    
    # Add customer tenure in days
    customers_df['tenure_days'] = (datetime.now() - customers_df['registration_date']).dt.days
    
    # Add customer segment based on type and tenure
    def determine_segment(row):
        if row['customer_type'] == 'Premium':
            return 'High Value'
        elif row['customer_type'] == 'Standard' and row['tenure_days'] > 365:
            return 'Loyal Standard'
        elif row['customer_type'] == 'Standard':
            return 'Standard'
        else:
            return 'Basic'
    
    customers_df['customer_segment'] = customers_df.apply(determine_segment, axis=1)
    
    # Remove duplicates
    customers_df = customers_df.drop_duplicates(subset=['customer_uuid'])
    
    logger.info(f"Customer data cleaned: {len(customers_df)} records")
    return customers_df

def clean_complaint_data(complaints_data):
    """Clean and transform complaint data"""
    logger.info("Cleaning complaint data...")
    
    # Convert to DataFrame
    complaints_df = pd.DataFrame(complaints_data)
    
    # Data cleaning
    complaints_df['complaint_text'] = complaints_df['complaint_text'].str.strip()
    complaints_df['complaint_type'] = complaints_df['complaint_type'].str.strip()
    complaints_df['severity'] = complaints_df['severity'].str.strip()
    complaints_df['status'] = complaints_df['status'].str.strip()
    complaints_df['channel'] = complaints_df['channel'].str.strip()
    
    # Convert dates
    complaints_df['complaint_date'] = pd.to_datetime(complaints_df['complaint_date'])
    complaints_df['created_at'] = pd.to_datetime(complaints_df['created_at'])
    
    # Add derived fields
    complaints_df['complaint_length'] = complaints_df['complaint_text'].str.len()
    complaints_df['word_count'] = complaints_df['complaint_text'].str.split().str.len()
    
    # Add time-based features
    complaints_df['complaint_day_of_week'] = complaints_df['complaint_date'].dt.day_name()
    complaints_df['complaint_month'] = complaints_df['complaint_date'].dt.month_name()
    complaints_df['complaint_hour'] = complaints_df['complaint_date'].dt.hour
    complaints_df['is_weekend'] = complaints_df['complaint_date'].dt.weekday >= 5
    
    # Add urgency score based on severity and priority
    severity_scores = {'Critical': 4, 'High': 3, 'Medium': 2, 'Low': 1}
    complaints_df['severity_score'] = complaints_df['severity'].map(severity_scores)
    complaints_df['urgency_score'] = complaints_df['severity_score'] * (5 - complaints_df['priority'])
    
    # Add sentiment analysis (simplified)
    def analyze_sentiment(text):
        negative_words = ['bad', 'terrible', 'awful', 'horrible', 'worst', 'hate', 'angry', 'frustrated']
        positive_words = ['good', 'great', 'excellent', 'amazing', 'love', 'happy', 'satisfied']
        
        text_lower = text.lower()
        negative_count = sum(1 for word in negative_words if word in text_lower)
        positive_count = sum(1 for word in positive_words if word in text_lower)
        
        if negative_count > positive_count:
            return 'Negative'
        elif positive_count > negative_count:
            return 'Positive'
        else:
            return 'Neutral'
    
    complaints_df['sentiment'] = complaints_df['complaint_text'].apply(analyze_sentiment)
    
    # Add resolution time for resolved complaints
    def calculate_resolution_time(row):
        if row['status'] in ['Resolved', 'Closed']:
            # Simulate resolution time based on severity
            if row['severity'] == 'Critical':
                return np.random.normal(2, 0.5)  # 2 hours average
            elif row['severity'] == 'High':
                return np.random.normal(8, 2)    # 8 hours average
            elif row['severity'] == 'Medium':
                return np.random.normal(24, 6)   # 24 hours average
            else:
                return np.random.normal(72, 12)  # 72 hours average
        return None
    
    complaints_df['resolution_time_hours'] = complaints_df.apply(calculate_resolution_time, axis=1)
    
    # Remove duplicates
    complaints_df = complaints_df.drop_duplicates(subset=['complaint_uuid'])
    
    logger.info(f"Complaint data cleaned: {len(complaints_df)} records")
    return complaints_df

def validate_data_quality(customers_df, complaints_df):
    """Validate data quality and generate quality report"""
    logger.info("Validating data quality...")
    
    quality_report = {
        'customers': {
            'total_records': len(customers_df),
            'missing_values': customers_df.isnull().sum().to_dict(),
            'duplicates': customers_df.duplicated().sum(),
            'data_types': customers_df.dtypes.astype(str).to_dict()
        },
        'complaints': {
            'total_records': len(complaints_df),
            'missing_values': complaints_df.isnull().sum().to_dict(),
            'duplicates': complaints_df.duplicated().sum(),
            'data_types': complaints_df.dtypes.astype(str).to_dict()
        }
    }
    
    # Check referential integrity
    customer_uuids_in_customers = set(customers_df['customer_uuid'])
    customer_uuids_in_complaints = set(complaints_df['customer_uuid'])
    orphaned_complaints = customer_uuids_in_complaints - customer_uuids_in_customers
    
    quality_report['referential_integrity'] = {
        'orphaned_complaints': len(orphaned_complaints),
        'orphaned_complaint_uuids': list(orphaned_complaints)[:10]  # First 10 for brevity
    }
    
    logger.info(f"Data quality validation completed")
    return quality_report

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Transform Function

# COMMAND ----------

def transform_data(input_path):
    """Main transform function"""
    logger.info("=== TRANSFORM PHASE ===")
    transform_start_time = datetime.now()
    
    try:
        # Load raw data
        logger.info(f"Loading data from: {input_path}")
        raw_data = load_data_from_storage(input_path)
        
        # Extract customers and complaints
        customers_data = raw_data['customers']
        complaints_data = raw_data['complaints']
        
        logger.info(f"Loaded {len(customers_data)} customers and {len(complaints_data)} complaints")
        
        # Transform data
        customers_df = clean_customer_data(customers_data)
        complaints_df = clean_complaint_data(complaints_data)
        
        # Validate data quality
        quality_report = validate_data_quality(customers_df, complaints_df)
        
        # Save transformed data
        customers_output_path = save_transformed_data(customers_df, "customers_transformed")
        complaints_output_path = save_transformed_data(complaints_df, "complaints_transformed")
        
        transform_metadata = {
            'phase': 'transform',
            'status': 'success',
            'input_path': input_path,
            'customers_output_path': customers_output_path,
            'complaints_output_path': complaints_output_path,
            'records_processed': {
                'customers': len(customers_df),
                'complaints': len(complaints_df)
            },
            'quality_report': quality_report,
            'completed_at': datetime.now().isoformat(),
            'duration_seconds': (datetime.now() - transform_start_time).total_seconds()
        }
        
        logger.info(f"Transform phase completed: {transform_metadata}")
        return transform_metadata
        
    except Exception as e:
        logger.error(f"Transform phase failed: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Transform Phase

# COMMAND ----------

# Get parameters
input_path = dbutils.widgets.get("input_path") if dbutils.widgets.get("input_path") else ""

if not input_path:
    raise ValueError("input_path parameter is required")

# Run transform phase
try:
    result = transform_data(input_path)
    
    # Display summary
    print("="*50)
    print("TRANSFORM PHASE SUMMARY")
    print("="*50)
    print(f"Status: {result['status']}")
    print(f"Input Path: {result['input_path']}")
    print(f"Customers Processed: {result['records_processed']['customers']}")
    print(f"Complaints Processed: {result['records_processed']['complaints']}")
    print(f"Duration: {result['duration_seconds']:.2f} seconds")
    print(f"Customers Output: {result['customers_output_path']}")
    print(f"Complaints Output: {result['complaints_output_path']}")
    
    # Display quality report summary
    quality = result['quality_report']
    print(f"\nData Quality Summary:")
    print(f"  - Customer duplicates: {quality['customers']['duplicates']}")
    print(f"  - Complaint duplicates: {quality['complaints']['duplicates']}")
    print(f"  - Orphaned complaints: {quality['referential_integrity']['orphaned_complaints']}")
    
    print("="*50)
    
    # Return result for next phase
    dbutils.notebook.exit(json.dumps(result))
    
except Exception as e:
    error_msg = f"Transform phase failed: {str(e)}"
    print(f"ERROR: {error_msg}")
    
    error_result = {
        "phase": "transform",
        "status": "failed",
        "error": error_msg,
        "timestamp": datetime.now().isoformat()
    }
    
    dbutils.notebook.exit(json.dumps(error_result))
