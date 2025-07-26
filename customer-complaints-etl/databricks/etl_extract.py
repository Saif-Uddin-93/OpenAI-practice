# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Complaints ETL Pipeline - Extract Phase
# MAGIC 
# MAGIC This notebook handles the data extraction phase, generating mock customer complaint data
# MAGIC and saving it to Azure Data Lake Storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import os
import json
import uuid
import random
import logging
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerComplaintsETL-Extract").getOrCreate()

# Configuration from Terraform template
STORAGE_ACCOUNT_NAME = "${storage_account_name}"
CONTAINER_NAME = "${container_name}"
KEY_VAULT_SCOPE = "${key_vault_scope}"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Configuration

# COMMAND ----------

# Data generation settings
DATA_GENERATION = {
    'age_groups': ['18-25', '26-35', '36-45', '46-55', '56-65', '65+'],
    'regions': ['North', 'South', 'East', 'West', 'Central'],
    'customer_types': ['Premium', 'Standard', 'Basic'],
    'genders': ['Male', 'Female', 'Other', 'Prefer not to say'],
    'complaint_types': [
        'Product Quality', 'Service Issues', 'Billing Problems', 
        'Delivery Issues', 'Technical Support', 'Account Management', 
        'Refund Requests'
    ],
    'severity_keywords': {
        'Critical': ['outage', 'breach', 'safety', 'emergency', 'urgent', 'critical'],
        'High': ['billing error', 'major', 'disruption', 'serious', 'important'],
        'Medium': ['feature', 'minor', 'improvement', 'moderate'],
        'Low': ['inquiry', 'question', 'cosmetic', 'suggestion']
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_secret(secret_name):
    """Get secret from Databricks secret scope"""
    return dbutils.secrets.get(scope=KEY_VAULT_SCOPE, key=secret_name)

def determine_severity(complaint_text):
    """Determine complaint severity based on keywords in the text"""
    complaint_lower = complaint_text.lower()
    
    for severity, keywords in DATA_GENERATION['severity_keywords'].items():
        if any(keyword in complaint_lower for keyword in keywords):
            return severity
    
    # Default to Medium if no keywords match
    return 'Medium'

def generate_complaint_text(complaint_type):
    """Generate realistic complaint text based on type"""
    templates = {
        'Product Quality': [
            f"The {fake.word()} I purchased has quality issues. {fake.sentence()}",
            f"Product defect found in {fake.word()}. {fake.sentence()}",
            f"Quality control problem with {fake.word()}. {fake.sentence()}"
        ],
        'Service Issues': [
            f"Poor customer service experience. {fake.sentence()}",
            f"Service representative was unhelpful. {fake.sentence()}",
            f"Long wait times for service. {fake.sentence()}"
        ],
        'Billing Problems': [
            f"Billing error on my account. {fake.sentence()}",
            f"Incorrect charges applied. {fake.sentence()}",
            f"Payment processing issue. {fake.sentence()}"
        ],
        'Delivery Issues': [
            f"Package delivery was delayed. {fake.sentence()}",
            f"Wrong item delivered. {fake.sentence()}",
            f"Damaged package received. {fake.sentence()}"
        ],
        'Technical Support': [
            f"Technical issue not resolved. {fake.sentence()}",
            f"Software bug affecting functionality. {fake.sentence()}",
            f"System outage causing problems. {fake.sentence()}"
        ],
        'Account Management': [
            f"Account access problems. {fake.sentence()}",
            f"Profile update issues. {fake.sentence()}",
            f"Account security concerns. {fake.sentence()}"
        ],
        'Refund Requests': [
            f"Requesting refund for {fake.word()}. {fake.sentence()}",
            f"Unsatisfied with purchase. {fake.sentence()}",
            f"Product not as described. {fake.sentence()}"
        ]
    }
    
    return random.choice(templates.get(complaint_type, [fake.sentence()]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Functions

# COMMAND ----------

def generate_customers(num_customers):
    """Generate customer data"""
    customers = []
    
    for _ in range(num_customers):
        customer = {
            'customer_uuid': str(uuid.uuid4()),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'age_group': random.choice(DATA_GENERATION['age_groups']),
            'gender': random.choice(DATA_GENERATION['genders']),
            'region': random.choice(DATA_GENERATION['regions']),
            'customer_type': random.choice(DATA_GENERATION['customer_types']),
            'registration_date': fake.date_between(start_date='-2y', end_date='today').isoformat(),
            'created_at': datetime.now().isoformat()
        }
        customers.append(customer)
    
    return customers

def generate_complaints(customers, num_complaints):
    """Generate complaint data linked to customers"""
    complaints = []
    
    for _ in range(num_complaints):
        customer = random.choice(customers)
        complaint_type = random.choice(DATA_GENERATION['complaint_types'])
        complaint_text = generate_complaint_text(complaint_type)
        severity = determine_severity(complaint_text)
        
        # Generate complaint date within last year
        complaint_date = fake.date_between(start_date='-1y', end_date='today')
        
        complaint = {
            'complaint_uuid': str(uuid.uuid4()),
            'customer_uuid': customer['customer_uuid'],
            'complaint_type': complaint_type,
            'complaint_text': complaint_text,
            'severity': severity,
            'complaint_date': complaint_date.isoformat(),
            'status': random.choice(['Open', 'In Progress', 'Resolved', 'Closed']),
            'priority': random.randint(1, 4),  # 1=Critical, 4=Low
            'channel': random.choice(['Email', 'Phone', 'Web', 'Chat', 'Social Media']),
            'created_at': datetime.now().isoformat()
        }
        complaints.append(complaint)
    
    return complaints

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Extract Function

# COMMAND ----------

def extract_data(num_records):
    """Main extract function"""
    logger.info("=== EXTRACT PHASE ===")
    extract_start_time = datetime.now()
    
    try:
        # Calculate number of customers (roughly 70% of complaints have unique customers)
        num_customers = max(1, int(num_records * 0.7))
        
        logger.info(f"Generating {num_customers} customers and {num_records} complaints")
        
        # Generate data
        customers = generate_customers(num_customers)
        complaints = generate_complaints(customers, num_records)
        
        # Create dataset
        dataset = {
            'customers': customers,
            'complaints': complaints,
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'num_customers': len(customers),
                'num_complaints': len(complaints),
                'generation_method': 'faker_mock_data'
            }
        }
        
        # Save to Azure Storage
        output_path = save_extracted_data(dataset)
        
        extract_metadata = {
            'phase': 'extract',
            'status': 'success',
            'records_generated': len(complaints),
            'customers_generated': len(customers),
            'output_path': output_path,
            'completed_at': datetime.now().isoformat(),
            'duration_seconds': (datetime.now() - extract_start_time).total_seconds()
        }
        
        logger.info(f"Extract phase completed: {extract_metadata}")
        return extract_metadata
        
    except Exception as e:
        logger.error(f"Extract phase failed: {e}")
        raise

def save_extracted_data(dataset):
    """Save extracted data to Azure Storage"""
    try:
        storage_key = get_secret("storage-account-key")
        blob_service_client = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=storage_key
        )
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save as JSON
        json_data = json.dumps(dataset, indent=2)
        json_blob_name = f"extracted/complaints_data_{timestamp}.json"
        
        json_blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME, 
            blob=json_blob_name
        )
        json_blob_client.upload_blob(json_data, overwrite=True)
        
        logger.info(f"Extracted data saved to: {json_blob_name}")
        return json_blob_name
        
    except Exception as e:
        logger.error(f"Failed to save extracted data: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Extract Phase

# COMMAND ----------

# Get parameters
num_records = int(dbutils.widgets.get("num_records") if dbutils.widgets.get("num_records") else "1000")

# Run extract phase
try:
    result = extract_data(num_records)
    
    # Display summary
    print("="*50)
    print("EXTRACT PHASE SUMMARY")
    print("="*50)
    print(f"Status: {result['status']}")
    print(f"Customers Generated: {result['customers_generated']}")
    print(f"Complaints Generated: {result['records_generated']}")
    print(f"Duration: {result['duration_seconds']:.2f} seconds")
    print(f"Output Path: {result['output_path']}")
    print("="*50)
    
    # Return result for next phase
    dbutils.notebook.exit(json.dumps(result))
    
except Exception as e:
    error_msg = f"Extract phase failed: {str(e)}"
    print(f"ERROR: {error_msg}")
    
    error_result = {
        "phase": "extract",
        "status": "failed",
        "error": error_msg,
        "timestamp": datetime.now().isoformat()
    }
    
    dbutils.notebook.exit(json.dumps(error_result))
