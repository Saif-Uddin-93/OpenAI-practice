# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Complaints ETL Pipeline - Load Phase
# MAGIC 
# MAGIC This notebook handles the data loading phase, loading transformed data
# MAGIC into Azure SQL Database with proper schema and analytics views.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import os
import json
import logging
from datetime import datetime
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerComplaintsETL-Load").getOrCreate()

# Configuration from Terraform template
STORAGE_ACCOUNT_NAME = "${storage_account_name}"
CONTAINER_NAME = "${container_name}"
KEY_VAULT_SCOPE = "${key_vault_scope}"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Database Schema

# COMMAND ----------

# SQL Schema for Azure SQL Database
DATABASE_SCHEMA = """
-- Drop existing tables if they exist
IF OBJECT_ID('complaint_resolutions', 'U') IS NOT NULL DROP TABLE complaint_resolutions;
IF OBJECT_ID('complaints', 'U') IS NOT NULL DROP TABLE complaints;
IF OBJECT_ID('customers', 'U') IS NOT NULL DROP TABLE customers;
IF OBJECT_ID('complaint_types', 'U') IS NOT NULL DROP TABLE complaint_types;
IF OBJECT_ID('severity_levels', 'U') IS NOT NULL DROP TABLE severity_levels;

-- Drop views if they exist
IF OBJECT_ID('complaint_analytics', 'V') IS NOT NULL DROP VIEW complaint_analytics;
IF OBJECT_ID('complaint_summary', 'V') IS NOT NULL DROP VIEW complaint_summary;

-- Create lookup tables
CREATE TABLE severity_levels (
    severity_id INT IDENTITY(1,1) PRIMARY KEY,
    severity_name NVARCHAR(50) NOT NULL UNIQUE,
    severity_level INT NOT NULL,
    description NVARCHAR(255),
    created_at DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE complaint_types (
    type_id INT IDENTITY(1,1) PRIMARY KEY,
    type_name NVARCHAR(100) NOT NULL UNIQUE,
    category NVARCHAR(50),
    description NVARCHAR(255),
    created_at DATETIME2 DEFAULT GETDATE()
);

-- Create main tables
CREATE TABLE customers (
    customer_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_uuid NVARCHAR(36) NOT NULL UNIQUE,
    first_name NVARCHAR(100) NOT NULL,
    last_name NVARCHAR(100) NOT NULL,
    full_name NVARCHAR(200),
    email NVARCHAR(255) NOT NULL,
    email_domain NVARCHAR(100),
    phone NVARCHAR(50),
    age_group NVARCHAR(20),
    gender NVARCHAR(50),
    region NVARCHAR(50),
    customer_type NVARCHAR(50),
    customer_segment NVARCHAR(50),
    registration_date DATE,
    tenure_days INT,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE complaints (
    complaint_id INT IDENTITY(1,1) PRIMARY KEY,
    complaint_uuid NVARCHAR(36) NOT NULL UNIQUE,
    customer_id INT NOT NULL,
    customer_uuid NVARCHAR(36) NOT NULL,
    complaint_type NVARCHAR(100) NOT NULL,
    complaint_text NVARCHAR(MAX) NOT NULL,
    complaint_length INT,
    word_count INT,
    severity NVARCHAR(50) NOT NULL,
    severity_score INT,
    priority INT NOT NULL,
    urgency_score DECIMAL(10,2),
    status NVARCHAR(50) NOT NULL,
    channel NVARCHAR(50),
    sentiment NVARCHAR(20),
    complaint_date DATE NOT NULL,
    complaint_day_of_week NVARCHAR(20),
    complaint_month NVARCHAR(20),
    complaint_hour INT,
    is_weekend BIT,
    resolution_time_hours DECIMAL(10,2),
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE complaint_resolutions (
    resolution_id INT IDENTITY(1,1) PRIMARY KEY,
    complaint_id INT NOT NULL,
    complaint_uuid NVARCHAR(36) NOT NULL,
    resolution_status NVARCHAR(50) NOT NULL,
    resolution_date DATETIME2,
    resolution_time_hours DECIMAL(10,2),
    resolver_name NVARCHAR(100),
    resolution_notes NVARCHAR(MAX),
    customer_satisfaction_score INT,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (complaint_id) REFERENCES complaints(complaint_id)
);

-- Create indexes for performance
CREATE INDEX IX_customers_uuid ON customers(customer_uuid);
CREATE INDEX IX_customers_email ON customers(email);
CREATE INDEX IX_customers_type ON customers(customer_type);
CREATE INDEX IX_customers_region ON customers(region);

CREATE INDEX IX_complaints_uuid ON complaints(complaint_uuid);
CREATE INDEX IX_complaints_customer_id ON complaints(customer_id);
CREATE INDEX IX_complaints_type ON complaints(complaint_type);
CREATE INDEX IX_complaints_severity ON complaints(severity);
CREATE INDEX IX_complaints_status ON complaints(status);
CREATE INDEX IX_complaints_date ON complaints(complaint_date);

-- Insert lookup data
INSERT INTO severity_levels (severity_name, severity_level, description) VALUES
('Critical', 1, 'Critical issues requiring immediate attention'),
('High', 2, 'High priority issues requiring prompt resolution'),
('Medium', 3, 'Medium priority issues with standard resolution time'),
('Low', 4, 'Low priority issues or general inquiries');

INSERT INTO complaint_types (type_name, category, description) VALUES
('Product Quality', 'Product', 'Issues related to product quality and defects'),
('Service Issues', 'Service', 'Problems with customer service experience'),
('Billing Problems', 'Financial', 'Billing errors and payment issues'),
('Delivery Issues', 'Logistics', 'Problems with product delivery and shipping'),
('Technical Support', 'Technical', 'Technical issues and system problems'),
('Account Management', 'Account', 'Account access and management issues'),
('Refund Requests', 'Financial', 'Requests for refunds and returns');

-- Create analytical views
CREATE VIEW complaint_analytics AS
SELECT 
    c.complaint_uuid,
    c.complaint_id,
    cust.customer_uuid,
    cust.full_name,
    cust.email,
    cust.customer_type,
    cust.customer_segment,
    cust.region,
    cust.age_group,
    cust.gender,
    c.complaint_type,
    c.complaint_text,
    c.severity,
    sl.severity_level,
    c.priority,
    c.urgency_score,
    c.status,
    c.channel,
    c.sentiment,
    c.complaint_date,
    c.complaint_day_of_week,
    c.complaint_month,
    c.is_weekend,
    c.resolution_time_hours,
    cr.resolution_status,
    cr.resolution_date,
    cr.customer_satisfaction_score,
    DATEDIFF(day, c.complaint_date, GETDATE()) as days_since_complaint
FROM complaints c
JOIN customers cust ON c.customer_id = cust.customer_id
LEFT JOIN severity_levels sl ON c.severity = sl.severity_name
LEFT JOIN complaint_resolutions cr ON c.complaint_id = cr.complaint_id;

CREATE VIEW complaint_summary AS
SELECT 
    complaint_date as complaint_day,
    COUNT(*) as total_complaints,
    COUNT(CASE WHEN severity = 'Critical' THEN 1 END) as critical_complaints,
    COUNT(CASE WHEN severity = 'High' THEN 1 END) as high_complaints,
    COUNT(CASE WHEN severity = 'Medium' THEN 1 END) as medium_complaints,
    COUNT(CASE WHEN severity = 'Low' THEN 1 END) as low_complaints,
    COUNT(CASE WHEN status = 'Resolved' THEN 1 END) as resolved_complaints,
    AVG(resolution_time_hours) as avg_resolution_hours,
    AVG(urgency_score) as avg_urgency_score
FROM complaints
GROUP BY complaint_date;
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_secret(secret_name):
    """Get secret from Databricks secret scope"""
    return dbutils.secrets.get(scope=KEY_VAULT_SCOPE, key=secret_name)

def get_database_connection():
    """Get database connection using SQLAlchemy"""
    try:
        connection_string = get_secret("sql-connection-string")
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        logger.error(f"Failed to create database connection: {e}")
        raise

def load_data_from_storage(blob_path):
    """Load CSV data from Azure Storage"""
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
        
        # Convert bytes to string and then to DataFrame
        from io import StringIO
        csv_string = blob_data.decode('utf-8')
        df = pd.read_csv(StringIO(csv_string))
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load data from storage: {e}")
        raise

def setup_database_schema(engine):
    """Setup database schema and tables"""
    logger.info("Setting up database schema...")
    
    try:
        with engine.connect() as conn:
            # Execute schema creation script
            for statement in DATABASE_SCHEMA.split('GO'):
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()
        
        logger.info("Database schema setup completed")
        return True
        
    except Exception as e:
        logger.error(f"Failed to setup database schema: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Functions

# COMMAND ----------

def load_customers_data(customers_df, engine):
    """Load customers data into database"""
    logger.info(f"Loading {len(customers_df)} customers...")
    
    try:
        # Prepare data for loading
        customers_load = customers_df.copy()
        
        # Convert datetime columns
        datetime_columns = ['registration_date', 'created_at']
        for col in datetime_columns:
            if col in customers_load.columns:
                customers_load[col] = pd.to_datetime(customers_load[col])
        
        # Load data using pandas to_sql
        customers_load.to_sql(
            'customers', 
            engine, 
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Successfully loaded {len(customers_df)} customers")
        return len(customers_df)
        
    except Exception as e:
        logger.error(f"Failed to load customers data: {e}")
        raise

def load_complaints_data(complaints_df, engine):
    """Load complaints data into database"""
    logger.info(f"Loading {len(complaints_df)} complaints...")
    
    try:
        # Get customer IDs for foreign key mapping
        with engine.connect() as conn:
            customer_mapping = pd.read_sql(
                "SELECT customer_id, customer_uuid FROM customers",
                conn
            )
        
        # Merge to get customer_id
        complaints_load = complaints_df.merge(
            customer_mapping, 
            on='customer_uuid', 
            how='left'
        )
        
        # Check for missing customer mappings
        missing_customers = complaints_load[complaints_load['customer_id'].isna()]
        if len(missing_customers) > 0:
            logger.warning(f"Found {len(missing_customers)} complaints with missing customer mappings")
            complaints_load = complaints_load.dropna(subset=['customer_id'])
        
        # Convert datetime columns
        datetime_columns = ['complaint_date', 'created_at']
        for col in datetime_columns:
            if col in complaints_load.columns:
                complaints_load[col] = pd.to_datetime(complaints_load[col])
        
        # Convert boolean columns
        if 'is_weekend' in complaints_load.columns:
            complaints_load['is_weekend'] = complaints_load['is_weekend'].astype(bool)
        
        # Load data
        complaints_load.to_sql(
            'complaints', 
            engine, 
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Successfully loaded {len(complaints_load)} complaints")
        return len(complaints_load)
        
    except Exception as e:
        logger.error(f"Failed to load complaints data: {e}")
        raise

def generate_complaint_resolutions(engine):
    """Generate complaint resolution data for resolved complaints"""
    logger.info("Generating complaint resolution data...")
    
    try:
        with engine.connect() as conn:
            # Get resolved complaints
            resolved_complaints = pd.read_sql("""
                SELECT complaint_id, complaint_uuid, status, resolution_time_hours
                FROM complaints 
                WHERE status IN ('Resolved', 'Closed')
            """, conn)
            
            if len(resolved_complaints) == 0:
                logger.info("No resolved complaints found")
                return 0
            
            # Generate resolution data
            resolutions = []
            for _, complaint in resolved_complaints.iterrows():
                resolution = {
                    'complaint_id': complaint['complaint_id'],
                    'complaint_uuid': complaint['complaint_uuid'],
                    'resolution_status': complaint['status'],
                    'resolution_date': datetime.now(),
                    'resolution_time_hours': complaint['resolution_time_hours'],
                    'resolver_name': f"Agent_{complaint['complaint_id'] % 10 + 1}",
                    'resolution_notes': f"Complaint resolved successfully",
                    'customer_satisfaction_score': np.random.randint(3, 6),  # 3-5 rating
                    'created_at': datetime.now()
                }
                resolutions.append(resolution)
            
            # Load resolutions
            resolutions_df = pd.DataFrame(resolutions)
            resolutions_df.to_sql(
                'complaint_resolutions',
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Generated {len(resolutions)} complaint resolutions")
            return len(resolutions)
            
    except Exception as e:
        logger.error(f"Failed to generate complaint resolutions: {e}")
        raise

def get_final_statistics(engine):
    """Get final statistics from loaded data"""
    logger.info("Collecting final statistics...")
    
    try:
        with engine.connect() as conn:
            stats = {}
            
            # Customer statistics
            customer_stats = pd.read_sql("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT customer_type) as unique_types,
                    COUNT(DISTINCT region) as unique_regions
                FROM customers
            """, conn).iloc[0].to_dict()
            stats['customers'] = customer_stats
            
            # Complaint statistics
            complaint_stats = pd.read_sql("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT complaint_type) as unique_types,
                    COUNT(DISTINCT severity) as unique_severities,
                    AVG(CAST(resolution_time_hours as FLOAT)) as avg_resolution_hours
                FROM complaints
            """, conn).iloc[0].to_dict()
            stats['complaints'] = complaint_stats
            
            # Resolution statistics
            resolution_stats = pd.read_sql("""
                SELECT 
                    COUNT(*) as total,
                    AVG(CAST(customer_satisfaction_score as FLOAT)) as avg_satisfaction
                FROM complaint_resolutions
            """, conn).iloc[0].to_dict()
            stats['resolutions'] = resolution_stats
            
            return stats
            
    except Exception as e:
        logger.error(f"Failed to collect final statistics: {e}")
        return {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Load Function

# COMMAND ----------

def load_data(customers_path, complaints_path):
    """Main load function"""
    logger.info("=== LOAD PHASE ===")
    load_start_time = datetime.now()
    
    try:
        # Get database connection
        engine = get_database_connection()
        
        # Setup database schema
        setup_database_schema(engine)
        
        # Load data from storage
        logger.info(f"Loading customers from: {customers_path}")
        customers_df = load_data_from_storage(customers_path)
        
        logger.info(f"Loading complaints from: {complaints_path}")
        complaints_df = load_data_from_storage(complaints_path)
        
        # Load data into database
        customers_loaded = load_customers_data(customers_df, engine)
        complaints_loaded = load_complaints_data(complaints_df, engine)
        resolutions_generated = generate_complaint_resolutions(engine)
        
        # Get final statistics
        final_statistics = get_final_statistics(engine)
        
        load_metadata = {
            'phase': 'load',
            'status': 'success',
            'customers_path': customers_path,
            'complaints_path': complaints_path,
            'records_loaded': {
                'customers': customers_loaded,
                'complaints': complaints_loaded,
                'resolutions': resolutions_generated
            },
            'final_statistics': final_statistics,
            'completed_at': datetime.now().isoformat(),
            'duration_seconds': (datetime.now() - load_start_time).total_seconds()
        }
        
        logger.info(f"Load phase completed: {load_metadata}")
        return load_metadata
        
    except Exception as e:
        logger.error(f"Load phase failed: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Load Phase

# COMMAND ----------

# Get parameters
customers_path = dbutils.widgets.get("customers_path") if dbutils.widgets.get("customers_path") else ""
complaints_path = dbutils.widgets.get("complaints_path") if dbutils.widgets.get("complaints_path") else ""

if not customers_path or not complaints_path:
    raise ValueError("Both customers_path and complaints_path parameters are required")

# Run load phase
try:
    result = load_data(customers_path, complaints_path)
    
    # Display summary
    print("="*50)
    print("LOAD PHASE SUMMARY")
    print("="*50)
    print(f"Status: {result['status']}")
    print(f"Duration: {result['duration_seconds']:.2f} seconds")
    
    records = result['records_loaded']
    print(f"\nRecords Loaded:")
    print(f"  - Customers: {records['customers']}")
    print(f"  - Complaints: {records['complaints']}")
    print(f"  - Resolutions: {records['resolutions']}")
    
    if 'final_statistics' in result:
        stats = result['final_statistics']
        print(f"\nFinal Database Statistics:")
        for table, table_stats in stats.items():
            if isinstance(table_stats, dict) and 'total' in table_stats:
                print(f"  - {table.title()}: {table_stats['total']} records")
    
    print("="*50)
    
    # Return result
    dbutils.notebook.exit(json.dumps(result))
    
except Exception as e:
    error_msg = f"Load phase failed: {str(e)}"
    print(f"ERROR: {error_msg}")
    
    error_result = {
        "phase": "load",
        "status": "failed",
        "error": error_msg,
        "timestamp": datetime.now().isoformat()
    }
    
    dbutils.notebook.exit(json.dumps(error_result))
