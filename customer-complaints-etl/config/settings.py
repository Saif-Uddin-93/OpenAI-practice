"""
Configuration settings for the Customer Complaints ETL project
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# ETL configuration
ETL_CONFIG = {
    'batch_size': int(os.getenv('BATCH_SIZE')),
    'mock_data_size': int(os.getenv('MOCK_DATA_SIZE')),
    'output_format': os.getenv('OUTPUT_FORMAT'),  # json or csv
    'data_output_path': os.getenv('DATA_OUTPUT_PATH'),
}

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

# Logging configuration
LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': os.getenv('LOG_FILE')
}
