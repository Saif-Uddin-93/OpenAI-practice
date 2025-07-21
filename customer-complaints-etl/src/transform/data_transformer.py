"""
Data transformation module for customer complaints ETL
"""
import pandas as pd
import logging
import re
from datetime import datetime
from typing import Dict, List, Tuple
from config.settings import DATA_GENERATION

logger = logging.getLogger(__name__)

class ComplaintDataTransformer:
    """Transforms raw complaint data according to business rules"""
    
    def __init__(self):
        self.config = DATA_GENERATION
        self.severity_mapping = {
            'Critical': 1,
            'High': 2,
            'Medium': 3,
            'Low': 4
        }
        
    def clean_text_data(self, text: str) -> str:
        """Clean and standardize text data"""
        if pd.isna(text) or text is None:
            return ""
        
        # Convert to string and strip whitespace
        text = str(text).strip()
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\.\,\!\?\-\']', '', text)
        
        return text
    
    def standardize_dates(self, date_str: str) -> datetime:
        """Standardize date formats"""
        if pd.isna(date_str) or date_str is None:
            return None
            
        try:
            # Handle ISO format
            if 'T' in str(date_str):
                return pd.to_datetime(date_str)
            else:
                return pd.to_datetime(date_str)
        except Exception as e:
            logger.warning(f"Could not parse date: {date_str}, error: {e}")
            return None
    
    def categorize_severity(self, complaint_text: str, current_severity: str = None) -> str:
        """Categorize complaint severity based on text analysis"""
        if pd.isna(complaint_text) or complaint_text is None:
            return current_severity or 'Low'
        
        text_lower = complaint_text.lower()
        
        # Check for severity keywords
        for severity, keywords in self.config['severity_keywords'].items():
            if any(keyword.lower() in text_lower for keyword in keywords):
                return severity
        
        # If no keywords found, return current severity or default
        return current_severity or 'Medium'
    
    def normalize_demographics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize demographic data"""
        df_normalized = df.copy()
        
        # Standardize age groups
        age_group_mapping = {
            '18-25': '18-25',
            '26-35': '26-35', 
            '36-45': '36-45',
            '46-55': '46-55',
            '56-65': '56-65',
            '65+': '65+',
            '65 and above': '65+',
            'senior': '65+',
            'young adult': '18-25',
            'middle aged': '36-45'
        }
        
        df_normalized['age_group'] = df_normalized['age_group'].map(
            lambda x: age_group_mapping.get(str(x).lower(), x) if pd.notna(x) else 'Unknown'
        )
        
        # Standardize regions
        region_mapping = {
            'north': 'North',
            'south': 'South',
            'east': 'East',
            'west': 'West',
            'central': 'Central',
            'northeast': 'North',
            'northwest': 'North',
            'southeast': 'South',
            'southwest': 'South'
        }
        
        df_normalized['region'] = df_normalized['region'].map(
            lambda x: region_mapping.get(str(x).lower(), str(x).title()) if pd.notna(x) else 'Unknown'
        )
        
        # Standardize customer types
        customer_type_mapping = {
            'premium': 'Premium',
            'standard': 'Standard',
            'basic': 'Basic',
            'vip': 'Premium',
            'regular': 'Standard',
            'free': 'Basic'
        }
        
        df_normalized['customer_type'] = df_normalized['customer_type'].map(
            lambda x: customer_type_mapping.get(str(x).lower(), str(x).title()) if pd.notna(x) else 'Standard'
        )
        
        # Standardize gender
        gender_mapping = {
            'male': 'Male',
            'm': 'Male',
            'female': 'Female',
            'f': 'Female',
            'other': 'Other',
            'non-binary': 'Other',
            'prefer not to say': 'Prefer not to say',
            'unknown': 'Prefer not to say'
        }
        
        df_normalized['gender'] = df_normalized['gender'].map(
            lambda x: gender_mapping.get(str(x).lower(), str(x).title()) if pd.notna(x) else 'Prefer not to say'
        )
        
        return df_normalized
    
    def create_derived_fields(self, complaints_df: pd.DataFrame) -> pd.DataFrame:
        """Create derived fields for analysis"""
        df_enhanced = complaints_df.copy()
        
        # Convert complaint_date to datetime if not already
        df_enhanced['complaint_date'] = pd.to_datetime(df_enhanced['complaint_date'])
        
        # Add time-based derived fields
        df_enhanced['complaint_year'] = df_enhanced['complaint_date'].dt.year
        df_enhanced['complaint_month'] = df_enhanced['complaint_date'].dt.month
        df_enhanced['complaint_day_of_week'] = df_enhanced['complaint_date'].dt.day_name()
        df_enhanced['complaint_hour'] = df_enhanced['complaint_date'].dt.hour
        
        # Add complaint age in days
        current_date = datetime.now()
        df_enhanced['complaint_age_days'] = (current_date - df_enhanced['complaint_date']).dt.days
        
        # Add text analysis fields
        df_enhanced['complaint_text_length'] = df_enhanced['complaint_text'].str.len()
        df_enhanced['complaint_word_count'] = df_enhanced['complaint_text'].str.split().str.len()
        
        # Add severity numeric mapping
        df_enhanced['severity_numeric'] = df_enhanced['severity'].map(self.severity_mapping)
        
        # Add priority flag (Critical and High are high priority)
        df_enhanced['is_high_priority'] = df_enhanced['severity'].isin(['Critical', 'High'])
        
        return df_enhanced
    
    def validate_data_quality(self, customers_df: pd.DataFrame, complaints_df: pd.DataFrame) -> Dict:
        """Validate data quality and return quality metrics"""
        quality_report = {
            'customers': {
                'total_records': len(customers_df),
                'missing_values': customers_df.isnull().sum().to_dict(),
                'duplicate_uuids': customers_df['customer_uuid'].duplicated().sum(),
                'invalid_age_groups': len(customers_df[~customers_df['age_group'].isin(self.config['age_groups'] + ['Unknown'])]),
                'invalid_regions': len(customers_df[~customers_df['region'].isin(self.config['regions'] + ['Unknown'])]),
            },
            'complaints': {
                'total_records': len(complaints_df),
                'missing_values': complaints_df.isnull().sum().to_dict(),
                'duplicate_uuids': complaints_df['complaint_uuid'].duplicated().sum(),
                'invalid_severities': len(complaints_df[~complaints_df['severity'].isin(['Critical', 'High', 'Medium', 'Low'])]),
                'invalid_types': len(complaints_df[~complaints_df['complaint_type'].isin(self.config['complaint_types'])]),
                'future_dates': len(complaints_df[complaints_df['complaint_date'] > datetime.now()]),
            }
        }
        
        # Log quality issues
        for table, metrics in quality_report.items():
            logger.info(f"Data quality for {table}: {metrics}")
            
        return quality_report
    
    def transform_dataset(self, raw_data: Dict) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
        """Main transformation function"""
        logger.info("Starting data transformation process")
        
        # Convert to DataFrames
        customers_df = pd.DataFrame(raw_data['customers'])
        complaints_df = pd.DataFrame(raw_data['complaints'])
        
        logger.info(f"Input data: {len(customers_df)} customers, {len(complaints_df)} complaints")
        
        # Clean text data
        complaints_df['complaint_text'] = complaints_df['complaint_text'].apply(self.clean_text_data)
        
        # Standardize dates
        complaints_df['complaint_date'] = complaints_df['complaint_date'].apply(self.standardize_dates)
        
        # Re-categorize severity based on text analysis
        complaints_df['severity'] = complaints_df.apply(
            lambda row: self.categorize_severity(row['complaint_text'], row['severity']), 
            axis=1
        )
        
        # Normalize demographic data
        customers_df = self.normalize_demographics(customers_df)
        
        # Create derived fields
        complaints_df = self.create_derived_fields(complaints_df)
        
        # Remove records with critical missing data
        initial_complaint_count = len(complaints_df)
        complaints_df = complaints_df.dropna(subset=['complaint_uuid', 'customer_uuid', 'complaint_text', 'complaint_date'])
        
        initial_customer_count = len(customers_df)
        customers_df = customers_df.dropna(subset=['customer_uuid'])
        
        # Remove complaints for customers that don't exist
        valid_customer_uuids = set(customers_df['customer_uuid'])
        complaints_df = complaints_df[complaints_df['customer_uuid'].isin(valid_customer_uuids)]
        
        logger.info(f"Removed {initial_complaint_count - len(complaints_df)} invalid complaint records")
        logger.info(f"Removed {initial_customer_count - len(customers_df)} invalid customer records")
        
        # Validate data quality
        quality_report = self.validate_data_quality(customers_df, complaints_df)
        
        # Add transformation metadata
        transformation_metadata = {
            'transformed_at': datetime.now().isoformat(),
            'transformer_version': '1.0',
            'input_customers': len(raw_data['customers']),
            'input_complaints': len(raw_data['complaints']),
            'output_customers': len(customers_df),
            'output_complaints': len(complaints_df),
            'quality_report': quality_report
        }
        
        logger.info("Data transformation completed successfully")
        
        return customers_df, complaints_df, transformation_metadata

def main():
    """Main function for testing transformation"""
    # This would typically load data from the extract phase
    import json
    
    # Load sample data (assuming it exists)
    try:
        with open('data/mock_complaints.json', 'r') as f:
            raw_data = json.load(f)
        
        transformer = ComplaintDataTransformer()
        customers_df, complaints_df, metadata = transformer.transform_dataset(raw_data)
        
        print(f"Transformed {len(customers_df)} customers and {len(complaints_df)} complaints")
        print("Transformation metadata:", metadata)
        
    except FileNotFoundError:
        print("No input data found. Run data generation first.")

if __name__ == "__main__":
    main()
