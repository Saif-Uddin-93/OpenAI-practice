"""
Database loader module for customer complaints ETL
"""
import pandas as pd
import logging
from sqlalchemy import text
from typing import Dict, Tuple
from config.database import db_manager

logger = logging.getLogger(__name__)

class ComplaintDatabaseLoader:
    """Loads transformed data into PostgreSQL database"""
    
    def __init__(self):
        self.db_manager = db_manager
        
    def setup_database(self, schema_file_path: str = 'sql/schema.sql') -> bool:
        """Setup database schema"""
        logger.info("Setting up database schema")
        
        try:
            # Test connection first
            if not self.db_manager.test_connection():
                logger.error("Database connection test failed")
                return False
            
            # Execute schema file
            success = self.db_manager.execute_sql_file(schema_file_path)
            if success:
                logger.info("Database schema setup completed successfully")
            else:
                logger.error("Failed to setup database schema")
                
            return success
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            return False
    
    def get_lookup_mappings(self) -> Dict:
        """Get lookup table mappings for foreign keys"""
        logger.info("Retrieving lookup table mappings")
        
        try:
            engine = self.db_manager.get_engine()
            
            with engine.connect() as conn:
                # Get severity levels mapping
                severity_query = text("SELECT severity_id, severity_name FROM severity_levels")
                severity_result = conn.execute(severity_query)
                severity_mapping = {row[1]: row[0] for row in severity_result}
                
                # Get complaint types mapping
                types_query = text("SELECT type_id, type_name FROM complaint_types")
                types_result = conn.execute(types_query)
                types_mapping = {row[1]: row[0] for row in types_result}
                
            mappings = {
                'severity': severity_mapping,
                'complaint_types': types_mapping
            }
            
            logger.info(f"Retrieved mappings: {len(severity_mapping)} severities, {len(types_mapping)} types")
            return mappings
            
        except Exception as e:
            logger.error(f"Error retrieving lookup mappings: {e}")
            return {}
    
    def load_customers(self, customers_df: pd.DataFrame) -> Dict:
        """Load customer data into database"""
        logger.info(f"Loading {len(customers_df)} customer records")
        
        try:
            engine = self.db_manager.get_engine()
            
            # Prepare customer data for insertion
            customers_to_insert = customers_df[[
                'customer_uuid', 'age_group', 'region', 'customer_type', 'gender'
            ]].copy()
            
            # Use pandas to_sql with upsert-like behavior
            # First, try to insert new customers
            with engine.connect() as conn:
                # Check existing customers
                existing_query = text("SELECT customer_uuid FROM customers")
                existing_result = conn.execute(existing_query)
                existing_uuids = {row[0] for row in existing_result}
                
                # Filter out existing customers
                new_customers = customers_to_insert[
                    ~customers_to_insert['customer_uuid'].isin(existing_uuids)
                ]
                
                if len(new_customers) > 0:
                    # Insert new customers
                    new_customers.to_sql(
                        'customers', 
                        engine, 
                        if_exists='append', 
                        index=False,
                        method='multi'
                    )
                    logger.info(f"Inserted {len(new_customers)} new customer records")
                else:
                    logger.info("No new customers to insert")
                
                # Get customer ID mappings
                customer_query = text("SELECT customer_id, customer_uuid FROM customers")
                customer_result = conn.execute(customer_query)
                customer_id_mapping = {row[1]: row[0] for row in customer_result}
            
            return {
                'inserted': len(new_customers),
                'skipped': len(customers_to_insert) - len(new_customers),
                'customer_id_mapping': customer_id_mapping
            }
            
        except Exception as e:
            logger.error(f"Error loading customers: {e}")
            raise
    
    def load_complaints(self, complaints_df: pd.DataFrame, customer_id_mapping: Dict, lookup_mappings: Dict) -> Dict:
        """Load complaint data into database"""
        logger.info(f"Loading {len(complaints_df)} complaint records")
        
        try:
            engine = self.db_manager.get_engine()
            
            # Prepare complaints data
            complaints_to_insert = complaints_df.copy()
            
            # Map customer UUIDs to customer IDs
            complaints_to_insert['customer_id'] = complaints_to_insert['customer_uuid'].map(customer_id_mapping)
            
            # Map severity names to severity IDs
            complaints_to_insert['severity_id'] = complaints_to_insert['severity'].map(lookup_mappings['severity'])
            
            # Map complaint types to type IDs
            complaints_to_insert['type_id'] = complaints_to_insert['complaint_type'].map(lookup_mappings['complaint_types'])
            
            # Remove rows with missing mappings
            initial_count = len(complaints_to_insert)
            complaints_to_insert = complaints_to_insert.dropna(subset=['customer_id', 'severity_id', 'type_id'])
            dropped_count = initial_count - len(complaints_to_insert)
            
            if dropped_count > 0:
                logger.warning(f"Dropped {dropped_count} complaints due to missing mappings")
            
            # Select columns for database insertion
            db_columns = [
                'complaint_uuid', 'customer_id', 'type_id', 'severity_id', 
                'complaint_text', 'complaint_date'
            ]
            
            complaints_final = complaints_to_insert[db_columns].copy()
            
            with engine.connect() as conn:
                # Check existing complaints
                existing_query = text("SELECT complaint_uuid FROM complaints")
                existing_result = conn.execute(existing_query)
                existing_uuids = {row[0] for row in existing_result}
                
                # Filter out existing complaints
                new_complaints = complaints_final[
                    ~complaints_final['complaint_uuid'].isin(existing_uuids)
                ]
                
                if len(new_complaints) > 0:
                    # Insert new complaints
                    new_complaints.to_sql(
                        'complaints', 
                        engine, 
                        if_exists='append', 
                        index=False,
                        method='multi'
                    )
                    logger.info(f"Inserted {len(new_complaints)} new complaint records")
                else:
                    logger.info("No new complaints to insert")
            
            return {
                'inserted': len(new_complaints),
                'skipped': len(complaints_final) - len(new_complaints),
                'dropped': dropped_count
            }
            
        except Exception as e:
            logger.error(f"Error loading complaints: {e}")
            raise
    
    def create_sample_resolutions(self, batch_size: int = 100) -> Dict:
        """Create sample resolution records for demonstration"""
        logger.info("Creating sample resolution records")
        
        try:
            engine = self.db_manager.get_engine()
            
            with engine.connect() as conn:
                # Get some complaint IDs to create resolutions for
                query = text("""
                    SELECT complaint_id 
                    FROM complaints 
                    WHERE complaint_id NOT IN (SELECT complaint_id FROM complaint_resolutions)
                    ORDER BY RANDOM() 
                    LIMIT :batch_size
                """)
                
                result = conn.execute(query, {'batch_size': batch_size})
                complaint_ids = [row[0] for row in result]
                
                if not complaint_ids:
                    logger.info("No complaints available for resolution creation")
                    return {'created': 0}
                
                # Create resolution records
                import random
                from datetime import datetime, timedelta
                
                resolutions = []
                statuses = ['Open', 'In Progress', 'Resolved', 'Closed']
                
                for complaint_id in complaint_ids:
                    status = random.choice(statuses)
                    resolution_date = None
                    resolution_time = None
                    notes = f"Sample resolution for complaint {complaint_id}"
                    
                    if status in ['Resolved', 'Closed']:
                        # Add resolution date and time
                        resolution_date = datetime.now() - timedelta(days=random.randint(1, 30))
                        resolution_time = random.randint(1, 168)  # 1 to 168 hours (1 week)
                        notes += f" - {status} after {resolution_time} hours"
                    
                    resolutions.append({
                        'complaint_id': complaint_id,
                        'resolution_status': status,
                        'resolution_date': resolution_date,
                        'resolution_time_hours': resolution_time,
                        'resolution_notes': notes
                    })
                
                # Insert resolutions
                resolutions_df = pd.DataFrame(resolutions)
                resolutions_df.to_sql(
                    'complaint_resolutions',
                    engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                logger.info(f"Created {len(resolutions)} sample resolution records")
                
                return {'created': len(resolutions)}
                
        except Exception as e:
            logger.error(f"Error creating sample resolutions: {e}")
            return {'created': 0}
    
    def get_load_statistics(self) -> Dict:
        """Get statistics about loaded data"""
        logger.info("Retrieving load statistics")
        
        try:
            engine = self.db_manager.get_engine()
            
            with engine.connect() as conn:
                stats = {}
                
                # Customer statistics
                customer_query = text("""
                    SELECT 
                        COUNT(*) as total_customers,
                        COUNT(DISTINCT region) as unique_regions,
                        COUNT(DISTINCT customer_type) as unique_customer_types
                    FROM customers
                """)
                customer_result = conn.execute(customer_query).fetchone()
                stats['customers'] = {
                    'total': customer_result[0],
                    'unique_regions': customer_result[1],
                    'unique_customer_types': customer_result[2]
                }
                
                # Complaint statistics
                complaint_query = text("""
                    SELECT 
                        COUNT(*) as total_complaints,
                        COUNT(DISTINCT type_id) as unique_types,
                        COUNT(DISTINCT severity_id) as unique_severities,
                        MIN(complaint_date) as earliest_complaint,
                        MAX(complaint_date) as latest_complaint
                    FROM complaints
                """)
                complaint_result = conn.execute(complaint_query).fetchone()
                stats['complaints'] = {
                    'total': complaint_result[0],
                    'unique_types': complaint_result[1],
                    'unique_severities': complaint_result[2],
                    'earliest_date': str(complaint_result[3]) if complaint_result[3] else None,
                    'latest_date': str(complaint_result[4]) if complaint_result[4] else None
                }
                
                # Resolution statistics
                resolution_query = text("""
                    SELECT 
                        COUNT(*) as total_resolutions,
                        COUNT(CASE WHEN resolution_status = 'Resolved' THEN 1 END) as resolved_count,
                        AVG(resolution_time_hours) as avg_resolution_time
                    FROM complaint_resolutions
                """)
                resolution_result = conn.execute(resolution_query).fetchone()
                stats['resolutions'] = {
                    'total': resolution_result[0],
                    'resolved': resolution_result[1],
                    'avg_resolution_time_hours': float(resolution_result[2]) if resolution_result[2] else None
                }
                
            logger.info(f"Load statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error retrieving load statistics: {e}")
            return {}
    
    def load_data(self, customers_df: pd.DataFrame, complaints_df: pd.DataFrame) -> Dict:
        """Main data loading function"""
        logger.info("Starting data loading process")
        
        try:
            # Get lookup mappings
            lookup_mappings = self.get_lookup_mappings()
            if not lookup_mappings:
                raise Exception("Failed to retrieve lookup mappings")
            
            # Load customers
            customer_result = self.load_customers(customers_df)
            
            # Load complaints
            complaint_result = self.load_complaints(
                complaints_df, 
                customer_result['customer_id_mapping'], 
                lookup_mappings
            )
            
            # Create sample resolutions
            resolution_result = self.create_sample_resolutions()
            
            # Get final statistics
            stats = self.get_load_statistics()
            
            load_summary = {
                'customers': customer_result,
                'complaints': complaint_result,
                'resolutions': resolution_result,
                'final_statistics': stats,
                'loaded_at': pd.Timestamp.now().isoformat()
            }
            
            logger.info("Data loading completed successfully")
            return load_summary
            
        except Exception as e:
            logger.error(f"Error in data loading process: {e}")
            raise

def main():
    """Main function for testing database loading"""
    # This would typically load transformed data
    import json
    
    try:
        # Load sample data (assuming it exists)
        with open('data/mock_complaints.json', 'r') as f:
            raw_data = json.load(f)
        
        # Transform data first
        from src.transform.data_transformer import ComplaintDataTransformer
        transformer = ComplaintDataTransformer()
        customers_df, complaints_df, metadata = transformer.transform_dataset(raw_data)
        
        # Load data
        loader = ComplaintDatabaseLoader()
        
        # Setup database
        if loader.setup_database():
            # Load data
            load_result = loader.load_data(customers_df, complaints_df)
            print("Load result:", load_result)
        else:
            print("Failed to setup database")
            
    except FileNotFoundError:
        print("No input data found. Run data generation and transformation first.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
