"""
Main ETL orchestration module for Customer Complaints ETL
"""
import sys
import os
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config.settings import LOGGING_CONFIG, ETL_CONFIG
from src.extract.data_generator import ComplaintDataGenerator
from src.transform.data_transformer import ComplaintDataTransformer
from src.load.database_loader import ComplaintDatabaseLoader

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG['level']),
    format=LOGGING_CONFIG['format'],
    handlers=[
        logging.FileHandler(LOGGING_CONFIG['file']),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class ComplaintETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self):
        self.generator = ComplaintDataGenerator()
        self.transformer = ComplaintDataTransformer()
        self.loader = ComplaintDatabaseLoader()
        self.pipeline_start_time = None
        self.pipeline_metadata = {}
        
    def extract_data(self, num_records: int = None) -> dict:
        """Extract phase - generate mock data"""
        logger.info("=== EXTRACT PHASE ===")
        
        try:
            # Generate mock data
            dataset = self.generator.generate_mock_data(num_records)
            
            # Save extracted data
            output_path = ETL_CONFIG['data_output_path'] + 'extracted_complaints'
            os.makedirs(ETL_CONFIG['data_output_path'], exist_ok=True)
            self.generator.save_data(dataset, output_path)
            
            extract_metadata = {
                'phase': 'extract',
                'status': 'success',
                'records_generated': len(dataset['complaints']),
                'customers_generated': len(dataset['customers']),
                'completed_at': datetime.now().isoformat()
            }
            
            logger.info(f"Extract phase completed: {extract_metadata}")
            return dataset, extract_metadata
            
        except Exception as e:
            logger.error(f"Extract phase failed: {e}")
            raise
    
    def transform_data(self, raw_data: dict) -> tuple:
        """Transform phase - clean and transform data"""
        logger.info("=== TRANSFORM PHASE ===")
        
        try:
            # Transform the data
            customers_df, complaints_df, transform_metadata = self.transformer.transform_dataset(raw_data)
            
            # Save transformed data
            output_dir = ETL_CONFIG['data_output_path'] + 'transformed/'
            os.makedirs(output_dir, exist_ok=True)
            
            customers_df.to_csv(output_dir + 'customers_transformed.csv', index=False)
            complaints_df.to_csv(output_dir + 'complaints_transformed.csv', index=False)
            
            transform_metadata['phase'] = 'transform'
            transform_metadata['status'] = 'success'
            
            logger.info(f"Transform phase completed: {transform_metadata}")
            return customers_df, complaints_df, transform_metadata
            
        except Exception as e:
            logger.error(f"Transform phase failed: {e}")
            raise
    
    def load_data(self, customers_df, complaints_df, setup_db: bool = True) -> dict:
        """Load phase - load data into PostgreSQL"""
        logger.info("=== LOAD PHASE ===")
        
        try:
            # Setup database schema if requested
            if setup_db:
                schema_path = str(project_root / 'sql' / 'schema.sql')
                if not self.loader.setup_database(schema_path):
                    raise Exception("Failed to setup database schema")
            
            # Load the data
            load_result = self.loader.load_data(customers_df, complaints_df)
            
            load_metadata = {
                'phase': 'load',
                'status': 'success',
                'load_result': load_result,
                'completed_at': datetime.now().isoformat()
            }
            
            logger.info(f"Load phase completed: {load_metadata}")
            return load_metadata
            
        except Exception as e:
            logger.error(f"Load phase failed: {e}")
            raise
    
    def run_full_pipeline(self, num_records: int = None, setup_db: bool = True) -> dict:
        """Run the complete ETL pipeline"""
        self.pipeline_start_time = datetime.now()
        logger.info(f"=== STARTING FULL ETL PIPELINE at {self.pipeline_start_time} ===")
        
        try:
            # Extract
            raw_data, extract_metadata = self.extract_data(num_records)
            
            # Transform
            customers_df, complaints_df, transform_metadata = self.transform_data(raw_data)
            
            # Load
            load_metadata = self.load_data(customers_df, complaints_df, setup_db)
            
            # Pipeline completion
            pipeline_end_time = datetime.now()
            pipeline_duration = (pipeline_end_time - self.pipeline_start_time).total_seconds()
            
            pipeline_summary = {
                'pipeline_status': 'SUCCESS',
                'start_time': self.pipeline_start_time.isoformat(),
                'end_time': pipeline_end_time.isoformat(),
                'duration_seconds': pipeline_duration,
                'extract_metadata': extract_metadata,
                'transform_metadata': transform_metadata,
                'load_metadata': load_metadata,
                'final_statistics': load_metadata['load_result']['final_statistics']
            }
            
            logger.info(f"=== PIPELINE COMPLETED SUCCESSFULLY in {pipeline_duration:.2f} seconds ===")
            logger.info(f"Final statistics: {pipeline_summary['final_statistics']}")
            
            return pipeline_summary
            
        except Exception as e:
            pipeline_end_time = datetime.now()
            pipeline_duration = (pipeline_end_time - self.pipeline_start_time).total_seconds()
            
            error_summary = {
                'pipeline_status': 'FAILED',
                'start_time': self.pipeline_start_time.isoformat(),
                'end_time': pipeline_end_time.isoformat(),
                'duration_seconds': pipeline_duration,
                'error': str(e)
            }
            
            logger.error(f"=== PIPELINE FAILED after {pipeline_duration:.2f} seconds ===")
            logger.error(f"Error: {e}")
            
            return error_summary
    
    def run_extract_only(self, num_records: int = None) -> dict:
        """Run only the extract phase"""
        logger.info("Running extract phase only")
        raw_data, extract_metadata = self.extract_data(num_records)
        return extract_metadata
    
    def run_transform_only(self, input_file: str) -> dict:
        """Run only the transform phase"""
        logger.info("Running transform phase only")
        
        import json
        with open(input_file, 'r') as f:
            raw_data = json.load(f)
        
        customers_df, complaints_df, transform_metadata = self.transform_data(raw_data)
        return transform_metadata
    
    def run_load_only(self, customers_file: str, complaints_file: str, setup_db: bool = True) -> dict:
        """Run only the load phase"""
        logger.info("Running load phase only")
        
        import pandas as pd
        customers_df = pd.read_csv(customers_file)
        complaints_df = pd.read_csv(complaints_file)
        
        load_metadata = self.load_data(customers_df, complaints_df, setup_db)
        return load_metadata

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description='Customer Complaints ETL Pipeline')
    parser.add_argument('--mode', choices=['full', 'extract', 'transform', 'load'], 
                       default='full', help='ETL mode to run')
    parser.add_argument('--records', type=int, default=None, 
                       help='Number of records to generate (default from config)')
    parser.add_argument('--no-setup-db', action='store_true', 
                       help='Skip database schema setup')
    parser.add_argument('--input-file', type=str, 
                       help='Input file for transform-only mode')
    parser.add_argument('--customers-file', type=str, 
                       help='Customers CSV file for load-only mode')
    parser.add_argument('--complaints-file', type=str, 
                       help='Complaints CSV file for load-only mode')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = ComplaintETLPipeline()
    
    try:
        if args.mode == 'full':
            result = pipeline.run_full_pipeline(
                num_records=args.records,
                setup_db=not args.no_setup_db
            )
            
        elif args.mode == 'extract':
            result = pipeline.run_extract_only(num_records=args.records)
            
        elif args.mode == 'transform':
            if not args.input_file:
                raise ValueError("--input-file required for transform mode")
            result = pipeline.run_transform_only(args.input_file)
            
        elif args.mode == 'load':
            if not args.customers_file or not args.complaints_file:
                raise ValueError("--customers-file and --complaints-file required for load mode")
            result = pipeline.run_load_only(
                args.customers_file, 
                args.complaints_file,
                setup_db=not args.no_setup_db
            )
        
        print("\n" + "="*50)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*50)
        print(f"Mode: {args.mode}")
        print(f"Status: {result.get('pipeline_status', 'COMPLETED')}")
        
        if 'duration_seconds' in result:
            print(f"Duration: {result['duration_seconds']:.2f} seconds")
        
        if 'final_statistics' in result:
            stats = result['final_statistics']
            print(f"Final Data Counts:")
            print(f"  - Customers: {stats.get('customers', {}).get('total', 'N/A')}")
            print(f"  - Complaints: {stats.get('complaints', {}).get('total', 'N/A')}")
            print(f"  - Resolutions: {stats.get('resolutions', {}).get('total', 'N/A')}")
        
        print("="*50)
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
