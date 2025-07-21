"""
Simple test script for Customer Complaints ETL
"""
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

def test_imports():
    """Test that all modules can be imported"""
    print("Testing imports...")
    
    try:
        from src.extract.data_generator import ComplaintDataGenerator
        print("✓ Extract module imported successfully")
        
        from src.transform.data_transformer import ComplaintDataTransformer
        print("✓ Transform module imported successfully")
        
        from src.load.database_loader import ComplaintDatabaseLoader
        print("✓ Load module imported successfully")
        
        from src.main import ComplaintETLPipeline
        print("✓ Main pipeline imported successfully")
        
        from config.settings import DATABASE_CONFIG, ETL_CONFIG
        print("✓ Configuration imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_data_generation():
    """Test data generation functionality"""
    print("\nTesting data generation...")
    
    try:
        from src.extract.data_generator import ComplaintDataGenerator
        
        generator = ComplaintDataGenerator()
        dataset = generator.generate_mock_data(10)  # Generate 10 records
        
        assert 'customers' in dataset
        assert 'complaints' in dataset
        assert 'metadata' in dataset
        assert len(dataset['complaints']) == 10
        
        print(f"✓ Generated {len(dataset['customers'])} customers and {len(dataset['complaints'])} complaints")
        return True
        
    except Exception as e:
        print(f"✗ Data generation failed: {e}")
        return False

def test_data_transformation():
    """Test data transformation functionality"""
    print("\nTesting data transformation...")
    
    try:
        from src.extract.data_generator import ComplaintDataGenerator
        from src.transform.data_transformer import ComplaintDataTransformer
        
        # Generate test data
        generator = ComplaintDataGenerator()
        dataset = generator.generate_mock_data(5)
        
        # Transform data
        transformer = ComplaintDataTransformer()
        customers_df, complaints_df, metadata = transformer.transform_dataset(dataset)
        
        assert len(customers_df) > 0
        assert len(complaints_df) > 0
        assert 'transformed_at' in metadata
        
        print(f"✓ Transformed {len(customers_df)} customers and {len(complaints_df)} complaints")
        return True
        
    except Exception as e:
        print(f"✗ Data transformation failed: {e}")
        return False

def test_configuration():
    """Test configuration loading"""
    print("\nTesting configuration...")
    
    try:
        from config.settings import DATABASE_CONFIG, ETL_CONFIG, DATA_GENERATION
        
        # Check required config keys
        required_db_keys = ['host', 'port', 'database', 'user', 'password']
        for key in required_db_keys:
            assert key in DATABASE_CONFIG
        
        required_etl_keys = ['batch_size', 'mock_data_size']
        for key in required_etl_keys:
            assert key in ETL_CONFIG
        
        assert 'age_groups' in DATA_GENERATION
        assert 'complaint_types' in DATA_GENERATION
        
        print("✓ Configuration loaded successfully")
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 50)
    print("CUSTOMER COMPLAINTS ETL - BASIC TESTS")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_configuration,
        test_data_generation,
        test_data_transformation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed! The ETL project is ready to use.")
        print("\nNext steps:")
        print("1. Start PostgreSQL: docker-compose up -d")
        print("2. Run the ETL pipeline: python src/main.py --records 100")
    else:
        print("✗ Some tests failed. Please check the errors above.")
        return 1
    
    print("=" * 50)
    return 0

if __name__ == "__main__":
    exit(main())
