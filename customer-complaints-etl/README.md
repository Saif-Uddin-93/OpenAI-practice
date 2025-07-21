# Customer Complaints ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for processing customer complaint data. This project generates mock customer complaint data, transforms it according to business rules, and loads it into a PostgreSQL database with optimized schemas for analytics.

## Features

- **Extract**: Generate realistic mock customer complaint data
- **Transform**: Clean, standardize, and enrich data with derived fields
- **Load**: Store data in PostgreSQL with optimized schemas and indexes
- **Analytics**: Pre-built views for easy data retrieval and reporting
- **Modular Design**: Run individual ETL phases or the complete pipeline
- **Data Quality**: Built-in validation and quality reporting
- **Docker Support**: Easy PostgreSQL setup with Docker Compose

## Project Structure

```
customer-complaints-etl/
├── src/
│   ├── extract/
│   │   └── data_generator.py      # Mock data generation
│   ├── transform/
│   │   └── data_transformer.py    # Data transformation logic
│   ├── load/
│   │   └── database_loader.py     # PostgreSQL loading
│   └── main.py                    # ETL orchestration
├── config/
│   ├── database.py                # DB configuration
│   └── settings.py                # General settings
├── sql/
│   └── schema.sql                 # Database schema creation
├── requirements.txt               # Python dependencies
├── docker-compose.yml             # PostgreSQL container setup
├── .env.example                   # Environment variables template
└── README.md                      # This file
```

## Data Model

### Demographics
- **Age Groups**: 18-25, 26-35, 36-45, 46-55, 56-65, 65+
- **Regions**: North, South, East, West, Central
- **Customer Types**: Premium, Standard, Basic
- **Gender**: Male, Female, Other, Prefer not to say

### Complaint Categories
- **Severity Levels**: Critical (1), High (2), Medium (3), Low (4)
- **Complaint Types**: Product Quality, Service Issues, Billing Problems, Delivery Issues, Technical Support, Account Management, Refund Requests

### Database Schema
- `customers` - Customer demographic information
- `complaint_types` - Lookup table for complaint categories
- `severity_levels` - Lookup table for severity definitions
- `complaints` - Main complaints fact table
- `complaint_resolutions` - Resolution tracking
- `complaint_analytics` - Pre-built analytical view
- `complaint_summary` - Summary view for reporting

## Quick Start

### 1. Prerequisites

- Python 3.8+
- Docker and Docker Compose (for PostgreSQL)
- Git

### 2. Setup

```bash
# Clone the repository
git clone <repository-url>
cd customer-complaints-etl

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Setup environment variables
cp .env.example .env
# Edit .env file with your configuration
```

### 3. Start PostgreSQL Database

```bash
# Start PostgreSQL and pgAdmin using Docker
docker-compose up -d

# Verify containers are running
docker-compose ps
```

**Database Access:**
- PostgreSQL: `localhost:5432`
- pgAdmin: `http://localhost:8080` (admin@example.com / admin)

### 4. Run the ETL Pipeline

```bash
# Run the complete ETL pipeline
python src/main.py

# Run with custom number of records
python src/main.py --records 500

# Run individual phases
python src/main.py --mode extract --records 100
python src/main.py --mode transform --input-file data/extracted_complaints.json
python src/main.py --mode load --customers-file data/transformed/customers_transformed.csv --complaints-file data/transformed/complaints_transformed.csv
```

## Usage Examples

### Command Line Options

```bash
# Full pipeline with 2000 records
python src/main.py --records 2000

# Extract only
python src/main.py --mode extract --records 500

# Transform existing data
python src/main.py --mode transform --input-file data/extracted_complaints.json

# Load pre-transformed data
python src/main.py --mode load --customers-file customers.csv --complaints-file complaints.csv

# Skip database schema setup (if already exists)
python src/main.py --no-setup-db
```

### Programmatic Usage

```python
from src.main import ComplaintETLPipeline

# Initialize pipeline
pipeline = ComplaintETLPipeline()

# Run full pipeline
result = pipeline.run_full_pipeline(num_records=1000)

# Run individual phases
extract_result = pipeline.run_extract_only(num_records=500)
```

## Data Analysis Queries

Once data is loaded, you can run analytical queries:

```sql
-- Complaints by severity and region
SELECT 
    region,
    severity_name,
    COUNT(*) as complaint_count,
    AVG(resolution_time_hours) as avg_resolution_time
FROM complaint_analytics
WHERE resolution_status = 'Resolved'
GROUP BY region, severity_name
ORDER BY complaint_count DESC;

-- Top complaint types by customer segment
SELECT 
    customer_type,
    complaint_type,
    COUNT(*) as complaint_count
FROM complaint_analytics
GROUP BY customer_type, complaint_type
ORDER BY complaint_count DESC
LIMIT 10;

-- Daily complaint trends
SELECT 
    complaint_day,
    COUNT(*) as total_complaints,
    AVG(avg_resolution_hours) as avg_resolution_time
FROM complaint_summary
WHERE complaint_day >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY complaint_day
ORDER BY complaint_day DESC;

-- High priority complaints needing attention
SELECT 
    complaint_uuid,
    customer_type,
    region,
    complaint_type,
    severity_name,
    complaint_date,
    resolution_status
FROM complaint_analytics
WHERE severity_name IN ('Critical', 'High')
    AND resolution_status IN ('Open', 'In Progress')
ORDER BY complaint_date ASC;
```

## Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=complaints_db
DB_USER=postgres
DB_PASSWORD=password

# ETL Configuration
BATCH_SIZE=100
MOCK_DATA_SIZE=1000
OUTPUT_FORMAT=json
DATA_OUTPUT_PATH=data/

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=etl.log
```

### Customizing Data Generation

Edit `config/settings.py` to customize:

- Demographic categories
- Complaint types and severity keywords
- Data generation parameters
- Output formats

## Data Quality Features

The pipeline includes comprehensive data quality checks:

- **Validation**: Missing values, invalid categories, future dates
- **Deduplication**: Removes duplicate records based on UUIDs
- **Referential Integrity**: Ensures customer-complaint relationships
- **Data Profiling**: Generates quality reports for each phase
- **Logging**: Detailed logs for monitoring and debugging

## Performance Considerations

- **Batch Processing**: Configurable batch sizes for large datasets
- **Database Indexes**: Optimized indexes for common query patterns
- **Memory Management**: Efficient pandas operations for large datasets
- **Connection Pooling**: SQLAlchemy engine management
- **Incremental Loading**: Upsert logic to handle data updates

## Monitoring and Logging

- **Structured Logging**: JSON-formatted logs with timestamps
- **Phase Tracking**: Detailed metadata for each ETL phase
- **Error Handling**: Comprehensive exception handling and recovery
- **Performance Metrics**: Execution time and throughput tracking

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   ```bash
   # Check if PostgreSQL is running
   docker-compose ps
   
   # Restart PostgreSQL
   docker-compose restart postgres
   ```

2. **Import Errors**
   ```bash
   # Ensure you're in the project root and virtual environment is activated
   cd customer-complaints-etl
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Permission Errors**
   ```bash
   # Check file permissions
   chmod +x src/main.py
   ```

4. **Memory Issues with Large Datasets**
   - Reduce `MOCK_DATA_SIZE` in configuration
   - Increase `BATCH_SIZE` for more efficient processing

### Logs and Debugging

- Check `etl.log` for detailed execution logs
- Use `--mode extract` to test data generation separately
- Verify database connectivity with pgAdmin at `http://localhost:8080`

## Development

### Adding New Features

1. **New Data Sources**: Extend `ComplaintDataGenerator`
2. **Custom Transformations**: Add methods to `ComplaintDataTransformer`
3. **Additional Analytics**: Create new views in `schema.sql`
4. **New Output Formats**: Extend the loader classes

### Testing

```bash
# Test individual components
python src/extract/data_generator.py
python src/transform/data_transformer.py
python src/load/database_loader.py

# Test with small datasets
python src/main.py --records 10
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs in `etl.log`
3. Open an issue on GitHub with detailed error information
