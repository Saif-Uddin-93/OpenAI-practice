# Customer Complaints ETL Pipeline - Azure Deployment Guide

This document provides a comprehensive guide for deploying the Customer Complaints ETL pipeline on Azure using Databricks and Terraform.

## 🏗️ Architecture Overview

The Azure deployment creates a complete data pipeline infrastructure:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Source   │    │   Azure Data     │    │  Azure SQL      │
│   (Generated)   │───▶│   Lake Storage   │───▶│   Database      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Databricks     │
                       │   Workspace      │
                       │   - ETL Cluster  │
                       │   - Notebooks    │
                       │   - Scheduled    │
                       │     Jobs         │
                       └──────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Azure Key      │
                       │   Vault          │
                       │   (Secrets)      │
                       └──────────────────┘
```

## 📋 What Gets Deployed

### Azure Resources

1. **Resource Group**: `rg-customer-complaints-etl`
   - Container for all resources
   - Organized by environment (dev/prod)

2. **Azure Databricks Workspace**: `etl-databricks-xxxxxx`
   - Standard or Premium SKU
   - Auto-scaling cluster configuration
   - Pre-deployed ETL notebooks
   - Scheduled job execution

3. **Azure SQL Database**: `etl-sql-server-xxxxxx`
   - S1 tier (20GB storage)
   - Optimized schema for analytics
   - Pre-built views for reporting
   - Firewall rules for Azure services

4. **Azure Data Lake Storage Gen2**: `etldatalakexxxxxx`
   - Hierarchical namespace enabled
   - Containers for raw, transformed, and metadata
   - LRS replication for cost optimization

5. **Azure Key Vault**: `etl-kv-xxxxxx`
   - Secure storage for connection strings
   - Database credentials
   - Storage account keys

### Databricks Components

1. **ETL Cluster**: `etl-cluster`
   - Auto-scaling (1-3 workers by default)
   - Auto-termination after 30 minutes
   - Pre-installed Python libraries
   - Optimized for ETL workloads

2. **Notebooks**: `/etl/` folder
   - `main`: Pipeline orchestrator
   - `extract`: Data generation and extraction
   - `transform`: Data cleaning and enrichment
   - `load`: Database loading with schema creation

3. **Scheduled Job**: "Customer Complaints ETL Pipeline"
   - Daily execution at 2 AM UTC (configurable)
   - Three-phase workflow (Extract → Transform → Load)
   - Error handling and logging
   - Email notifications (configurable)

## 🚀 Quick Deployment

### Prerequisites

1. **Azure CLI** installed and configured
2. **Terraform** (>= 1.0) installed
3. **Azure subscription** with appropriate permissions
4. **PowerShell** or **Bash** terminal

### Step-by-Step Deployment

1. **Navigate to terraform directory**:
   ```bash
   cd customer-complaints-etl/terraform
   ```

2. **Configure variables**:
   ```bash
   # Copy example file
   cp terraform.tfvars.example terraform.tfvars
   
   # Edit with your values (especially sql_admin_password!)
   notepad terraform.tfvars  # Windows
   # or
   nano terraform.tfvars     # Linux/Mac
   ```

3. **Deploy using script** (Linux/Mac):
   ```bash
   ./deploy.sh
   ```

4. **Manual deployment** (Windows/All platforms):
   ```bash
   # Initialize Terraform
   terraform init
   
   # Plan deployment
   terraform plan
   
   # Deploy infrastructure
   terraform apply
   ```

5. **Access your resources**:
   - Databricks workspace URL will be displayed in output
   - SQL Database connection details provided
   - Storage account name for data access

## 📊 ETL Pipeline Details

### Data Flow

1. **Extract Phase**:
   - Generates realistic mock customer complaint data
   - Creates customers and complaints datasets
   - Saves raw data to Azure Data Lake Storage
   - Configurable number of records (default: 1000)

2. **Transform Phase**:
   - Loads raw data from storage
   - Cleans and standardizes data
   - Adds derived fields and analytics features
   - Performs data quality validation
   - Saves transformed data back to storage

3. **Load Phase**:
   - Creates optimized database schema
   - Loads customers and complaints data
   - Generates complaint resolution records
   - Creates analytical views for reporting
   - Provides final statistics

### Data Schema

**Customers Table**:
- Basic demographics (name, email, phone)
- Segmentation (type, region, age group)
- Derived fields (tenure, segment classification)

**Complaints Table**:
- Complaint details (type, text, severity)
- Temporal features (date, day of week, hour)
- Sentiment analysis and urgency scoring
- Resolution tracking

**Analytics Views**:
- `complaint_analytics`: Comprehensive complaint analysis
- `complaint_summary`: Daily aggregated metrics

## 💰 Cost Estimation

### Monthly Costs (Approximate)

| Service | Configuration | Estimated Cost |
|---------|---------------|----------------|
| Azure SQL Database | S1 (20GB) | $20 |
| Databricks Workspace | Standard SKU | $0.40/DBU |
| Data Lake Storage | Standard LRS | $5-10 |
| Key Vault | Standard | $1 |
| **Total** | | **$30-50/month** |

### Cost Optimization Tips

1. **Auto-termination**: Clusters terminate after 30 minutes of inactivity
2. **Scheduled execution**: Jobs run only when needed
3. **Right-sizing**: Start with smaller clusters and scale as needed
4. **Storage tiers**: Use appropriate storage tiers for data lifecycle

## 🔧 Configuration Options

### Environment Variables

Edit `terraform.tfvars` to customize:

```hcl
# Resource naming
resource_group_name = "rg-complaints-etl-prod"
location = "East US"

# Databricks configuration
databricks_sku = "premium"  # standard or premium
databricks_min_workers = 2
databricks_max_workers = 8

# ETL scheduling
etl_schedule_cron = "0 */6 * * *"  # Every 6 hours

# Database configuration
sql_admin_password = "YourSecurePassword123!"
```

### Scaling Configuration

For production workloads:

```hcl
# Larger cluster for production
databricks_node_type = "Standard_DS4_v2"
databricks_min_workers = 2
databricks_max_workers = 10

# Premium features
databricks_sku = "premium"

# More frequent execution
etl_schedule_cron = "0 */4 * * *"  # Every 4 hours
```

## 📈 Monitoring and Operations

### Databricks Monitoring

1. **Job Execution**:
   - Navigate to Workflows → Jobs
   - View execution history and logs
   - Set up email notifications for failures

2. **Cluster Monitoring**:
   - Monitor cluster utilization
   - Review auto-scaling events
   - Check library installation status

### Database Monitoring

1. **Azure SQL Database**:
   - Use Azure portal for performance metrics
   - Set up alerts for high CPU/memory usage
   - Monitor storage consumption

2. **Query Performance**:
   ```sql
   -- Check data freshness
   SELECT MAX(created_at) as last_update FROM complaints;
   
   -- Monitor data volume
   SELECT 
       'customers' as table_name, COUNT(*) as record_count 
   FROM customers
   UNION ALL
   SELECT 
       'complaints' as table_name, COUNT(*) as record_count 
   FROM complaints;
   ```

### Storage Monitoring

1. **Data Lake Storage**:
   - Monitor storage usage and costs
   - Review access patterns
   - Set up lifecycle management policies

## 🔒 Security Considerations

### Access Control

1. **Azure AD Integration**:
   - Databricks uses Azure AD for authentication
   - SQL Database supports Azure AD authentication
   - Key Vault access controlled by Azure RBAC

2. **Network Security**:
   - SQL Database firewall allows Azure services
   - Consider private endpoints for production
   - Storage account uses private containers

3. **Secret Management**:
   - All credentials stored in Key Vault
   - Databricks secret scopes for secure access
   - No hardcoded passwords in code

### Best Practices

1. **Password Policy**:
   - Use strong passwords (8+ characters, mixed case, numbers, symbols)
   - Rotate passwords regularly
   - Consider Azure AD authentication

2. **Access Reviews**:
   - Regular review of user access
   - Principle of least privilege
   - Audit logs for compliance

## 🛠️ Troubleshooting

### Common Issues

1. **Terraform Deployment Fails**:
   ```bash
   # Check Azure login status
   az account show
   
   # Verify subscription permissions
   az role assignment list --assignee $(az account show --query user.name -o tsv)
   
   # Clear Terraform state if needed
   rm -rf .terraform
   terraform init
   ```

2. **Databricks Cluster Won't Start**:
   - Check Azure subscription quotas
   - Verify Databricks workspace is active
   - Review cluster configuration for valid node types

3. **SQL Database Connection Issues**:
   - Verify firewall rules
   - Check connection string format
   - Ensure password meets complexity requirements

4. **ETL Job Failures**:
   - Check Databricks job logs
   - Verify Key Vault access permissions
   - Review storage account connectivity

### Debug Commands

```bash
# Terraform debugging
export TF_LOG=DEBUG
terraform apply

# Azure resource verification
az resource list --resource-group <resource-group-name>

# Databricks CLI (if installed)
databricks jobs list
databricks clusters list
```

## 🧹 Cleanup

### Destroy Infrastructure

**⚠️ Warning**: This will permanently delete all data and resources!

```bash
# Using deployment script
./deploy.sh --destroy

# Manual cleanup
terraform destroy
```

### Partial Cleanup

To reduce costs while keeping data:

1. **Stop Databricks cluster** (manual in portal)
2. **Pause SQL Database** (if supported in your region)
3. **Keep storage and data** for later use

## 📚 Additional Resources

### Documentation Links

- [Azure Databricks Documentation](https://docs.microsoft.com/en-us/azure/databricks/)
- [Azure SQL Database Documentation](https://docs.microsoft.com/en-us/azure/azure-sql/)
- [Terraform Azure Provider](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)
- [Azure Data Lake Storage Gen2](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)

### Sample Queries

```sql
-- Top complaint types by volume
SELECT 
    complaint_type,
    COUNT(*) as complaint_count,
    AVG(resolution_time_hours) as avg_resolution_time
FROM complaint_analytics
WHERE resolution_status = 'Resolved'
GROUP BY complaint_type
ORDER BY complaint_count DESC;

-- Customer satisfaction by region
SELECT 
    region,
    AVG(customer_satisfaction_score) as avg_satisfaction,
    COUNT(*) as total_complaints
FROM complaint_analytics
WHERE customer_satisfaction_score IS NOT NULL
GROUP BY region
ORDER BY avg_satisfaction DESC;

-- Daily complaint trends
SELECT 
    complaint_day,
    total_complaints,
    critical_complaints,
    high_complaints,
    resolved_complaints,
    avg_resolution_hours
FROM complaint_summary
WHERE complaint_day >= DATEADD(day, -30, GETDATE())
ORDER BY complaint_day DESC;
```

## 🎯 Next Steps

After successful deployment:

1. **Test the pipeline** by running the job manually
2. **Explore the data** using the provided SQL queries
3. **Set up monitoring** and alerting
4. **Customize the ETL logic** for your specific needs
5. **Implement CI/CD** for automated deployments
6. **Add data quality checks** and validation rules
7. **Create dashboards** using Power BI or similar tools

## 📞 Support

For issues and questions:

1. **Terraform Issues**: Check the Terraform documentation and Azure provider docs
2. **Azure Resources**: Use Azure support channels
3. **Databricks**: Refer to Databricks documentation and community forums
4. **ETL Pipeline**: Review the main project README and code comments

---

**Happy Data Engineering!** 🚀📊
