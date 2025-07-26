# Customer Complaints ETL Pipeline - Azure Terraform Deployment

This directory contains Terraform configuration files to deploy the Customer Complaints ETL pipeline on Azure using Databricks.

## Architecture Overview

The deployment creates the following Azure resources:

- **Resource Group**: Container for all resources
- **Azure Databricks Workspace**: For running ETL notebooks and jobs
- **Azure SQL Database**: For storing processed data
- **Azure Data Lake Storage Gen2**: For intermediate data storage
- **Azure Key Vault**: For secure secret management
- **Databricks Cluster**: Auto-scaling cluster for ETL processing
- **Databricks Job**: Scheduled ETL pipeline execution

## Prerequisites

1. **Azure CLI**: Install and configure Azure CLI
   ```bash
   az login
   az account set --subscription "your-subscription-id"
   ```

2. **Terraform**: Install Terraform (>= 1.0)
   ```bash
   # On Windows (using Chocolatey)
   choco install terraform
   
   # On macOS (using Homebrew)
   brew install terraform
   
   # On Linux
   wget https://releases.hashicorp.com/terraform/1.6.0/terraform_1.6.0_linux_amd64.zip
   unzip terraform_1.6.0_linux_amd64.zip
   sudo mv terraform /usr/local/bin/
   ```

3. **Azure Permissions**: Ensure your Azure account has:
   - Contributor role on the subscription
   - Ability to create service principals
   - Key Vault access permissions

## Quick Start

### 1. Configure Variables

Copy the example variables file and customize it:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your specific values:

```hcl
# Azure Configuration
resource_group_name = "rg-customer-complaints-etl-prod"
location           = "East US"

# SQL Database Configuration
sql_admin_username = "sqladmin"
sql_admin_password = "YourVerySecurePassword123!"  # Use a strong password!
sql_database_name  = "complaints_db"

# Databricks Configuration
databricks_sku = "standard"  # or "premium" for advanced features

# ETL Configuration
etl_schedule_cron = "0 2 * * *"  # Daily at 2 AM UTC
```

### 2. Initialize Terraform

```bash
cd terraform
terraform init
```

### 3. Plan Deployment

```bash
terraform plan
```

Review the planned changes carefully before proceeding.

### 4. Deploy Infrastructure

```bash
terraform apply
```

Type `yes` when prompted to confirm the deployment.

### 5. Access Deployed Resources

After successful deployment, Terraform will output connection details:

```
databricks_workspace_url = "https://adb-xxxxx.xx.azuredatabricks.net"
sql_server_fqdn = "etl-sql-server-xxxxx.database.windows.net"
storage_account_name = "etldatalakexxxxx"
```

## File Structure

```
terraform/
├── main.tf                    # Main Terraform configuration
├── variables.tf               # Variable definitions
├── databricks.tf             # Databricks-specific resources
├── outputs.tf                # Output values
├── terraform.tfvars.example  # Example variables file
└── README.md                 # This file
```

## Configuration Details

### Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `resource_group_name` | Name of the Azure Resource Group | `rg-customer-complaints-etl` | No |
| `location` | Azure region for resources | `East US` | No |
| `sql_admin_password` | SQL Server admin password | - | Yes |
| `databricks_sku` | Databricks workspace SKU | `standard` | No |
| `etl_schedule_cron` | Cron expression for ETL job | `0 2 * * *` | No |

### Outputs

After deployment, the following outputs are available:

- `databricks_workspace_url`: URL to access Databricks workspace
- `sql_server_fqdn`: Fully qualified domain name of SQL server
- `storage_account_name`: Name of the created storage account
- `key_vault_name`: Name of the Key Vault for secrets
- `databricks_job_url`: Direct URL to the ETL job in Databricks

## Post-Deployment Steps

### 1. Access Databricks Workspace

1. Navigate to the Databricks workspace URL from the output
2. Sign in with your Azure credentials
3. The ETL notebooks will be automatically deployed to `/etl/` folder
4. The ETL job will be created and scheduled automatically

### 2. Monitor ETL Job

1. In Databricks, go to **Workflows** → **Jobs**
2. Find "Customer Complaints ETL Pipeline" job
3. Click to view job details and execution history
4. You can run the job manually or wait for scheduled execution

### 3. Access SQL Database

Connect to the Azure SQL Database using:
- **Server**: `<sql_server_fqdn>` (from output)
- **Database**: `complaints_db`
- **Username**: `sqladmin` (or your configured username)
- **Password**: Your configured password

### 4. Query Data

Once the ETL job runs successfully, you can query the data:

```sql
-- View complaint analytics
SELECT TOP 10 * FROM complaint_analytics;

-- Daily complaint summary
SELECT * FROM complaint_summary 
ORDER BY complaint_day DESC;

-- Complaints by severity
SELECT severity, COUNT(*) as count
FROM complaints 
GROUP BY severity 
ORDER BY count DESC;
```

## Customization

### Scaling Configuration

Adjust cluster scaling in `variables.tf`:

```hcl
variable "databricks_min_workers" {
  default = 2  # Increase for higher baseline capacity
}

variable "databricks_max_workers" {
  default = 8  # Increase for higher peak capacity
}
```

### Schedule Configuration

Modify ETL schedule in `terraform.tfvars`:

```hcl
etl_schedule_cron = "0 */6 * * *"  # Every 6 hours
etl_schedule_cron = "0 8 * * 1"    # Weekly on Mondays at 8 AM
```

### Environment-Specific Deployments

Create separate `.tfvars` files for different environments:

```bash
# Development
terraform apply -var-file="dev.tfvars"

# Production
terraform apply -var-file="prod.tfvars"
```

## Security Considerations

### 1. Password Management

- Use strong passwords for SQL admin account
- Consider using Azure Key Vault references instead of plain text
- Rotate passwords regularly

### 2. Network Security

- SQL Database has firewall rules allowing Azure services
- Consider adding specific IP restrictions for production
- Use private endpoints for enhanced security

### 3. Access Control

- Databricks workspace uses Azure AD integration
- Key Vault access is restricted to deployment principal
- Consider implementing RBAC for fine-grained access control

## Troubleshooting

### Common Issues

1. **Terraform Init Fails**
   ```bash
   # Clear Terraform cache and reinitialize
   rm -rf .terraform
   terraform init
   ```

2. **SQL Password Validation Error**
   - Ensure password meets Azure SQL complexity requirements
   - Must be 8-128 characters with uppercase, lowercase, numbers, and symbols

3. **Databricks Cluster Creation Fails**
   - Check Azure subscription quotas for compute resources
   - Verify Databricks workspace is properly created

4. **Key Vault Access Denied**
   - Ensure your Azure account has Key Vault permissions
   - Check if soft-delete protection is causing conflicts

### Debugging

Enable Terraform debug logging:

```bash
export TF_LOG=DEBUG
terraform apply
```

Check Azure activity logs:

```bash
az monitor activity-log list --resource-group <resource-group-name>
```

## Cost Optimization

### 1. Auto-Termination

Clusters automatically terminate after 30 minutes of inactivity by default.

### 2. Scheduled Scaling

Consider using smaller clusters for development:

```hcl
databricks_node_type = "Standard_DS3_v2"  # Smaller for dev
databricks_min_workers = 1                # Minimum for dev
```

### 3. Storage Tiers

Data Lake Storage uses Standard LRS by default. Consider:
- Hot tier for frequently accessed data
- Cool tier for archival data

## Cleanup

To destroy all created resources:

```bash
terraform destroy
```

**Warning**: This will permanently delete all data and resources!

## Support

For issues with:
- **Terraform configuration**: Check Terraform documentation
- **Azure resources**: Consult Azure documentation
- **Databricks**: Refer to Databricks documentation
- **ETL pipeline**: Check the main project README

## Next Steps

After successful deployment:

1. **Test the ETL pipeline** by running the job manually
2. **Set up monitoring** using Azure Monitor and Databricks alerts
3. **Configure backup** for SQL Database
4. **Implement CI/CD** for automated deployments
5. **Add data quality checks** and validation rules

## Version History

- **v1.0**: Initial Terraform configuration with basic ETL pipeline
- **v1.1**: Added Key Vault integration and security improvements
- **v1.2**: Enhanced monitoring and logging capabilities
