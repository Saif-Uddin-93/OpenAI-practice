output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.main.name
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace"
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "databricks_workspace_id" {
  description = "ID of the Databricks workspace"
  value       = azurerm_databricks_workspace.main.workspace_id
}

output "storage_account_name" {
  description = "Name of the storage account"
  value       = azurerm_storage_account.datalake.name
}

output "storage_account_primary_endpoint" {
  description = "Primary endpoint of the storage account"
  value       = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "sql_server_fqdn" {
  description = "Fully qualified domain name of the SQL server"
  value       = azurerm_mssql_server.main.fully_qualified_domain_name
}

output "sql_database_name" {
  description = "Name of the SQL database"
  value       = azurerm_mssql_database.main.name
}

output "key_vault_name" {
  description = "Name of the Key Vault"
  value       = azurerm_key_vault.main.name
}

output "key_vault_uri" {
  description = "URI of the Key Vault"
  value       = azurerm_key_vault.main.vault_uri
}

output "databricks_cluster_id" {
  description = "ID of the Databricks cluster"
  value       = databricks_cluster.etl_cluster.id
}

output "databricks_job_id" {
  description = "ID of the Databricks ETL job"
  value       = databricks_job.etl_pipeline.id
}

output "databricks_job_url" {
  description = "URL of the Databricks ETL job"
  value       = databricks_job.etl_pipeline.url
}

output "etl_data_container_url" {
  description = "URL of the ETL data container"
  value       = "${azurerm_storage_account.datalake.primary_dfs_endpoint}${azurerm_storage_container.etl_data.name}"
}

output "connection_instructions" {
  description = "Instructions for connecting to the deployed resources"
  value = <<-EOT
    
    === Azure Customer Complaints ETL Pipeline Deployment ===
    
    Resource Group: ${azurerm_resource_group.main.name}
    Location: ${azurerm_resource_group.main.location}
    
    Databricks Workspace:
    - URL: https://${azurerm_databricks_workspace.main.workspace_url}
    - Cluster: ${var.databricks_cluster_name}
    - Job: Customer Complaints ETL Pipeline
    
    Azure SQL Database:
    - Server: ${azurerm_mssql_server.main.fully_qualified_domain_name}
    - Database: ${azurerm_mssql_database.main.name}
    - Username: ${var.sql_admin_username}
    
    Storage Account:
    - Name: ${azurerm_storage_account.datalake.name}
    - Data Container: ${azurerm_storage_container.etl_data.name}
    - Scripts Container: ${azurerm_storage_container.etl_scripts.name}
    
    Key Vault:
    - Name: ${azurerm_key_vault.main.name}
    - URI: ${azurerm_key_vault.main.vault_uri}
    
    Next Steps:
    1. Access Databricks workspace using the URL above
    2. Navigate to Jobs to see the ETL pipeline
    3. Run the job manually or wait for scheduled execution
    4. Monitor job execution and logs in Databricks
    5. Query results in Azure SQL Database
    
  EOT
}
