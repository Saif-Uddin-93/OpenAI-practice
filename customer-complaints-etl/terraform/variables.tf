variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "rg-customer-complaints-etl"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "West Europe"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Environment = "dev"
    Project     = "customer-complaints-etl"
    Owner       = "data-team"
  }
}

variable "databricks_sku" {
  description = "SKU for Databricks workspace"
  type        = string
  default     = "standard"
  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "Databricks SKU must be one of: standard, premium, trial."
  }
}

variable "sql_admin_username" {
  description = "Administrator username for Azure SQL Server"
  type        = string
  default     = "sqladmin"
}

variable "sql_admin_password" {
  description = "Administrator password for Azure SQL Server"
  type        = string
  sensitive   = true
  validation {
    condition     = length(var.sql_admin_password) >= 8
    error_message = "SQL admin password must be at least 8 characters long."
  }
}

variable "sql_database_name" {
  description = "Name of the Azure SQL Database"
  type        = string
  default     = "complaints_db"
}

variable "databricks_cluster_name" {
  description = "Name of the Databricks cluster"
  type        = string
  default     = "etl-cluster"
}

variable "databricks_node_type" {
  description = "Node type for Databricks cluster"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "databricks_min_workers" {
  description = "Minimum number of workers for Databricks cluster"
  type        = number
  default     = 1
}

variable "databricks_max_workers" {
  description = "Maximum number of workers for Databricks cluster"
  type        = number
  default     = 3
}

variable "databricks_spark_version" {
  description = "Spark version for Databricks cluster"
  type        = string
  default     = "13.3.x-scala2.12"
}

variable "databricks_python_version" {
  description = "Python version for Databricks cluster"
  type        = string
  default     = "3"
}

variable "etl_schedule_cron" {
  description = "Cron expression for ETL job schedule"
  type        = string
  default     = "0 2 * * *" # Daily at 2 AM
}

variable "enable_auto_termination" {
  description = "Enable auto-termination for Databricks cluster"
  type        = bool
  default     = true
}

variable "auto_termination_minutes" {
  description = "Minutes of inactivity before auto-termination"
  type        = number
  default     = 30
}
