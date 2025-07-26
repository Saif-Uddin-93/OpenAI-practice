# Configure Databricks provider
provider "databricks" {
  host = azurerm_databricks_workspace.main.workspace_url
}

# Databricks Cluster
resource "databricks_cluster" "etl_cluster" {
  cluster_name            = var.databricks_cluster_name
  spark_version           = var.databricks_spark_version
  node_type_id            = var.databricks_node_type
  driver_node_type_id     = var.databricks_node_type
  autotermination_minutes = var.enable_auto_termination ? var.auto_termination_minutes : 0

  autoscale {
    min_workers = var.databricks_min_workers
    max_workers = var.databricks_max_workers
  }

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  custom_tags = var.tags

  library {
    pypi {
      package = "pandas==2.1.4"
    }
  }

  library {
    pypi {
      package = "sqlalchemy==2.0.23"
    }
  }

  library {
    pypi {
      package = "faker==20.1.0"
    }
  }

  library {
    pypi {
      package = "python-dotenv==1.0.0"
    }
  }

  library {
    pypi {
      package = "pyodbc"
    }
  }

  library {
    pypi {
      package = "azure-storage-blob"
    }
  }

  library {
    pypi {
      package = "azure-keyvault-secrets"
    }
  }

  library {
    pypi {
      package = "azure-identity"
    }
  }

  depends_on = [azurerm_databricks_workspace.main]
}

# Databricks Secret Scope for Key Vault
resource "databricks_secret_scope" "keyvault" {
  name = "keyvault-secrets"

  keyvault_metadata {
    resource_id = azurerm_key_vault.main.id
    dns_name    = azurerm_key_vault.main.vault_uri
  }

  depends_on = [databricks_cluster.etl_cluster]
}

# Upload ETL scripts to Databricks workspace
resource "databricks_notebook" "etl_main" {
  path     = "/etl/main"
  language = "PYTHON"
  content_base64 = base64encode(templatefile("${path.module}/../databricks/etl_main.py", {
    storage_account_name = azurerm_storage_account.datalake.name
    container_name       = azurerm_storage_container.etl_data.name
    key_vault_scope      = databricks_secret_scope.keyvault.name
  }))

  depends_on = [databricks_cluster.etl_cluster]
}

resource "databricks_notebook" "etl_extract" {
  path     = "/etl/extract"
  language = "PYTHON"
  content_base64 = base64encode(templatefile("${path.module}/../databricks/etl_extract.py", {
    storage_account_name = azurerm_storage_account.datalake.name
    container_name       = azurerm_storage_container.etl_data.name
    key_vault_scope      = databricks_secret_scope.keyvault.name
  }))

  depends_on = [databricks_cluster.etl_cluster]
}

resource "databricks_notebook" "etl_transform" {
  path     = "/etl/transform"
  language = "PYTHON"
  content_base64 = base64encode(templatefile("${path.module}/../databricks/etl_transform.py", {
    storage_account_name = azurerm_storage_account.datalake.name
    container_name       = azurerm_storage_container.etl_data.name
    key_vault_scope      = databricks_secret_scope.keyvault.name
  }))

  depends_on = [databricks_cluster.etl_cluster]
}

resource "databricks_notebook" "etl_load" {
  path     = "/etl/load"
  language = "PYTHON"
  content_base64 = base64encode(templatefile("${path.module}/../databricks/etl_load.py", {
    storage_account_name = azurerm_storage_account.datalake.name
    container_name       = azurerm_storage_container.etl_data.name
    key_vault_scope      = databricks_secret_scope.keyvault.name
  }))

  depends_on = [databricks_cluster.etl_cluster]
}

# Databricks Job for ETL Pipeline
resource "databricks_job" "etl_pipeline" {
  name = "Customer Complaints ETL Pipeline"

  task {
    task_key = "extract"

    notebook_task {
      notebook_path = databricks_notebook.etl_extract.path
    }

    existing_cluster_id = databricks_cluster.etl_cluster.id

    timeout_seconds = 3600
  }

  task {
    task_key = "transform"
    depends_on {
      task_key = "extract"
    }

    notebook_task {
      notebook_path = databricks_notebook.etl_transform.path
    }

    existing_cluster_id = databricks_cluster.etl_cluster.id

    timeout_seconds = 3600
  }

  task {
    task_key = "load"
    depends_on {
      task_key = "transform"
    }

    notebook_task {
      notebook_path = databricks_notebook.etl_load.path
    }

    existing_cluster_id = databricks_cluster.etl_cluster.id

    timeout_seconds = 3600
  }

  schedule {
    quartz_cron_expression = var.etl_schedule_cron
    timezone_id            = "UTC"
  }

  email_notifications {
    on_start   = []
    on_success = []
    on_failure = []
  }

  tags = var.tags

  depends_on = [
    databricks_notebook.etl_extract,
    databricks_notebook.etl_transform,
    databricks_notebook.etl_load
  ]
}
