# 1. Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# 2. Génération d'une chaîne aléatoire pour garantir l'unicité
resource "random_string" "random" {
  length  = 6
  special = false
  upper   = false
}

# 3. Storage Account Azure (ADLS Gen2 - stockage externe & backend Function)
resource "azurerm_storage_account" "storage" {
  name                     = "stdataops${var.environment}${random_string.random.result}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Active Data Lake Storage Gen2

  tags = azurerm_resource_group.rg.tags
}

# Conteneur pour nos fichiers bruts (Bronze)
resource "azurerm_storage_container" "raw_data" {
  name                  = "bronze"
  storage_account_id    = azurerm_storage_account.storage.id
  container_access_type = "private"
}

# 4. Azure Databricks Workspace (Premium SKU - 14j essai gratuit DBU)
resource "azurerm_databricks_workspace" "databricks" {
  name                = "dbw-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"

  tags = azurerm_resource_group.rg.tags
}

# 5. Compte Azure AI Services / OpenAI (Socle de Microsoft Azure AI Foundry)
resource "azurerm_cognitive_account" "ai_foundry" {
  name                  = "aifs-${var.project_name}-${var.environment}"
  location              = azurerm_resource_group.rg.location
  resource_group_name   = azurerm_resource_group.rg.name
  kind                  = "OpenAI"
  sku_name              = "S0"
  custom_subdomain_name = "aifs-${var.project_name}-${var.environment}-${random_string.random.result}"

  tags = azurerm_resource_group.rg.tags
}

# 6. Déploiement du modèle d'IA (GPT-4.1-mini sous GlobalStandard avec quota actif)
resource "azurerm_cognitive_deployment" "ai_model" {
  name                 = "gpt-4.1-mini"
  cognitive_account_id = azurerm_cognitive_account.ai_foundry.id

  model {
    format  = "OpenAI"
    name    = "gpt-4.1-mini"
    version = "2025-04-14"
  }

  sku {
    name     = "GlobalStandard"
    capacity = 10 # 10k TPM
  }
}

# 7. Déploiement du Notebook PySpark d'ingestion dans le Workspace Databricks
resource "databricks_notebook" "sales_pipeline" {
  source = "${path.module}/../notebooks/01_ingest_sales_pipeline.py"
  path   = "/Shared/01_ingest_sales_pipeline"
}

# 8. Déploiement du Notebook Agent Troubleshooter dans le Workspace Databricks
resource "databricks_notebook" "troubleshooter_agent" {
  source = "${path.module}/../notebooks/02_troubleshooter_agent.py"
  path   = "/Shared/02_troubleshooter_agent"
}

# 9. Données dynamiques de calcul Databricks (Version Spark LTS & Node Type minimal)
data "databricks_spark_version" "latest" {
  long_term_support = true
}

data "databricks_node_type" "smallest" {
  local_disk = true
}

# 10. Databricks Workflow automatisé avec tâche On-Failure
resource "databricks_job" "troubleshooter_workflow" {
  name = "workflow-sales-pipeline-troubleshooter"

  job_cluster {
    job_cluster_key = "troubleshooter_cluster"
    new_cluster {
      num_workers   = 0
      spark_version = data.databricks_spark_version.latest.id
      node_type_id  = data.databricks_node_type.smallest.id
      spark_conf = {
        "spark.master"                     = "local[*]"
        "spark.databricks.cluster.profile" = "singleNode"
      }
      custom_tags = {
        "ResourceClass" = "SingleNode"
      }
    }
  }

  task {
    task_key        = "ingest_sales_pipeline"
    job_cluster_key = "troubleshooter_cluster"
    notebook_task {
      notebook_path = databricks_notebook.sales_pipeline.path
    }
  }

  task {
    task_key = "troubleshoot_on_failure"
    depends_on {
      task_key = "ingest_sales_pipeline"
    }
    run_if          = "AT_LEAST_ONE_FAILED"
    job_cluster_key = "troubleshooter_cluster"
    notebook_task {
      notebook_path = databricks_notebook.troubleshooter_agent.path
      base_parameters = {
        "foundry_endpoint"  = azurerm_cognitive_account.ai_foundry.endpoint
        "foundry_api_key"   = azurerm_cognitive_account.ai_foundry.primary_access_key
        "ai_model_name"     = azurerm_cognitive_deployment.ai_model.name
        "pipeline_name"     = "01_ingest_sales_pipeline"
        "azure_devops_org"  = "khalid-dataops"
        "azure_devops_proj" = "khalid-dataops"
        "azure_devops_pat"  = var.azure_devops_pat
      }
    }
  }
}


