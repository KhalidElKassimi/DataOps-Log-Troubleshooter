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

# 2. Génération d'une chaîne aléatoire pour garantir l'unicité mondiale du Storage Account
resource "random_string" "random" {
  length  = 6
  special = false
  upper   = false
}

# 3. Storage Account Azure (nom limité à 24 caractères max par Azure)
resource "azurerm_storage_account" "storage" {
  name                     = "stdataops${var.environment}${random_string.random.result}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Active Data Lake Storage Gen2 (standard data)

  tags = azurerm_resource_group.rg.tags
}

# Conteneur pour nos fichiers bruts (Bronze)
resource "azurerm_storage_container" "raw_data" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = "private"
}

# 4. Azure Data Factory
resource "azurerm_data_factory" "adf" {
  name                = "adf-${var.project_name}-${var.environment}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  identity {
    type = "SystemAssigned"
  }

  tags = azurerm_resource_group.rg.tags
}

# 5. Rôle RBAC : autoriser ADF à lire/écrire dans le Storage Account (sécurité par Managed Identity)
resource "azurerm_role_assignment" "adf_storage_contributor" {
  scope                = azurerm_storage_account.storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# 6. Compte Azure AI Services / OpenAI (Socle de Microsoft Azure AI Foundry)
resource "azurerm_cognitive_account" "ai_foundry" {
  name                  = "aifs-${var.project_name}-${var.environment}"
  location              = azurerm_resource_group.rg.location
  resource_group_name   = azurerm_resource_group.rg.name
  kind                  = "OpenAI"
  sku_name              = "S0"
  custom_subdomain_name = "aifs-${var.project_name}-${var.environment}-${random_string.random.result}"

  tags = azurerm_resource_group.rg.tags
}

# 7. Déploiement du modèle d'IA (GPT-4.1-mini sous GlobalStandard avec quota actif)
resource "azurerm_cognitive_deployment" "ai_model" {
  name                 = "gpt-4.1-mini"
  cognitive_account_id = azurerm_cognitive_account.ai_foundry.id

  model {
    format  = "OpenAI"
    name    = "gpt-4.1-mini"
    version = "2025-04-14"
  }

  scale {
    type     = "GlobalStandard"
    capacity = 10 # 10k TPM (sur les 200k disponibles)
  }
}


