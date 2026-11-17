# Configuration des providers Terraform
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

# Configuration du provider Azure Resource Manager
provider "azurerm" {
  features {}
}

# Configuration du provider Databricks (connecté à notre workspace Azure)
provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.databricks.id
}
