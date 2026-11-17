variable "location" {
  type        = string
  description = "Région Azure la plus économique avec support complet de Foundry et Databricks"
  default     = "eastus"
}

variable "project_name" {
  type        = string
  description = "Préfixe utilisé pour nommer les ressources Azure"
  default     = "dataops-troubleshooter"
}

variable "environment" {
  type        = string
  description = "Environnement cible (dev, test, prod)"
  default     = "dev"
}

variable "azure_devops_pat" {
  type        = string
  description = "Personal Access Token pour Azure DevOps (injecté via terraform.tfvars ignoré par git ou TF_VAR_)"
  sensitive   = true
  default     = ""
}
