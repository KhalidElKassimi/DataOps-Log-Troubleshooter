variable "location" {
  type        = string
  description = "Région Azure la plus économique avec support complet de Foundry et ADF"
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
