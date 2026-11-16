output "resource_group_name" {
  description = "Nom du Resource Group créé"
  value       = azurerm_resource_group.rg.name
}

output "data_factory_name" {
  description = "Nom de l'instance Azure Data Factory"
  value       = azurerm_data_factory.adf.name
}

output "storage_account_name" {
  description = "Nom du Storage Account ADLS Gen2"
  value       = azurerm_storage_account.storage.name
}

output "ai_foundry_endpoint" {
  description = "Point de terminaison (Endpoint) du socle Microsoft Azure AI Foundry"
  value       = azurerm_cognitive_account.ai_foundry.endpoint
}

output "ai_model_deployment_name" {
  description = "Nom du déploiement du modèle d'IA"
  value       = azurerm_cognitive_deployment.ai_model.name
}
