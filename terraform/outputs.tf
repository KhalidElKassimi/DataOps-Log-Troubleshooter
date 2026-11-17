output "resource_group_name" {
  description = "Nom du Resource Group créé"
  value       = azurerm_resource_group.rg.name
}

output "databricks_workspace_name" {
  description = "Nom du Workspace Azure Databricks"
  value       = azurerm_databricks_workspace.databricks.name
}

output "databricks_workspace_url" {
  description = "URL d'accès à l'interface Azure Databricks"
  value       = "https://${azurerm_databricks_workspace.databricks.workspace_url}"
}

output "databricks_notebook_path" {
  description = "Chemin du Notebook d'ingestion déployé dans Databricks via CI/CD"
  value       = "/Shared/01_ingest_sales_pipeline"
}

output "databricks_troubleshooter_notebook_path" {
  description = "Chemin du Notebook Agent Troubleshooter déployé dans Databricks via CI/CD"
  value       = "/Shared/02_troubleshooter_agent"
}

output "databricks_job_id" {
  description = "Identifiant du Databricks Workflow automatisé"
  value       = databricks_job.troubleshooter_workflow.id
}

output "databricks_job_url" {
  description = "URL directe du Job dans Databricks"
  value       = "${azurerm_databricks_workspace.databricks.workspace_url}#job/${databricks_job.troubleshooter_workflow.id}"
}



output "ai_foundry_endpoint" {
  description = "Point de terminaison (Endpoint) du socle Microsoft Azure AI Foundry"
  value       = azurerm_cognitive_account.ai_foundry.endpoint
}

output "ai_model_deployment_name" {
  description = "Nom du déploiement du modèle d'IA"
  value       = azurerm_cognitive_deployment.ai_model.name
}
