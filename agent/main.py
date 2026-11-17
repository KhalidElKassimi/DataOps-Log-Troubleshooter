import json
import os
import subprocess
from pathlib import Path

from agent.core import DataOpsAgent

def get_foundry_credentials():
    """Récupère l'Endpoint et la clé Foundry depuis l'environnement ou via Azure CLI."""
    endpoint = os.getenv("FOUNDRY_ENDPOINT", "https://aifs-dataops-troubleshooter-dev-do3dh2.openai.azure.com/")
    api_key = os.getenv("FOUNDRY_API_KEY")

    if not api_key:
        try:
            cmd = [
                "az", "cognitiveservices", "account", "keys", "list",
                "--name", "aifs-dataops-troubleshooter-dev",
                "--resource-group", "rg-dataops-troubleshooter-dev",
                "--query", "key1", "-o", "tsv"
            ]
            api_key = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            pass

    return endpoint, api_key

def run():
    print("=" * 80)
    print(" 🚀 DATAOPS TROUBLESHOOTER AGENT - MICROSOFT AZURE AI FOUNDRY ")
    print("=" * 80)

    endpoint, api_key = get_foundry_credentials()

    # Chargement de nos logs réels Databricks
    logs_file = Path(__file__).parent.parent / "data" / "raw_logs" / "databricks_spark_errors.json"
    with open(logs_file, "r", encoding="utf-8") as f:
        incidents = json.load(f)

    # Sélection du premier incident : le crash Delta Schema Mismatch
    incident = incidents[0]
    print(f"\n📂 Ingestion du Log d'incident : {incident['id']}")
    print(f"🔧 Composant                   : {incident['service']}")
    print(f"📄 Notebook Databricks         : {incident['notebook']}")
    print("-" * 80)
    print("Trace brute reçue :")
    print(incident["raw_log"][:280] + "...\n")
    print("🤖 Envoi à l'Agent Microsoft Azure AI Foundry (gpt-4.1-mini) avec Tools (Teams, Boards, Repos PR)...")

    agent = DataOpsAgent(foundry_endpoint=endpoint, api_key=api_key or "DUMMY_KEY", model_name="gpt-4.1-mini")

    if not api_key:
        print("\n⚠️ [Note] Aucune clé API active trouvée. Simulation réaliste du comportement de l'Agent...")
        simulated_tools = [
            {
                "id": "call_teams_01",
                "function": {
                    "name": "notify_teams",
                    "arguments": json.dumps({
                        "severity": "HIGH",
                        "pipeline_name": incident["notebook"],
                        "error_summary": "Incompatibilité de schéma lors de l'écriture Delta Lake dans silver_sales : la colonne unit_price arrive en texte au lieu de double.",
                        "root_cause": "La colonne 'unit_price' du lot entrant est typée en 'string' (Utf8), violant le schéma invariant de la table Delta 'silver_sales' qui attend un 'double'.",
                        "suggested_fix": "1. Caster explicitement la colonne en double dans le DataFrame source avant écriture.\n2. Rejeter les lignes corrompues ('INVALID_PRICE') via un filtre de validation."
                    })
                }
            },
            {
                "id": "call_ado_02",
                "function": {
                    "name": "create_azure_devops_work_item",
                    "arguments": json.dumps({
                        "title": f"[DATA-BUG] Schema Mismatch sur {incident['notebook']} (Table silver_sales)",
                        "severity": "1 - Critical",
                        "repro_steps": f"Le pipeline `{incident['notebook']}` a échoué lors de l'écriture Delta.\nStacktrace: AnalysisException: A schema mismatch detected when writing to the Delta table 'silver_sales'.",
                        "proposed_fix": "# Correctif PySpark pour le notebook :\nfrom pyspark.sql.functions import col\n\ndf_cleaned = df_bad.filter(col('unit_price').rlike('^[0-9]+(\\\\.[0-9]+)?$')) \\\n                    .withColumn('unit_price', col('unit_price').cast('double'))\n\ndf_cleaned.write.format('delta').mode('append').saveAsTable('silver_sales')"
                    })
                }
            },
            {
                "id": "call_pr_03",
                "function": {
                    "name": "create_azure_devops_pull_request",
                    "arguments": json.dumps({
                        "source_branch": "fix/schema-mismatch-silver-sales",
                        "target_branch": "main",
                        "pr_title": f"fix(databricks): Cast unit_price to double and filter bad records in {incident['notebook']}",
                        "pr_description": f"## Résumé de la correction\nCette PR corrige le crash Delta Lake survenu sur `{incident['notebook']}`.\n\n- Filtre les valeurs non numériques sur `unit_price`\n- Caste explicitement le champ en `Double`\n- Lié au Bug Azure DevOps #1042.\n\n**Action requise :** Merci de relire le diff et d'approuver manuellement pour déclencher le déploiement CI/CD.",
                        "file_path": f"notebooks/{incident['notebook']}.py",
                        "fixed_code": "# Correctif PySpark à valider :\nfrom pyspark.sql.functions import col\n\ndf_cleaned = df_bad.filter(col('unit_price').rlike('^[0-9]+(\\\\.[0-9]+)?$')) \\\n                    .withColumn('unit_price', col('unit_price').cast('double'))\n\ndf_cleaned.write.format('delta').mode('append').saveAsTable('silver_sales')"
                    })
                }
            }
        ]
        agent.execute_tools(simulated_tools)
    else:
        result = agent.troubleshoot_and_act(incident["raw_log"], incident["notebook"])
        print(f"\nRésultat d'exécution : {result['status']}")

    print("\n" + "=" * 80)
    print(" ✅ TOUT EST PRÊT : PULL REQUEST CRÉÉE ET EN ATTENTE DE VOTRE VALIDATION MANUELLE ")
    print("=" * 80)

if __name__ == "__main__":
    run()
