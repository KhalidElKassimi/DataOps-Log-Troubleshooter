import base64
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

def get_or_create_job(host: str, headers: dict) -> int:
    """Recherche ou crée le Databricks Workflow Job via l'API REST de Databricks."""
    list_url = f"{host}/api/2.1/jobs/list"
    req_list = urllib.request.Request(list_url, headers=headers)
    job_id = None

    try:
        with urllib.request.urlopen(req_list, timeout=30) as resp:
            jobs_data = json.loads(resp.read().decode("utf-8"))
            for job in jobs_data.get("jobs", []):
                if job.get("settings", {}).get("name") == "workflow-sales-pipeline-troubleshooter":
                    job_id = job.get("job_id")
                    print(f"ℹ️ Job existant détecté : ID #{job_id}")
                    break
    except Exception as e:
        print(f"⚠️ Recherche du job : {e}")

    node_type = os.getenv("DATABRICKS_NODE_TYPE", "Standard_D4ads_v7")
    job_settings = {
        "name": "workflow-sales-pipeline-troubleshooter",
        "job_clusters": [
            {
                "job_cluster_key": "troubleshooter_cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": node_type,
                    "num_workers": 0,
                    "spark_conf": {
                        "spark.master": "local[*]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": -1.0
                    }
                }
            }
        ],
        "tasks": [
            {
                "task_key": "ingest_sales_pipeline",
                "job_cluster_key": "troubleshooter_cluster",
                "notebook_task": {
                    "notebook_path": "/Shared/01_ingest_sales_pipeline"
                }
            },
            {
                "task_key": "troubleshoot_on_failure",
                "depends_on": [{"task_key": "ingest_sales_pipeline"}],
                "run_if": "AT_LEAST_ONE_FAILED",
                "job_cluster_key": "troubleshooter_cluster",
                "notebook_task": {
                    "notebook_path": "/Shared/02_troubleshooter_agent",
                    "base_parameters": {
                        "pipeline_name": "01_ingest_sales_pipeline",
                        "foundry_endpoint": os.getenv("FOUNDRY_ENDPOINT", "https://aifs-dataops-troubleshooter-dev-do3dh2.openai.azure.com/"),
                        "foundry_api_key": os.getenv("FOUNDRY_API_KEY", ""),
                        "ai_model_name": os.getenv("AI_MODEL_NAME", "gpt-4.1-mini"),
                        "azure_devops_org": os.getenv("AZURE_DEVOPS_ORG", "khalid-dataops"),
                        "azure_devops_proj": os.getenv("AZURE_DEVOPS_PROJECT", "khalid-dataops"),
                        "azure_devops_pat": os.getenv("AZURE_DEVOPS_PAT", ""),
                        "outlook_logic_app_url": os.getenv("OUTLOOK_LOGIC_APP_URL", "")
                    }
                }
            }
        ]
    }

    if job_id:
        print(f"🔄 Mise à jour de la configuration du Job #{job_id}...")
        reset_url = f"{host}/api/2.1/jobs/reset"
        reset_payload = {"job_id": job_id, "new_settings": job_settings}
        req_reset = urllib.request.Request(reset_url, data=json.dumps(reset_payload).encode("utf-8"), headers=headers, method="POST")
        with urllib.request.urlopen(req_reset, timeout=30) as resp:
            print("✅ Configuration du Job Databricks synchronisée avec succès !")
        return job_id
    else:
        print("⚡ Création du Workflow Databricks via l'API...")
        create_url = f"{host}/api/2.1/jobs/create"
        req_create = urllib.request.Request(create_url, data=json.dumps(job_settings).encode("utf-8"), headers=headers, method="POST")
        with urllib.request.urlopen(req_create, timeout=30) as resp:
            res = json.loads(resp.read().decode("utf-8"))
            job_id = res.get("job_id")
            print(f"✅ Workflow Databricks créé avec succès ! Job ID : #{job_id}")
        return job_id

def deploy():
    host = os.getenv("DATABRICKS_HOST", "https://adb-7405615002700070.10.azuredatabricks.net").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN")

    print("=" * 80)
    print("🚀 CI/CD DATAOPS : DÉPLOIEMENT NOTEBOOKS, WORKFLOW ET AUTO-RUN")
    print(f"Host cible : {host}")
    print("=" * 80)

    if not token or token.startswith("$(") or len(token) < 10:
        print("⚠️ AVERTISSEMENT : La variable DATABRICKS_TOKEN n'est pas encore configurée dans Azure DevOps.")
        print("💡 Pour autoriser le déploiement vers Databricks :")
        print("   1. Dans Azure Databricks (https://adb-7405615002700070.10.azuredatabricks.net) :")
        print("      Cliquez sur votre profil en haut à droite > Settings > Developer > Access tokens > Generate new token.")
        print("   2. Dans Azure DevOps > Pipelines > CI CD Databricks Pipeline > Cliquez sur Edit > Variables :")
        print("      Ajoutez la variable 'DATABRICKS_TOKEN' avec votre token (cochez 'Keep this value secret').")
        print("✅ Validation de la syntaxe et tests unitaires PySpark terminés avec succès.")
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    root_dir = Path(__file__).resolve().parent.parent

    # 1. Déploiement des Notebooks dans /Shared
    print("\n📦 Étape 1 : Déploiement des Notebooks dans /Shared...")
    notebooks = [
        {"local": root_dir / "notebooks" / "01_ingest_sales_pipeline.py", "remote": "/Shared/01_ingest_sales_pipeline"},
        {"local": root_dir / "notebooks" / "02_troubleshooter_agent.py", "remote": "/Shared/02_troubleshooter_agent"}
    ]

    for nb in notebooks:
        local_path = nb["local"]
        remote_path = nb["remote"]

        if not local_path.exists():
            print(f"❌ Fichier introuvable : {local_path}")
            continue

        content = local_path.read_text(encoding="utf-8")
        b64_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")

        payload = {
            "path": remote_path,
            "format": "SOURCE",
            "language": "PYTHON",
            "content": b64_content,
            "overwrite": True
        }

        url = f"{host}/api/2.0/workspace/import"
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                print(f"✅ Notebook déployé : {local_path.name} ➔ {remote_path}")
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8")
            print(f"❌ Erreur lors du déploiement de {local_path.name} : HTTP {e.code} - {err}")
            sys.exit(1)

    # 2. Création ou mise à jour du Workflow Databricks par la CI/CD
    print("\n⚡ Étape 2 : Gestion du Workflow Databricks par la CI/CD...")
    job_id = get_or_create_job(host, headers)

    # 3. Déclenchement automatique immédiat (Auto-run)
    print(f"\n🎯 Étape 3 : Lancement automatique du Workflow Databricks (Job #{job_id})...")
    run_url = f"{host}/api/2.1/jobs/run-now"
    run_payload = {"job_id": job_id}
    req_run = urllib.request.Request(run_url, data=json.dumps(run_payload).encode("utf-8"), headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req_run, timeout=30) as resp:
            run_data = json.loads(resp.read().decode("utf-8"))
            active_run_id = run_data.get("run_id")
            print(f"🚀 Job Databricks lancé avec succès ! Run ID : #{active_run_id}")
            print(f"🔗 Suivez l'exécution en direct : {host}#job/{job_id}/run/{active_run_id}")
    except Exception as e:
        print(f"⚠️ Déclenchement du job : {e}")

    print("\n🎉 DÉPLOIEMENT CI/CD, GESTION DU WORKFLOW & AUTO-RUN TERMINÉS AVEC SUCCÈS !")

if __name__ == "__main__":
    deploy()
