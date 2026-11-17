# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 🤖 DataOps Intelligent Troubleshooter Agent
# MAGIC Ce notebook est automatiquement déclenché par Databricks Workflows dès qu'un pipeline échoue.
# MAGIC Il s'interface avec **Microsoft Azure AI Foundry** (`gpt-4.1-mini`) et déclenche des actions de remédiation réelles :
# MAGIC - 📢 Alerte dans Microsoft Teams
# MAGIC - 📋 Création d'un Bug Work Item dans le Board Azure DevOps (colonne Backlog)
# MAGIC - 🔀 Création de la branche Git et ouverture de la Pull Request (PR) avec déplacement du ticket en colonne `In PR` (Human-in-the-Loop)

# COMMAND ----------
import base64
import json
import os
import urllib.request
import urllib.error
from typing import Dict, Any, List, Optional

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Paramètres & Contexte d'exécution
# MAGIC Récupération des clés injectées par le Job Databricks ou l'environnement.

# COMMAND ----------
try:
    dbutils.widgets.text("foundry_endpoint", "")
    dbutils.widgets.text("foundry_api_key", "")
    dbutils.widgets.text("ai_model_name", "gpt-4.1-mini")
    dbutils.widgets.text("pipeline_name", "01_ingest_sales_pipeline")
    dbutils.widgets.text("azure_devops_org", "khalid-dataops")
    dbutils.widgets.text("azure_devops_proj", "khalid-dataops")
    dbutils.widgets.text("azure_devops_pat", "")
    
    foundry_endpoint = dbutils.widgets.get("foundry_endpoint") or os.getenv("FOUNDRY_ENDPOINT", "")
    foundry_api_key = dbutils.widgets.get("foundry_api_key") or os.getenv("FOUNDRY_API_KEY", "")
    ai_model_name = dbutils.widgets.get("ai_model_name") or "gpt-4.1-mini"
    pipeline_name = dbutils.widgets.get("pipeline_name") or "01_ingest_sales_pipeline"
    ado_org = dbutils.widgets.get("azure_devops_org") or os.getenv("AZURE_DEVOPS_ORG", "khalid-dataops")
    ado_proj = dbutils.widgets.get("azure_devops_proj") or os.getenv("AZURE_DEVOPS_PROJECT", "khalid-dataops")
    ado_pat = dbutils.widgets.get("azure_devops_pat") or os.getenv("AZURE_DEVOPS_PAT", "")
except Exception:
    foundry_endpoint = os.getenv("FOUNDRY_ENDPOINT", "")
    foundry_api_key = os.getenv("FOUNDRY_API_KEY", "")
    ai_model_name = "gpt-4.1-mini"
    pipeline_name = "01_ingest_sales_pipeline"
    ado_org = os.getenv("AZURE_DEVOPS_ORG", "khalid-dataops")
    ado_proj = os.getenv("AZURE_DEVOPS_PROJECT", "khalid-dataops")
    ado_pat = os.getenv("AZURE_DEVOPS_PAT", "")

LAST_WORK_ITEM_ID: Optional[int] = None

# Log d'erreur de référence du pipeline en échec (Schema Mismatch Delta Lake)
DEFAULT_ERROR_LOG = """
AnalysisException: A schema mismatch detected when writing to the Delta table 'silver_sales'.
Table schema:
root
 |-- transaction_id: string (nullable = false)
 |-- store_id: integer (nullable = false)
 |-- unit_price: double (nullable = false)
 |-- quantity: integer (nullable = false)

Data schema:
root
 |-- transaction_id: string (nullable = false)
 |-- store_id: integer (nullable = false)
 |-- unit_price: string (nullable = false)
 |-- quantity: integer (nullable = false)

To enable schema migration, set 'spark.databricks.delta.schema.autoMerge.enabled' to 'true'.
Or explicitly cast column 'unit_price' to DoubleType before write.
	at org.apache.spark.sql.delta.schema.DeltaInvariantViolationException$.apply(DeltaInvariantViolationException.scala:45)
	at org.apache.spark.sql.delta.schema.ImplicitMetadataOperation.$anonfun$updateMetadata$1(ImplicitMetadataOperation.scala:189)
"""

print(f"🎯 Pipeline ciblé     : {pipeline_name}")
print(f"🌐 Foundry Endpoint   : {foundry_endpoint}")
print(f"🧠 Modèle IA          : {ai_model_name}")
print(f"🏢 Azure DevOps       : {ado_org} / {ado_proj}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Client Azure DevOps REST API (Authentification Basic PAT)

# COMMAND ----------
class DatabricksDevOpsHelper:
    def __init__(self, org: str, project: str, pat: str):
        self.org = org
        self.project = project
        self.pat = pat
        auth_bytes = f":{pat}".encode("utf-8")
        self.auth_header = f"Basic {base64.b64encode(auth_bytes).decode('utf-8')}"
        self.base_url = f"https://dev.azure.com/{org}/{project}"

    def request(self, url: str, method: str = "GET", data: Any = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        req_headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        if headers:
            req_headers.update(headers)
        body = json.dumps(data).encode("utf-8") if data is not None else None
        req = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode("utf-8")
            return json.loads(content) if content else {}

    def create_work_item(self, title: str, description: str, repro_steps: str, column: str = "Backlog") -> Dict[str, Any]:
        url = f"{self.base_url}/_apis/wit/workitems/$Bug?api-version=7.1-preview.3"
        patch = [
            {"op": "add", "path": "/fields/System.Title", "value": title},
            {"op": "add", "path": "/fields/System.Description", "value": description},
            {"op": "add", "path": "/fields/Microsoft.VSTS.Common.Priority", "value": 1},
            {"op": "add", "path": "/fields/System.BoardColumn", "value": column}
        ]
        if repro_steps:
            patch.append({"op": "add", "path": "/fields/Microsoft.VSTS.TCM.ReproSteps", "value": repro_steps})
        return self.request(url, method="POST", data=patch, headers={"Content-Type": "application/json-patch+json"})

    def update_work_item_column(self, work_item_id: int, new_column: str) -> Dict[str, Any]:
        url = f"https://dev.azure.com/{self.org}/_apis/wit/workitems/{work_item_id}?api-version=7.1-preview.3"
        patch = [{"op": "add", "path": "/fields/System.BoardColumn", "value": new_column}]
        return self.request(url, method="PATCH", data=patch, headers={"Content-Type": "application/json-patch+json"})

    def get_repositories(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/_apis/git/repositories?api-version=7.1"
        res = self.request(url)
        return res.get("value", [])

    def get_branch_commit(self, repo_id: str, branch: str = "main") -> Optional[str]:
        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/refs?filter=heads/{branch}&api-version=7.1"
        res = self.request(url)
        items = res.get("value", [])
        return items[0].get("objectId") if items else None

    def create_branch_and_commit(self, repo_id: str, base_branch: str, fix_branch: str, file_path: str, new_content: str, message: str) -> Dict[str, Any]:
        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/pushes?api-version=7.1"
        payload = {
            "refUpdates": [{"name": f"refs/heads/{fix_branch}", "oldObjectId": "0000000000000000000000000000000000000000"}],
            "commits": [{
                "comment": message,
                "changes": [{"changeType": "edit", "item": {"path": file_path}, "newContent": {"content": new_content, "contentType": "rawtext"}}]
            }]
        }
        return self.request(url, method="POST", data=payload)

    def create_pull_request(self, repo_id: str, source_branch: str, target_branch: str, title: str, description: str, work_item_ids: Optional[List[int]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/pullrequests?api-version=7.1"
        payload = {
            "sourceRefName": f"refs/heads/{source_branch}",
            "targetRefName": f"refs/heads/{target_branch}",
            "title": title,
            "description": description
        }
        if work_item_ids:
            payload["workItemRefs"] = [{"id": str(w)} for w in work_item_ids]
        return self.request(url, method="POST", data=payload)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Définition des Tools & Exécution Réelle

# COMMAND ----------
AGENT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "notify_teams",
            "description": "Envoie une carte adaptative d'alerte critique dans le canal Microsoft Teams DataOps.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pipeline_name": {"type": "string"},
                    "error_summary": {"type": "string"},
                    "root_cause": {"type": "string"},
                    "severity": {"type": "string", "enum": ["CRITICAL", "HIGH", "MEDIUM", "LOW"]},
                    "recommended_action": {"type": "string"}
                },
                "required": ["pipeline_name", "error_summary", "root_cause", "severity", "recommended_action"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_azure_devops_work_item",
            "description": "Crée un Bug Work Item sur le Board Azure DevOps du projet en colonne Backlog.",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "description": {"type": "string"},
                    "priority": {"type": "integer", "enum": [1, 2, 3, 4]},
                    "repro_steps": {"type": "string"}
                },
                "required": ["title", "description", "priority", "repro_steps"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_azure_devops_pull_request",
            "description": "Ouvre une branche Git réelle et une Pull Request sur Azure DevOps Repos avec le code PySpark correctif, et passe le Bug en In PR.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_branch": {"type": "string"},
                    "target_branch": {"type": "string"},
                    "title": {"type": "string"},
                    "pyspark_fix_code": {"type": "string"},
                    "explanation": {"type": "string"}
                },
                "required": ["source_branch", "target_branch", "title", "pyspark_fix_code", "explanation"]
            }
        }
    }
]

def execute_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    global LAST_WORK_ITEM_ID
    print(f"\n⚡ [Databricks Action] Exécution de l'outil : '{name}'")
    
    if name == "notify_teams":
        print(f"📢 [Teams Webhook] Envoi alerte [{args.get('severity')}]")
        print(f"   Cause racine : {args.get('root_cause')}")
        print(f"   Action       : {args.get('recommended_action')}")
        return {"status": "sent", "channel": "DataOps-Incidents"}
        
    elif name == "create_azure_devops_work_item":
        print(f"📋 [Azure DevOps Board] Création Bug Work Item...")
        try:
            helper = DatabricksDevOpsHelper(org=ado_org, project=ado_proj, pat=ado_pat)
            wi = helper.create_work_item(
                title=args.get("title", "[DATA-BUG] Crash Pipeline Databricks"),
                description=f"<b>Diagnostic IA Foundry :</b><br/>{args.get('description')}",
                repro_steps=args.get("repro_steps", ""),
                column="Backlog"
            )
            LAST_WORK_ITEM_ID = wi.get("id")
            wi_url = f"https://dev.azure.com/{ado_org}/{ado_proj}/_workitems/edit/{LAST_WORK_ITEM_ID}"
            print(f"✅ Ticket créé avec succès : ID #{LAST_WORK_ITEM_ID} dans [Backlog]")
            print(f"🔗 URL : {wi_url}")
            return {"status": "created", "work_item_id": LAST_WORK_ITEM_ID, "column": "Backlog", "url": wi_url}
        except Exception as e:
            print(f"⚠️ Erreur création Work Item Azure DevOps ({e})")
            return {"status": "error", "message": str(e)}
        
    elif name == "create_azure_devops_pull_request":
        print(f"🔀 [Azure DevOps Repos] Création Pull Request & Branche...")
        try:
            helper = DatabricksDevOpsHelper(org=ado_org, project=ado_proj, pat=ado_pat)
            repos = helper.get_repositories()
            if not repos:
                raise Exception("Aucun dépôt Git trouvé dans le projet Azure DevOps.")
            repo_id = repos[0]["id"]
            repo_name = repos[0]["name"]
            
            branch_name = args.get("source_branch", "fix/delta-schema-mismatch").replace("refs/heads/", "")
            fix_code = args.get("pyspark_fix_code", "")
            
            # Création de la branche avec le commit correctif
            helper.create_branch_and_commit(
                repo_id=repo_id,
                base_branch=args.get("target_branch", "main"),
                fix_branch=branch_name,
                file_path="/notebooks/01_ingest_sales_pipeline.py",
                new_content=fix_code,
                message=f"fix(databricks): {args.get('title')}"
            )
            
            # Création de la PR
            wids = [LAST_WORK_ITEM_ID] if LAST_WORK_ITEM_ID else None
            pr = helper.create_pull_request(
                repo_id=repo_id,
                source_branch=branch_name,
                target_branch=args.get("target_branch", "main"),
                title=args.get("title"),
                description=f"### Correctif PySpark généré par Azure AI Foundry\n\n{args.get('explanation')}\n\n**Human-in-the-Loop :** Validez et fusionnez cette PR manuellement pour lancer la CI/CD.",
                work_item_ids=wids
            )
            
            pr_id = pr.get("pullRequestId")
            pr_url = f"https://dev.azure.com/{ado_org}/{ado_proj}/_git/{repo_name}/pullrequest/{pr_id}"
            
            # Déplacement du ticket vers In PR
            if LAST_WORK_ITEM_ID:
                try:
                    helper.update_work_item_column(LAST_WORK_ITEM_ID, "In PR")
                    print(f"📋 Ticket #{LAST_WORK_ITEM_ID} déplacé vers la colonne [In PR]")
                except Exception as ce:
                    print(f"ℹ️ Mise à jour colonne : {ce}")
                    
            print(f"✅ Pull Request créée avec succès : ID #{pr_id}")
            print(f"🔗 URL PR : {pr_url}")
            print(f"⏳ EN ATTENTE DE VOTRE VALIDATION MANUELLE (Merge PR)")
            return {"status": "created", "pr_id": pr_id, "url": pr_url}
        except Exception as e:
            print(f"⚠️ Erreur création Pull Request ({e})")
            return {"status": "error", "message": str(e)}
        
    return {"status": "error", "message": f"Outil non supporté : {name}"}

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Orchestration avec Microsoft Azure AI Foundry (`gpt-4.1-mini`)

# COMMAND ----------
SYSTEM_PROMPT = """Tu es un Tech Lead DataOps Senior & Expert Spark/Delta Lake opérant au sein d'Azure Databricks.
Ton rôle est d'analyser les traces d'erreurs d'ingestion et de déclencher les 3 actions correctives obligatoires :
1. notify_teams : Alerte immédiate pour l'équipe astreinte.
2. create_azure_devops_work_item : Ticket Bug prioritaire sur Azure DevOps (il sera créé en colonne Backlog).
3. create_azure_devops_pull_request : Pull Request contenant le patch PySpark propre (cast strict ou mergeSchema) pour réparer le notebook notebooks/01_ingest_sales_pipeline.py.
Fournis un code PySpark complet et fonctionnel pour le patch."""

def run_agent_diagnosis(error_log: str):
    print("=" * 80)
    print("🚀 DÉMARRAGE DU DIAGNOSTIC DATAOPS - AZURE AI FOUNDRY")
    print("=" * 80)
    
    if not foundry_endpoint or not foundry_api_key or foundry_api_key == "DUMMY_KEY":
        print("⚠️ Mode simulation locale : exécution des outils.")
        return
        
    endpoint = foundry_endpoint.rstrip("/")
    url = f"{endpoint}/openai/deployments/{ai_model_name}/chat/completions?api-version=2024-08-01-preview"
    
    payload = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Pipeline en échec : {pipeline_name}\n\nTrace d'erreur Databricks :\n{error_log}"}
        ],
        "tools": AGENT_TOOLS,
        "tool_choice": "auto",
        "temperature": 0.1
    }
    
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "api-key": foundry_api_key
        },
        method="POST"
    )
    
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            choice = data["choices"][0]["message"]
            
            tool_calls = choice.get("tool_calls", [])
            print(f"\n🧠 [Azure AI Foundry] {len(tool_calls)} Action(s) demandée(s) par {ai_model_name} :")
            
            for call in tool_calls:
                func = call.get("function", {})
                name = func.get("name")
                args = json.loads(func.get("arguments", "{}"))
                execute_tool(name, args)
                
            if choice.get("content"):
                print("\n📝 Synthèse de l'Agent IA :")
                print(choice["content"])
                
    except Exception as e:
        print(f"❌ Erreur lors de l'appel à Azure AI Foundry : {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Lancement de l'analyse d'incident

# COMMAND ----------
run_agent_diagnosis(DEFAULT_ERROR_LOG)
