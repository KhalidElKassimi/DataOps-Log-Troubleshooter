# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 🤖 DataOps Intelligent Troubleshooter Agent
# MAGIC Ce notebook est automatiquement déclenché par Databricks Workflows dès qu'un pipeline échoue.
# MAGIC Il s'interface avec **Microsoft Azure AI Foundry** (`gpt-4.1-mini`) et déclenche des actions de remédiation réelles :
# MAGIC - 📧 Alerte par e-mail via Outlook
# MAGIC - 📋 Création d'un Bug Work Item dans le Board Azure DevOps (colonne Backlog)
# MAGIC - 🔀 Création de la branche Git et ouverture de la Pull Request (PR) avec déplacement du ticket en colonne `In PR` (Human-in-the-Loop)

# COMMAND ----------
import base64
from html import escape
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
FALLBACK_FOUNDRY_ENDPOINT = "https://aifs-dataops-troubleshooter-dev-do3dh2.openai.azure.com/"

def get_param(widget_name: str, env_name: str, default_val: str) -> str:
    try:
        val = dbutils.widgets.get(widget_name)
        if val and val.strip() and not val.strip().startswith("$("):
            return val.strip()
    except Exception:
        pass
    env_val = os.getenv(env_name)
    if env_val and env_val.strip() and not env_val.strip().startswith("$("):
        return env_val.strip()
    return default_val

try:
    dbutils.widgets.text("foundry_endpoint", FALLBACK_FOUNDRY_ENDPOINT)
    dbutils.widgets.text("foundry_api_key", "")
    dbutils.widgets.text("ai_model_name", "gpt-4.1-mini")
    dbutils.widgets.text("pipeline_name", "01_ingest_sales_pipeline")
    dbutils.widgets.text("azure_devops_org", "khalid-dataops")
    dbutils.widgets.text("azure_devops_proj", "khalid-dataops")
    dbutils.widgets.text("azure_devops_pat", "")
    dbutils.widgets.text("outlook_logic_app_url", "")
except Exception:
    pass

foundry_endpoint = get_param("foundry_endpoint", "FOUNDRY_ENDPOINT", FALLBACK_FOUNDRY_ENDPOINT)
foundry_api_key = get_param("foundry_api_key", "FOUNDRY_API_KEY", "")
ai_model_name = get_param("ai_model_name", "AI_MODEL_NAME", "gpt-4.1-mini")
pipeline_name = get_param("pipeline_name", "PIPELINE_NAME", "01_ingest_sales_pipeline")
ado_org = get_param("azure_devops_org", "AZURE_DEVOPS_ORG", "khalid-dataops")
ado_proj = get_param("azure_devops_proj", "AZURE_DEVOPS_PROJECT", "khalid-dataops")
ado_pat = get_param("azure_devops_pat", "AZURE_DEVOPS_PAT", "")
outlook_logic_app_url = get_param("outlook_logic_app_url", "OUTLOOK_LOGIC_APP_URL", "")

LAST_WORK_ITEM_ID: Optional[int] = None
LAST_WORK_ITEM_URL: Optional[str] = None
LAST_PR_ID: Optional[int] = None
LAST_PR_URL: Optional[str] = None

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
            content = resp.read()
            if not content:
                return {}
            try:
                result = json.loads(content)
            except (json.JSONDecodeError, UnicodeDecodeError):
                raise RuntimeError(
                    f"Azure DevOps a renvoyé une réponse non JSON (HTTP {resp.status}). "
                    "Vérifiez la validité du PAT et son accès au projet."
                ) from None
            if not isinstance(result, dict):
                raise RuntimeError("Format de réponse Azure DevOps inattendu : objet JSON attendu.")
            return result

    def create_work_item(self, title: str, description: str, repro_steps: str, column: str = "Backlog", assigned_to: str = "elkassimi.khalid07@outlook.com") -> Dict[str, Any]:
        # Supporte à la fois le template 'Basic' (type Issue) et 'Scrum/Agile' (type Bug)
        last_err = None
        for item_type in ["Issue", "Bug"]:
            try:
                url = f"{self.base_url}/_apis/wit/workitems/${item_type}?api-version=7.1-preview.3"
                full_desc = f"{description}<br/><br/><strong>Étapes de reproduction / Trace :</strong><pre>{repro_steps}</pre>"
                patch = [
                    {"op": "add", "path": "/fields/System.Title", "value": title},
                    {"op": "add", "path": "/fields/System.Description", "value": full_desc}
                ]
                if assigned_to:
                    patch.append({"op": "add", "path": "/fields/System.AssignedTo", "value": assigned_to})
                item = self.request(url, method="POST", data=patch, headers={"Content-Type": "application/json-patch+json"})
                if not item.get("id"):
                    raise RuntimeError("Azure DevOps n'a pas confirmé la création du ticket : ID absent.")
                return item
            except urllib.error.HTTPError as e:
                if e.code not in (400, 404):
                    raise
                last_err = e
                continue
        raise last_err or Exception("Impossible de créer le Work Item.")

    def update_work_item_column(self, work_item_id: int, new_column: str) -> Dict[str, Any]:
        # Récupération des champs pour détecter le nom exact de la colonne Kanban
        try:
            get_url = f"https://dev.azure.com/{self.org}/{self.project}/_apis/wit/workitems/{work_item_id}?$expand=all&api-version=7.1"
            wi_data = self.request(get_url)
            fields = wi_data.get("fields", {})
            kanban_field = None
            for k in fields:
                if "Kanban.Column" in k and not k.endswith(".Done"):
                    kanban_field = k
                    break
            
            patch = []
            if kanban_field:
                patch.append({"op": "add", "path": f"/fields/{kanban_field}", "value": new_column})
            else:
                patch.append({"op": "add", "path": "/fields/System.BoardColumn", "value": new_column})
            
            url = f"https://dev.azure.com/{self.org}/{self.project}/_apis/wit/workitems/{work_item_id}?api-version=7.1"
            return self.request(url, method="PATCH", data=patch, headers={"Content-Type": "application/json-patch+json"})
        except Exception as e:
            print(f"⚠️ Erreur lors du déplacement Kanban : {e}")
            return {"status": "error", "message": str(e)}

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
        base_commit = self.get_branch_commit(repo_id, base_branch)
        if not base_commit:
            raise Exception(f"Branche de base '{base_branch}' introuvable.")

        # Étape 1 : Créer la branche Git pointant sur le commit de main
        ref_url = f"{self.base_url}/_apis/git/repositories/{repo_id}/refs?api-version=7.1"
        ref_payload = [
            {
                "name": f"refs/heads/{fix_branch}",
                "oldObjectId": "0000000000000000000000000000000000000000",
                "newObjectId": base_commit
            }
        ]
        try:
            self.request(ref_url, method="POST", data=ref_payload)
            print(f"✅ Branche '{fix_branch}' créée à partir de '{base_branch}' ({base_commit[:8]}).")
        except Exception as e:
            print(f"ℹ️ Note sur la création de branche : {e}")

        # Étape 2 : Pousser le commit contenant le patch PySpark
        push_url = f"{self.base_url}/_apis/git/repositories/{repo_id}/pushes?api-version=7.1"
        push_payload = {
            "refUpdates": [
                {
                    "name": f"refs/heads/{fix_branch}",
                    "oldObjectId": base_commit
                }
            ],
            "commits": [
                {
                    "comment": message,
                    "changes": [
                        {
                            "changeType": "edit",
                            "item": {"path": file_path},
                            "newContent": {
                                "content": new_content,
                                "contentType": "rawtext"
                            }
                        }
                    ]
                }
            ]
        }
        res = self.request(push_url, method="POST", data=push_payload)
        print(f"✅ Commit de correction poussé avec succès sur '{fix_branch}' !")
        return res

    def create_pull_request(self, repo_id: str, source_branch: str, target_branch: str, title: str, description: str, work_item_ids: Optional[List[int]] = None, reviewer_id: Optional[str] = "498095d0-edd3-6a67-ae2f-c39ef05bb2d5") -> Dict[str, Any]:
        url = f"{self.base_url}/_apis/git/repositories/{repo_id}/pullrequests?api-version=7.1"
        payload = {
            "sourceRefName": f"refs/heads/{source_branch}",
            "targetRefName": f"refs/heads/{target_branch}",
            "title": title,
            "description": description
        }
        if work_item_ids:
            payload["workItemRefs"] = [{"id": str(w)} for w in work_item_ids]
        if reviewer_id:
            payload["reviewers"] = [{"id": reviewer_id}]
        return self.request(url, method="POST", data=payload)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Définition des Tools & Exécution Réelle

# COMMAND ----------
AGENT_TOOLS = [
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
    },
    {
        "type": "function",
        "function": {
            "name": "send_outlook_email",
            "description": "Envoie un véritable email d'alerte complet via le connecteur Microsoft Outlook à l'ingénieur d'astreinte (elkassimi.khalid07@outlook.com) contenant le diagnostic IA, la cause racine, et les liens cliquables vers le Bug et la PR.",
            "parameters": {
                "type": "object",
                "properties": {
                    "subject": {"type": "string", "description": "Sujet officiel du mail d'astreinte"},
                    "root_cause": {"type": "string", "description": "Explication claire et détaillée de la cause racine"},
                    "recommended_action": {"type": "string", "description": "Action recommandée pour réparer"}
                },
                "required": ["subject", "root_cause", "recommended_action"]
            }
        }
    }
]

def execute_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    global LAST_WORK_ITEM_ID, LAST_WORK_ITEM_URL, LAST_PR_ID, LAST_PR_URL
    print(f"\n⚡ [Databricks Action] Exécution de l'outil : '{name}'")
    
    if name == "create_azure_devops_work_item":
        print(f"📋 [Azure DevOps Board] Création Bug Work Item...")
        try:
            helper = DatabricksDevOpsHelper(org=ado_org, project=ado_proj, pat=ado_pat)
            wi = helper.create_work_item(
                title=args.get("title", "[DATA-BUG] Crash Pipeline Databricks"),
                description=f"<b>Diagnostic IA Foundry :</b><br/>{args.get('description')}",
                repro_steps=args.get("repro_steps", ""),
                column="Backlog",
                assigned_to="elkassimi.khalid07@outlook.com"
            )
            LAST_WORK_ITEM_ID = wi.get("id")
            LAST_WORK_ITEM_URL = f"https://dev.azure.com/{ado_org}/{ado_proj}/_workitems/edit/{LAST_WORK_ITEM_ID}"
            print(f"✅ Ticket créé avec succès : ID #{LAST_WORK_ITEM_ID} dans [Backlog]")
            print(f"🔗 URL : {LAST_WORK_ITEM_URL}")
            return {"status": "created", "work_item_id": LAST_WORK_ITEM_ID, "column": "Backlog", "url": LAST_WORK_ITEM_URL}
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
            
            LAST_PR_ID = pr.get("pullRequestId")
            LAST_PR_URL = f"https://dev.azure.com/{ado_org}/{ado_proj}/_git/{repo_name}/pullrequest/{LAST_PR_ID}"
            
            # Déplacement du ticket vers In PR
            if LAST_WORK_ITEM_ID:
                try:
                    helper.update_work_item_column(LAST_WORK_ITEM_ID, "In PR")
                    print(f"📋 Ticket #{LAST_WORK_ITEM_ID} déplacé vers la colonne [In PR]")
                except Exception as ce:
                    print(f"ℹ️ Mise à jour colonne : {ce}")
                    
            print(f"✅ Pull Request créée avec succès : ID #{LAST_PR_ID}")
            print(f"🔗 URL PR : {LAST_PR_URL}")
            print(f"⏳ EN ATTENTE DE VOTRE VALIDATION MANUELLE (Merge PR)")
            return {"status": "created", "pr_id": LAST_PR_ID, "url": LAST_PR_URL}
        except Exception as e:
            print(f"⚠️ Erreur création Pull Request ({e})")
            return {"status": "error", "message": str(e)}

    elif name == "send_outlook_email":
        wi_url = LAST_WORK_ITEM_URL or (f"https://dev.azure.com/{ado_org}/{ado_proj}/_workitems/edit/{LAST_WORK_ITEM_ID}" if LAST_WORK_ITEM_ID else f"https://dev.azure.com/{ado_org}/{ado_proj}/_workitems")
        pr_url = LAST_PR_URL or f"https://dev.azure.com/{ado_org}/{ado_proj}/_git/{ado_proj}/pullrequests"
        recipient = "elkassimi.khalid07@outlook.com"
        subject = args.get("subject") or f"[DataOps] Incident sur {pipeline_name}"

        if not outlook_logic_app_url:
            message = (
                "Aucun mail envoyé : configurez OUTLOOK_LOGIC_APP_URL comme variable secrète "
                "dans Azure DevOps Pipelines, puis redéployez le job Databricks."
            )
            print(f"⚠️ {message}")
            return {"status": "error", "message": message}

        mail_body = (
            f"Incident détecté sur le pipeline <b>{escape(pipeline_name)}</b>.<br/><br/>"
            f"<b>Cause racine :</b><br/>{escape(str(args.get('root_cause') or 'Non renseignée'))}<br/><br/>"
            f"<b>Correctif proposé :</b><br/>{escape(str(args.get('recommended_action') or 'Non renseigné'))}"
        )
        if LAST_WORK_ITEM_URL or LAST_WORK_ITEM_ID:
            mail_body += f"<br/><br/><a href='{escape(wi_url)}'>Consulter le ticket Azure DevOps</a>"
        if LAST_PR_URL:
            mail_body += f"<br/><br/><a href='{escape(pr_url)}'>Examiner la Pull Request</a>"
        mail_payload = {"to": recipient, "subject": subject, "body": mail_body}

        try:
            m_req = urllib.request.Request(
                outlook_logic_app_url,
                data=json.dumps(mail_payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(m_req, timeout=60) as m_resp:
                status_code = m_resp.status
                result = json.load(m_resp) if status_code == 200 else None
        except urllib.error.HTTPError as error:
            message = f"La Logic App Outlook a répondu HTTP {error.code}."
        except (urllib.error.URLError, TimeoutError, ValueError):
            message = "Impossible d'obtenir une confirmation de la Logic App Outlook."
        else:
            if status_code == 200 and isinstance(result, dict) and result.get("status") == "success":
                print(f"✅ Envoi confirmé par le connecteur Outlook pour {recipient}.")
                return {"status": "sent", "recipient": recipient, "subject": subject, "work_item_url": wi_url, "pr_url": pr_url}
            message = f"La Logic App Outlook n'a pas confirmé l'envoi (HTTP {status_code})."

        print(f"⚠️ {message}")
        return {"status": "error", "message": message}
        
    return {"status": "error", "message": f"Outil non supporté : {name}"}

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Orchestration avec Microsoft Azure AI Foundry (`gpt-4.1-mini`)

# COMMAND ----------
SYSTEM_PROMPT = """Tu es un Tech Lead DataOps Senior & Expert Spark/Delta Lake opérant au sein d'Azure Databricks.
Ton rôle est d'analyser les traces d'erreurs d'ingestion et d'exécuter OBLIGATOIREMENT ET SUCCESSIVEMENT LES ACTIONS SUIVANTES :
1. create_azure_devops_work_item : Crée le ticket de Bug dans la colonne Backlog d'Azure DevOps Boards.
2. create_azure_devops_pull_request : Crée la branche Git 'fix/schema-mismatch-unit-price', applique le code PySpark corrigé (cast strict de 'unit_price' vers double) dans /notebooks/01_ingest_sales_pipeline.py, et ouvre la PR liée au Bug.
3. send_outlook_email : Envoie l'email officiel Microsoft Outlook à l'ingénieur d'astreinte (elkassimi.khalid07@outlook.com) contenant le diagnostic complet, la cause racine, et les liens directs vers le Bug et la PR créés.

Tu DOIS impérativement enchaîner ces outils jusqu'à ce que tous soient exécutés."""

def run_agent_diagnosis(error_log: str):
    print("=" * 80)
    print("🚀 DÉMARRAGE DU DIAGNOSTIC DATAOPS - AZURE AI FOUNDRY")
    print("=" * 80)
    
    if not foundry_endpoint:
        raise RuntimeError("FOUNDRY_ENDPOINT manquant : configurez l'endpoint Azure OpenAI.")
    if not foundry_api_key or foundry_api_key == "DUMMY_KEY":
        raise RuntimeError(
            "FOUNDRY_API_KEY manquante : configurez-la dans Azure DevOps Pipelines, "
            "relancez le déploiement, puis exécutez le job Databricks."
        )
    if not ado_pat:
        raise RuntimeError(
            "AZURE_DEVOPS_PAT manquant : configurez la variable secrète du pipeline Azure DevOps, "
            "puis redéployez le job Databricks."
        )
        
    endpoint = foundry_endpoint.rstrip("/")
    url = f"{endpoint}/openai/deployments/{ai_model_name}/chat/completions?api-version=2024-08-01-preview"
    
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Pipeline en échec : {pipeline_name}\n\nTrace d'erreur Databricks :\n{error_log}\n\nExécute impérativement les 3 actions suivantes dans l'ordre : 1. create_azure_devops_work_item, 2. create_azure_devops_pull_request, 3. send_outlook_email."}
    ]
    
    for turn in range(5):
        payload = {
            "messages": messages,
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
        
        failed_work_item = None
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                choice = data["choices"][0]["message"]
                messages.append(choice)
                
                tool_calls = choice.get("tool_calls", [])
                if not tool_calls:
                    print("\n📝 Synthèse finale de l'Agent IA :")
                    print(choice.get("content", ""))
                    break
                    
                print(f"\n🧠 [Azure AI Foundry - Tour {turn+1}] {len(tool_calls)} Action(s) demandée(s) :")
                for tc in tool_calls:
                    tc_id = tc.get("id")
                    func = tc.get("function", {})
                    f_name = func.get("name")
                    f_args = json.loads(func.get("arguments", "{}"))
                    result = execute_tool(f_name, f_args)
                    if f_name == "create_azure_devops_work_item" and result.get("status") != "created":
                        failed_work_item = result.get("message") or "Création du ticket non confirmée."
                        break
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc_id,
                        "name": f_name,
                        "content": json.dumps(result)
                    })
        except Exception as e:
            print(f"❌ Erreur lors de l'appel à Azure AI Foundry : {e}")
            break
        if failed_work_item is not None:
            raise RuntimeError(f"Remédiation arrêtée après l'échec du ticket Azure DevOps : {failed_work_item}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Lancement de l'analyse d'incident

# COMMAND ----------
run_agent_diagnosis(DEFAULT_ERROR_LOG)
