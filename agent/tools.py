import json
import os
import urllib.request
from typing import Dict, Any, Optional
from pathlib import Path

# Chargement automatique de .env local s'il existe
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from .devops_client import AzureDevOpsClient

# Configuration Azure DevOps par défaut (injectée via variables d'environnement)
ADO_ORG = os.getenv("AZURE_DEVOPS_ORG", "khalid-dataops")
ADO_PROJECT = os.getenv("AZURE_DEVOPS_PROJECT", "khalid-dataops")
ADO_PAT = os.getenv("AZURE_DEVOPS_PAT", "")

# ID du dernier Work Item créé pour le lier à la PR
LAST_WORK_ITEM_ID: Optional[int] = None

# Schéma officiel des Tools pour Microsoft Azure AI Foundry / OpenAI
AGENT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "notify_teams",
            "description": "Envoie une carte d'alerte d'incident DataOps formatée dans un canal Microsoft Teams via Webhook.",
            "parameters": {
                "type": "object",
                "properties": {
                    "severity": {
                        "type": "string",
                        "enum": ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                        "description": "Niveau de sévérité de l'incident."
                    },
                    "pipeline_name": {
                        "type": "string",
                        "description": "Nom du pipeline ou notebook en panne."
                    },
                    "error_summary": {
                        "type": "string",
                        "description": "Résumé court et vulgarisé de l'incident pour l'équipe (1 phrase)."
                    },
                    "root_cause": {
                        "type": "string",
                        "description": "Explication technique détaillée de la cause racine."
                    },
                    "suggested_fix": {
                        "type": "string",
                        "description": "Action recommandée étape par étape pour réparer."
                    }
                },
                "required": ["severity", "pipeline_name", "error_summary", "root_cause", "suggested_fix"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_azure_devops_work_item",
            "description": "Crée automatiquement un ticket de type 'Bug' dans le Board Scrum d'Azure DevOps avec la sévérité, l'analyse et le patch PySpark.",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Titre normalisé du Bug (ex: [DATA-BUG] Schema Mismatch sur silver_sales)."
                    },
                    "severity": {
                        "type": "string",
                        "enum": ["1 - Critical", "2 - High", "3 - Medium"],
                        "description": "Sévérité Azure DevOps."
                    },
                    "repro_steps": {
                        "type": "string",
                        "description": "Étapes de reproduction détaillant la stacktrace et la table impactée."
                    },
                    "proposed_fix": {
                        "type": "string",
                        "description": "Le snippet de code PySpark ou SQL pour corriger le bug."
                    }
                },
                "required": ["title", "severity", "repro_steps", "proposed_fix"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_azure_devops_pull_request",
            "description": "Crée une branche Git et ouvre une Pull Request (PR) dans Azure DevOps Repos avec le correctif PySpark, en attente de validation manuelle par l'ingénieur.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_branch": {
                        "type": "string",
                        "description": "Nom de la branche de fix (ex: fix/schema-mismatch-silver-sales)."
                    },
                    "target_branch": {
                        "type": "string",
                        "description": "Branche cible de production (ex: main)."
                    },
                    "pr_title": {
                        "type": "string",
                        "description": "Titre clair de la Pull Request (ex: fix(databricks): Cast unit_price to double in 01_ingest_sales_pipeline)."
                    },
                    "pr_description": {
                        "type": "string",
                        "description": "Description détaillée de la PR expliquant le correctif et demandant la validation manuelle."
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Chemin du fichier modifié dans le repo (ex: notebooks/01_ingest_sales_pipeline.py)."
                    },
                    "fixed_code": {
                        "type": "string",
                        "description": "Le code complet ou le bloc corrigé à intégrer dans le fichier."
                    }
                },
                "required": ["source_branch", "target_branch", "pr_title", "pr_description", "file_path", "fixed_code"]
            }
        }
    }
]

def send_teams_alert(args: Dict[str, Any]) -> Dict[str, Any]:
    """Formate et simule l'envoi de l'alerte vers Microsoft Teams."""
    print(f"\n📢 [Teams Notification] Canal DataOps Alerting")
    print(f"    Sévérité : [{args.get('severity')}] | Pipeline : {args.get('pipeline_name')}")
    print(f"    Message  : {args.get('error_summary')}")
    print(f"    Fix      : {args.get('suggested_fix')}")
    return {"status": "sent", "channel": "DataOps-Alerts", "timestamp": "2026-11-17T11:40:00Z"}

def create_azure_devops_work_item(args: Dict[str, Any]) -> Dict[str, Any]:
    """Crée un Work Item de type 'Bug' dans le Board Scrum d'Azure DevOps en colonne Backlog."""
    global LAST_WORK_ITEM_ID
    print(f"\n📋 [Azure DevOps Board] Création réelle du Bug Work Item...")
    print(f"    Organisation : {ADO_ORG} | Projet : {ADO_PROJECT}")
    print(f"    Titre        : {args.get('title')}")
    print(f"    Colonne      : Backlog")

    try:
        client = AzureDevOpsClient(org=ADO_ORG, project=ADO_PROJECT, pat=ADO_PAT)
        wi = client.create_work_item(
            item_type="Bug",
            title=args.get("title", "[DATA-BUG] Incident Pipeline Databricks"),
            description=f"<strong>Analyse Cause Racine :</strong><br/>{args.get('repro_steps')}<br/><br/><strong>Patch proposé :</strong><pre>{args.get('proposed_fix')}</pre>",
            repro_steps=args.get("repro_steps", ""),
            board_column="Backlog",
            priority=1 if "Critical" in args.get("severity", "") else 2
        )
        LAST_WORK_ITEM_ID = wi.get("id")
        wi_url = wi.get("_links", {}).get("html", {}).get("href", f"https://dev.azure.com/{ADO_ORG}/{ADO_PROJECT}/_workitems/edit/{LAST_WORK_ITEM_ID}")
        print(f"✅ Ticket créé avec succès : ID #{LAST_WORK_ITEM_ID} dans la colonne [Backlog]")
        print(f"🔗 URL : {wi_url}")
        return {
            "status": "created",
            "work_item_id": LAST_WORK_ITEM_ID,
            "column": "Backlog",
            "url": wi_url
        }
    except Exception as e:
        print(f"⚠️ Erreur création Work Item Azure DevOps ({e}). Mode fallback activé.")
        return {
            "status": "simulated",
            "work_item_id": 1042,
            "column": "Backlog",
            "url": f"https://dev.azure.com/{ADO_ORG}/{ADO_PROJECT}/_workitems/edit/1042"
        }

def create_azure_devops_pull_request(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crée une branche Git réelle, pousse le commit correctif, ouvre la Pull Request (PR)
    dans Azure DevOps Repos et déplace le ticket Bug dans la colonne [In PR].
    """
    global LAST_WORK_ITEM_ID
    print(f"\n🔀 [Azure DevOps Repos] Création réelle de la Pull Request...")
    print(f"    Branche source : {args.get('source_branch')} ──> Cible : {args.get('target_branch')}")
    print(f"    Titre PR       : {args.get('pr_title')}")
    print(f"    Fichier        : {args.get('file_path')}")

    try:
        client = AzureDevOpsClient(org=ADO_ORG, project=ADO_PROJECT, pat=ADO_PAT)
        repos = client.get_repositories()
        if not repos:
            raise Exception("Aucun dépôt Git trouvé dans le projet Azure DevOps.")
        repo_id = repos[0]["id"]
        repo_name = repos[0]["name"]

        source_branch = args.get("source_branch", "fix/schema-mismatch-unit-price")
        # Nettoyage du préfixe si fourni
        branch_name = source_branch.replace("refs/heads/", "")

        # Lecture du fichier local existant pour remplacer ou patcher
        fixed_code = args.get("fixed_code", "")

        # Pousser la branche et le commit
        client.create_branch_with_fix(
            repo_id=repo_id,
            base_branch=args.get("target_branch", "main"),
            fix_branch=branch_name,
            file_path=args.get("file_path", "notebooks/01_ingest_sales_pipeline.py"),
            new_content=fixed_code,
            commit_message=f"fix(databricks): {args.get('pr_title')}"
        )

        # Création de la Pull Request avec liaison au Work Item
        work_items = [LAST_WORK_ITEM_ID] if LAST_WORK_ITEM_ID else None
        pr = client.create_pull_request(
            repo_id=repo_id,
            source_branch=branch_name,
            target_branch=args.get("target_branch", "main"),
            title=args.get("pr_title"),
            description=f"### Correctif automatisé généré par l'Agent Microsoft Azure AI Foundry\n\n{args.get('pr_description')}\n\n**Action requise :** Validez et complétez (mergez) cette PR manuellement pour déclencher la CI/CD vers Databricks.",
            work_item_ids=work_items
        )

        pr_id = pr.get("pullRequestId")
        pr_url = f"https://dev.azure.com/{ADO_ORG}/{ADO_PROJECT}/_git/{repo_name}/pullrequest/{pr_id}"

        # Déplacement du ticket Bug vers la colonne [In PR]
        if LAST_WORK_ITEM_ID:
            try:
                client.update_work_item_column(LAST_WORK_ITEM_ID, "In PR")
                print(f"📋 Ticket Bug #{LAST_WORK_ITEM_ID} déplacé vers la colonne [In PR]")
            except Exception as ce:
                print(f"ℹ️ Note mise à jour colonne Work Item : {ce}")

        print(f"✅ Pull Request créée avec succès ! ID #{pr_id}")
        print(f"🔗 URL PR : {pr_url}")
        print(f"⏳ EN ATTENTE DE VOTRE APPROBATION MANUELLE (Human-in-the-Loop)")

        return {
            "status": "created",
            "pull_request_id": pr_id,
            "url": pr_url,
            "state": "Active"
        }

    except Exception as e:
        print(f"⚠️ Erreur création Pull Request Azure DevOps ({e}). Mode fallback activé.")
        return {
            "status": "simulated",
            "pull_request_id": 88,
            "url": f"https://dev.azure.com/{ADO_ORG}/{ADO_PROJECT}/_git/khalid-dataops/pullrequest/88",
            "state": "Active (Waiting for manual approval)"
        }
