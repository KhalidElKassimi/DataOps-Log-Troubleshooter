import os
import sys
from urllib.error import HTTPError
from pathlib import Path

# Add project root to sys.path
root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

from scripts.devops_client import AzureDevOpsClient

ORG = os.getenv("AZURE_DEVOPS_ORG", "khalid-dataops")
PROJECT = os.getenv("AZURE_DEVOPS_PROJECT", "khalid-dataops")
PAT = os.getenv("AZURE_DEVOPS_PAT", "")
PIPELINE_FILE = "pipelines/azure-pipelines.yml"

def main():
    if not PAT:
        print("❌ Erreur : La variable d'environnement AZURE_DEVOPS_PAT n'est pas définie.")
        print("💡 Sur GitHub Actions : Ajoutez AZURE_DEVOPS_PAT dans Settings > Secrets > Actions")
        sys.exit(1)
    print("=" * 80)
    print("🚀 CONFIGURATION D'AZURE DEVOPS POUR LE PROJET DATAOPS")
    print(f"Organisation : {ORG}")
    print(f"Projet       : {PROJECT}")
    print("=" * 80)

    client = AzureDevOpsClient(org=ORG, project=PROJECT, pat=PAT)

    # 1. Vérification du projet
    try:
        project = client.get_project_details()
        print(f"✅ Projet accessible : {project.get('name')} (ID: {project.get('id')})")
    except HTTPError as error:
        if error.code not in (401, 403):
            raise
        sys.exit(
            f"❌ Azure DevOps HTTP {error.code} : accès refusé au projet {ORG}/{PROJECT}. "
            "Vérifiez le secret GitHub Actions AZURE_DEVOPS_PAT et les droits de son compte sur le projet."
        )

    # 2. Vérification des Teams
    teams = client.get_teams()
    if not teams:
        sys.exit("❌ Aucune équipe trouvée dans le projet : synchronisation non exécutée.")
    team = teams[0]
    team_name = team["name"]
    print(f"👥 Équipe par défaut : {team_name} (ID: {team['id']})")

    # 3. Configuration des colonnes du Kanban Board
    print("\n📋 Configuration du Board Agile...")
    target_columns = ["Backlog", "In Dev", "In Progress", "In PR", "In QA", "Done"]
    try:
        boards = client.get_boards(team_name=team_name)
        print(f"   Boards détectés ({len(boards)}) : {[b.get('name') for b in boards]}")

        for board in boards:
            board_id = board.get("id")
            board_name = board.get("name")
            print(f"\n🔍 Analyse du Board : '{board_name}' (ID: {board_id})...")

            try:
                current_cols = client.get_board_columns(team_name=team_name, board_id=board_id)
                print(f"   Colonnes existantes ({len(current_cols)}) : {[c.get('name') for c in current_cols]}")

                # Extraction des stateMappings existants
                incoming_mapping = {}
                inprogress_mapping = {}
                outgoing_mapping = {}
                incoming_id = None
                outgoing_id = None

                for col in current_cols:
                    c_type = col.get("columnType")
                    s_map = col.get("stateMappings", {})
                    if c_type == "incoming":
                        incoming_mapping = s_map
                        incoming_id = col.get("id")
                    elif c_type == "outgoing":
                        outgoing_mapping = s_map
                        outgoing_id = col.get("id")
                    elif c_type == "inProgress" and not inprogress_mapping:
                        inprogress_mapping = s_map

                if not inprogress_mapping:
                    inprogress_mapping = incoming_mapping

                # Création des 6 colonnes demandées
                new_cols = []
                for i, col_name in enumerate(target_columns):
                    if i == 0:
                        col_dict = {
                            "id": incoming_id,
                            "name": col_name,
                            "itemLimit": 0,
                            "columnType": "incoming",
                            "stateMappings": incoming_mapping
                        }
                    elif i == len(target_columns) - 1:
                        col_dict = {
                            "id": outgoing_id,
                            "name": col_name,
                            "itemLimit": 0,
                            "columnType": "outgoing",
                            "stateMappings": outgoing_mapping
                        }
                    else:
                        col_dict = {
                            "name": col_name,
                            "itemLimit": 0,
                            "columnType": "inProgress",
                            "stateMappings": inprogress_mapping
                        }
                    new_cols.append(col_dict)

                print(f"   🚀 Application des 6 colonnes sur le board '{board_name}'...")
                updated = client.set_board_columns(team_name=team_name, columns=new_cols, board_id=board_id)
                print(f"✅ Succès ! Board '{board_name}' mis à jour avec les 6 colonnes :")
                print(f"   👉 {' -> '.join(target_columns)}")
            except Exception as be:
                print(f"   ❌ Erreur sur '{board_name}' : {be}")

    except Exception as e:
        print(f"⚠️ Erreur globale lors de la détection des boards : {e}")

    # 4. Synchronisation des Notebooks et de la CI/CD sur le Git Repository
    print("\n📦 Configuration et synchronisation du Git Repository...")
    repos = client.get_repositories()
    if not repos:
        sys.exit("❌ Aucun dépôt Git trouvé dans le projet : synchronisation non exécutée.")

    repo = repos[0]
    repo_id = repo["id"]
    repo_name = repo["name"]
    print(f"📁 Repository sélectionné : {repo_name} (ID: {repo_id})")

    # Lecture des fichiers à synchroniser sur main
    pyspark_notebook = (root_dir / "notebooks" / "01_ingest_sales_pipeline.py").read_text(encoding="utf-8")
    agent_notebook = (root_dir / "notebooks" / "02_troubleshooter_agent.py").read_text(encoding="utf-8")
    deploy_script = (root_dir / "scripts" / "deploy_to_databricks.py").read_text(encoding="utf-8")
    ci_cd_pipeline = (root_dir / PIPELINE_FILE).read_text(encoding="utf-8")
    pipeline_path = f"/{PIPELINE_FILE}"
    readme = (root_dir / "README.md").read_text(encoding="utf-8")

    files_to_push = {
        "/notebooks/01_ingest_sales_pipeline.py": pyspark_notebook,
        "/notebooks/02_troubleshooter_agent.py": agent_notebook,
        "/scripts/deploy_to_databricks.py": deploy_script,
        pipeline_path: ci_cd_pipeline,
        "/README.md": readme
    }
    files_to_push["/tests/requirements.txt"] = (root_dir / "tests" / "requirements.txt").read_text(encoding="utf-8")
    for test_file in sorted((root_dir / "tests").glob("test_*.py")):
        files_to_push[f"/tests/{test_file.name}"] = test_file.read_text(encoding="utf-8")

    # Vérification si la branche main a déjà un commit
    main_commit = client.get_default_branch_commit(repo_id, "main")
    if main_commit:
        print(f"ℹ️ La branche 'main' existe déjà (Commit: {main_commit[:8]}).")
        print("⚡ Synchronisation des Notebooks (01_ingest et 02_agent) et de la CI/CD sur 'main'...")
        client.push_to_existing_branch(repo_id, "main", files_to_push, "feat(dataops): synchronisation des notebooks et scripts de deploiement CI/CD")
        print("✅ Synchronisation réussie sur la branche 'main' d'Azure DevOps !")
    else:
        print("⚡ Le repository est vide. Initialisation avec le premier commit sur 'main'...")
        res = client.push_initial_commit(repo_id, files_to_push, "feat(dataops): Initialisation du repo avec les pipelines PySpark et CI/CD")
        print("✅ Repository initialisé avec succès avec 'main' !")

    # 5. Enregistrement automatique de la Pipeline CI/CD dans Azure DevOps
    print("\n🚀 Configuration de la Pipeline CI/CD dans Azure DevOps...")
    pipeline_id = client.ensure_yaml_pipeline(repo_id, repo_name, pipeline_path, "CI-CD-Databricks-Pipeline")
    print(f"✅ Pipeline Azure DevOps #{pipeline_id} configuré sur {pipeline_path}")
    print(f"🔗 URL : https://dev.azure.com/{ORG}/{PROJECT}/_build?definitionId={pipeline_id}")

    # 6. Configuration automatique du secret DATABRICKS_TOKEN et lancement de la pipeline
    db_token = os.getenv("DATABRICKS_TOKEN")
    if db_token:
        print("\n🔑 Configuration sécurisée du DATABRICKS_TOKEN dans la Pipeline Azure DevOps...")
        client.set_pipeline_secret_variable(pipeline_id, "DATABRICKS_TOKEN", db_token)
        print("✅ Secret DATABRICKS_TOKEN enregistré avec succès dans la Pipeline !")
        print("🚀 Déclenchement de la Pipeline CI/CD Azure DevOps...")
        build = client.queue_build(pipeline_id)
        print(f"✅ Pipeline lancée avec succès ! Build ID #{build.get('id')}")
        print(f"🔗 Suivi du Run : {build.get('_links', {}).get('web', {}).get('href')}")

    print("\n🎉 INITIALISATION AZURE DEVOPS TERMINÉE !")

if __name__ == "__main__":
    main()
