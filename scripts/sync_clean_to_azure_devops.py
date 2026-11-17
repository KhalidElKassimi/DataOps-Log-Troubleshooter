import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent

# Charger .env si present
env_file = root_dir / ".env"
if env_file.exists():
    with open(env_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

pat = os.getenv("AZURE_DEVOPS_PAT")
org = os.getenv("AZURE_DEVOPS_ORG", "khalid-dataops")
project = os.getenv("AZURE_DEVOPS_PROJECT", "khalid-dataops")

if not pat:
    print("❌ AZURE_DEVOPS_PAT manquant.")
    sys.exit(1)

azure_remote_url = f"https://{pat}@dev.azure.com/{org}/{project}/_git/{project}"

print("=" * 80)
print("🚀 SYNCHRONISATION DU REPO AZURE DEVOPS (NOTEBOOKS & CI/CD UNIQUEMENT)")
print(f"Organisation : {org}")
print(f"Projet       : {project}")
print("=" * 80)

# Créer un dossier temporaire propre
with tempfile.TemporaryDirectory() as tmp_dir_str:
    tmp_path = Path(tmp_dir_str)
    
    # 1. Copie exclusive des Notebooks
    (tmp_path / "notebooks").mkdir(parents=True, exist_ok=True)
    shutil.copy2(root_dir / "notebooks" / "01_ingest_sales_pipeline.py", tmp_path / "notebooks" / "01_ingest_sales_pipeline.py")
    shutil.copy2(root_dir / "notebooks" / "02_troubleshooter_agent.py", tmp_path / "notebooks" / "02_troubleshooter_agent.py")
    
    # 2. Copie exclusive du script de déploiement Databricks
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    shutil.copy2(root_dir / "scripts" / "deploy_to_databricks.py", tmp_path / "scripts" / "deploy_to_databricks.py")
    
    # 3. Copie du azure-pipelines.yml
    shutil.copy2(root_dir / "azure-pipelines.yml", tmp_path / "azure-pipelines.yml")
    
    # 4. README dédié pour Azure Databricks
    readme_content = """# 🚀 DataOps - Azure Databricks & CI/CD Pipeline

Ce repository Azure DevOps héberge **exclusivement** les composants exécutés par et pour **Azure Databricks** :

## 📁 Structure du Repository
- **`notebooks/01_ingest_sales_pipeline.py`** : Pipeline d'ingestion PySpark écrivant vers Delta Lake (`silver_sales`).
- **`notebooks/02_troubleshooter_agent.py`** : Agent IA autonome (Azure AI Foundry `gpt-4.1-mini`) pour le diagnostic et la remédiation automatique en cas d'erreur.
- **`scripts/deploy_to_databricks.py`** : Script de synchronisation vers `/Shared` et orchestration du Workflow Job Databricks.
- **`azure-pipelines.yml`** : Pipeline CI/CD automatisée (Linting PySpark + Déploiement Databricks).

---
*Géré automatiquement par la CI/CD DataOps.*
"""
    (tmp_path / "README.md").write_text(readme_content, encoding="utf-8")
    
    # 5. .gitignore minimal
    (tmp_path / ".gitignore").write_text("__pycache__/\n*.pyc\n.env\n", encoding="utf-8")
    
    # Initialisation git
    env = os.environ.copy()
    subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "DataOps Automation Agent"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "agent@dataops.local"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-m", "feat(dataops): sync isolated databricks notebooks and ci/cd pipeline"], cwd=tmp_path, check=True)
    
    print("\n📦 Fichiers inclus dans Azure DevOps Repos :")
    for f in tmp_path.rglob("*"):
        if f.is_file() and ".git" not in f.parts:
            print(f"   📄 {f.relative_to(tmp_path)}")
            
    print(f"\n⚡ Push vers Azure DevOps Repos ({azure_remote_url[:35]}...)...")
    res = subprocess.run(["git", "push", azure_remote_url, "main", "--force"], cwd=tmp_path, capture_output=True, text=True)
    
    if res.returncode == 0:
        print("✅ Azure DevOps Repos synchronisé avec succès !")
        print(f"👉 Vérifiez sur : https://dev.azure.com/{org}/{project}/_git/{project}")
    else:
        print(f"❌ Erreur lors du push : {res.stderr}")
        sys.exit(res.returncode)
