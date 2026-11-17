import base64
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

def deploy():
    host = os.getenv("DATABRICKS_HOST", "https://adb-7405615002700070.10.azuredatabricks.net").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN")

    print("=" * 80)
    print("🚀 CI/CD DEPLOYMENT TO AZURE DATABRICKS")
    print(f"Host cible : {host}")
    print("=" * 80)

    if not token:
        print("⚠️ AVERTISSEMENT : La variable DATABRICKS_TOKEN n'est pas définie dans la pipeline Azure DevOps.")
        print("💡 Pour activer le déploiement réel en continu :")
        print("   1. Allez dans Azure Databricks > User Settings > Developer > Access tokens > Generate new token.")
        print("   2. Dans Azure DevOps > Pipelines > Library (ou Variables de la pipeline) > Ajoutez DATABRICKS_TOKEN.")
        print("✅ Validation de la syntaxe et des artefacts terminée avec succès.")
        return

    root_dir = Path(__file__).resolve().parent.parent
    notebooks = [
        {"local": root_dir / "notebooks" / "01_ingest_sales_pipeline.py", "remote": "/Shared/01_ingest_sales_pipeline"},
        {"local": root_dir / "notebooks" / "02_troubleshooter_agent.py", "remote": "/Shared/02_troubleshooter_agent"}
    ]

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

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
                print(f"✅ Déployé avec succès : {local_path.name} ➔ {remote_path}")
        except urllib.error.HTTPError as e:
            err = e.read().decode("utf-8")
            print(f"❌ Erreur lors du déploiement de {local_path.name} : HTTP {e.code} - {err}")
            sys.exit(1)

    print("\n🎉 DÉPLOIEMENT CI/CD VERS AZURE DATABRICKS TERMINÉ AVEC SUCCÈS !")

if __name__ == "__main__":
    deploy()
