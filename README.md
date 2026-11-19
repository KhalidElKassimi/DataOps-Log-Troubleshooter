# DataOps Log Troubleshooter 🚀

> **AI-Powered Diagnostics for Azure Databricks PySpark Notebooks & Delta Lake**  
> Diagnostic automatisé et intelligent des pannes de pipelines et Notebooks Spark sous **Azure Databricks** grâce à **Microsoft Azure AI Foundry** et architecture **Serverless Event-Driven**.

---

## 🎯 Problématique & Objectif

Dans les architectures Data modernes sous **Azure Databricks**, les échecs de **Notebooks PySpark** et d'opérations **Delta Lake** génèrent des stacktraces distribuées complexes (exceptions Spark Java, ruptures de schéma Delta, saturation mémoire OOM, conflits d'écritures concurrentes).

Ce projet déploie une infrastructure Cloud complète et un **Agent IA avec Tool Calling** capable de :
1. **Intercepter et analyser** les traces d'erreurs de jobs et Notebooks Databricks.
2. **Identifier la cause racine exacte** (ex: *Schema Drift Delta*, *Data Skew Spark*, *Memory OOM*).
3. **Déclencher automatiquement les actions (Tools)** :
   * Envoi d'une alerte d'incident par **Outlook** via Logic App.
   * Création automatique d'un **Bug Work Item** sur le Board **Azure DevOps**.
   * Création d'une **Pull Request (PR)** sur **Azure DevOps Repos** avec le patch PySpark correctif prêt pour validation humaine (Human-in-the-Loop).

---

## 🏗️ Architecture du Projet

L'infrastructure et l'orchestration sont 100% automatisées via **Terraform** :

```
┌────────────────────────────────────────────────────────────────────────┐
│               FLUX D'ORCHESTRATION 100% AZURE DATABRICKS               │
│                                                                        │
│  Tâche 1 : [01_ingest_sales_pipeline] (Notebook PySpark)               │
│                            │                                           │
│                            ▼ (Échec détecté : AnalysisException Delta) │
│  Condition : [RUN IF DEPENDENCY FAILED]                                │
│                            │ (Déclenchement automatique immédiat)      │
│                            ▼                                           │
│  Tâche 2 : [02_troubleshooter_agent] (Notebook Agent Python)           │
│                            │                                           │
│                            ▼ (Requête d'inférence avec Tool Calling)   │
│             [Microsoft Azure AI Foundry] (gpt-4.1-mini)                │
│                            │                                           │
│             ┌──────────────┴──────────────┐                            │
│             ▼                             ▼                            │
│      [Outlook via Logic App]    [Azure DevOps Repos PR]                │
│      (Alerte par e-mail)        (Bug Work Item + Patch PySpark)        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Structure du Répertoire

```text
DataOps-Log-Troubleshooter/
├── .github/workflows/              # Configuration et synchronisation manuelles vers Azure DevOps
├── notebooks/                      # Code PySpark et Agent déployés dans Databricks
│   ├── 01_ingest_sales_pipeline.py # Script simulant un crash de Schema Drift Delta Lake
│   └── 02_troubleshooter_agent.py  # Agent exécuté automatiquement sur échec
├── pipelines/
│   └── azure-pipelines.yml         # Validation des notebooks et déploiement Databricks
├── scripts/                        # Scripts exécutés par la CI/CD
│   ├── deploy_to_databricks.py     # Déploie les notebooks et configure le workflow
│   ├── setup_azure_devops.py       # Configure Azure DevOps et synchronise les fichiers
│   └── devops_client.py            # Client REST utilisé par le script de configuration
├── terraform/                      # Infrastructure as Code (IaC)
│   ├── provider.tf                 # Déclaration des providers (azurerm, databricks)
│   ├── variables.tf                # Variables (région eastus, nom du projet)
│   ├── main.tf                     # Ressources Azure, espace Databricks et Azure OpenAI
│   ├── outputs.tf                  # URL et chemins Databricks, endpoint Azure OpenAI
│   ├── logic_app_outlook.json       # Modèle ARM pour l'envoi d'alertes Outlook
│   └── .terraform.lock.hcl         # Lockfile des versions officielles des providers
├── .gitignore                      # Protection des états (.tfstate) et documentation
└── README.md                       # Documentation du projet
```

---

## 🚀 Prise en Main & Déploiement

### 1. Déploiement de l'Infrastructure (Terraform)
```powershell
cd terraform
terraform init
terraform validate
terraform plan
```

Le notebook utilise `gpt-4.1-mini` sur le compte Azure OpenAI. Après relecture du plan, utilisez `terraform apply` pour appliquer les changements prévus.

Pour l'envoi depuis Databricks, ouvrez la connexion API `outlook` dans le portail Azure, choisissez **Modifier la connexion** → **Autoriser**, connectez la boîte Outlook.com expéditrice et enregistrez. Le notebook n'annonce un envoi que si la Logic App confirme le succès du connecteur.

Si l'URL Outlook manque, ouvrez l'application logique `logic-dataops-outlook-sender` dans le portail Azure (pas la connexion API `outlook`). Dans **Outils de développement → Concepteur**, ouvrez la première carte **Lorsqu'une requête HTTP est reçue** (`manual` est son nom interne). Copiez son **URL HTTP** complète dans la variable secrète `OUTLOOK_LOGIC_APP_URL` du pipeline Azure DevOps (**Modifier → Variables**). L'URL est générée à l'enregistrement du flux. Relancez le pipeline pour mettre à jour le job Databricks. Pour diagnostiquer un échec après configuration, consultez l'historique des exécutions de la Logic App et le résultat de l'action d'envoi Outlook.

Dans Azure DevOps Pipelines, configurez `DATABRICKS_TOKEN`, `FOUNDRY_API_KEY`, `AZURE_DEVOPS_PAT` et, pour les alertes Outlook, `OUTLOOK_LOGIC_APP_URL` comme variables secrètes. Ne placez pas leurs valeurs dans les notebooks ni dans le dépôt. Le workflow GitHub manuel utilise `scripts/setup_azure_devops.py` pour synchroniser les notebooks et la CI/CD sans réécrire l'historique Azure Repos. Les scripts lisent les variables fournies par la CI/CD, pas un fichier `.env` local.

Le workflow GitHub publie [`pipelines/azure-pipelines.yml`](./pipelines/azure-pipelines.yml), puis configure le pipeline Azure DevOps sur ce chemin. Si le pipeline existe déjà dans le même dépôt, le workflow met à jour son chemin sans le recréer. Une fois la migration vérifiée, vous pouvez retirer l'ancien YAML à la racine d'Azure Repos.

Si le workflow GitHub échoue avec `HTTP 401` ou `403` à la vérification du projet, mettez à jour le secret **GitHub → Settings → Secrets and variables → Actions → `AZURE_DEVOPS_PAT`**. Utilisez un PAT valide pour l'organisation Azure DevOps cible, avec **Project and Team (Read)** pour lire le projet et **Code (Read & write)** pour publier les fichiers. Le compte doit avoir accès au projet et pouvoir contribuer au dépôt. Ce secret est distinct de la variable secrète du pipeline Azure DevOps. Relancez ensuite le workflow GitHub sur `main` et vérifiez le message de synchronisation dans ses logs.

Si le notebook signale `FOUNDRY_API_KEY` manquante, ajoutez une clé valide du compte Azure OpenAI `aifs-dataops-troubleshooter-dev` dans **Azure DevOps → Pipelines → pipeline Databricks → Modifier → Variables** et marquez-la comme secrète. Le workflow GitHub n'enregistre pas cette clé. Relancez ensuite le pipeline Azure DevOps pour mettre à jour le job, puis testez depuis **Databricks → Workflows** : une exécution directe du notebook ne reçoit pas les paramètres du job.

Si la création du ticket renvoie une réponse non JSON, ou un HTTP `401`/`403`, vérifiez `AZURE_DEVOPS_PAT` au même endroit. Le secret GitHub du même nom sert à la synchronisation et n'est pas copié dans Azure DevOps Pipelines. Utilisez un PAT valide pour l'organisation cible, avec **Work Items (Read & write)** et **Code (Read & write)** pour la PR. Relancez le pipeline Azure DevOps après toute modification de la variable.

### 2. Déclenchement du Workflow sur Azure Databricks

Le déploiement crée aussi une copie de l'ingestion sous `/Shared/dataops_versions/ingest_<sha256>` et utilise cette copie pour le job. Le diagnostic récupère le chemin depuis le run en échec, exporte le code, vérifie son empreinte et transmet au modèle le code et la trace filtrés des secrets connus et des paramètres sensibles. Le commit de déploiement Azure Repos (`BUILD_SOURCEVERSION`) accompagne le contexte ; il ne remplace pas la vérification du contenu. Évitez les secrets et données personnelles dans les notebooks et les messages d'exception : ce filtrage n'est pas une garantie d'anonymisation universelle.

Une source non vérifiable, un ancien job non versionné ou une différence entre le code exécuté et `main` bloque la PR, mais autorise un diagnostic signalant le manque de contexte. Les PR sont des brouillons : la compilation Python est contrôlée, pas l'exécution Spark. Testez le correctif sur un calcul et des données isolés avant de publier/fusionner la PR. L'agent ne doit pas décider seul de remplacer un prix, supprimer une transaction ou changer une contrainte métier.

La CI exécute `python -B -m unittest discover -s tests -p "test_*.py"` pour couvrir le contexte réel, les secrets masqués, les versions incohérentes et les correctifs limités à un bloc. Ces tests simulés ne remplacent pas un test fonctionnel Databricks.

Avant le déploiement, configurez la variable Azure DevOps `DATABRICKS_SILVER_SCHEMA` avec un schéma **Unity Catalog** existant réservé à la démonstration (`catalogue.schema`). L'identité du job doit disposer de `USE CATALOG`, `USE SCHEMA` et `CREATE TABLE`, et pouvoir modifier la table si elle existe déjà. Le notebook réinitialise `dataops_demo_silver_sales` à chaque run, sans utiliser DBFS root.

1. Ouvrez l'interface Databricks via l'URL générée en output :  
   `https://<databricks_workspace_url>#job/<databricks_job_id>`
2. Cliquez sur le bouton **"Run now"**.
3. **Observation en direct** :
   * La tâche `ingest_sales_pipeline` s'exécute et échoue suite à la violation de schéma Delta (`AnalysisException`).
   * La condition `AT_LEAST_ONE_FAILED` active instantanément la tâche `troubleshoot_on_failure`.
   * Le notebook `02_troubleshooter_agent` lit l'erreur du run de la tâche `ingest_sales_pipeline` via l'API Jobs avec l'identité d'exécution Databricks. Cette identité doit pouvoir consulter le job. Si l'erreur est indisponible, le diagnostic s'arrête sans créer de ticket, de PR ou d'alerte fondés sur un exemple.
   * Azure OpenAI (`gpt-4.1-mini`) analyse cette erreur et peut déclencher le ticket et l'alerte Outlook. L'outil `read_ingestion_notebook` lui donne accès en lecture au notebook d'ingestion sur `main` dans Azure Repos pour comparer l'erreur au code existant. La PR est facultative : elle applique uniquement un bloc exact et unique du notebook lu, sans remplacer le reste. Si `main` a changé depuis la lecture, la PR est refusée jusqu'à une nouvelle lecture et réévaluation. Testez depuis le workflow redéployé, car l'identifiant de la tâche en échec est injecté par Databricks.
