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
   * Envoi d'une alerte d'incident enrichie dans **Microsoft Teams** via Webhook.
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
│      [Microsoft Teams]          [Azure DevOps Repos PR]                │
│      (Alerte d'incident)        (Bug Work Item + Patch PySpark)        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Structure du Répertoire

```text
DataOps-Log-Troubleshooter/
├── agent/                          # Code de l'Agent IA avec Tool Calling (Local / CI-CD)
│   ├── main.py                     # Point d'entrée d'exécution local du dépanneur
│   ├── core.py                     # Logique d'orchestration avec Azure AI Foundry
│   ├── tools.py                    # Définition des Tools (Teams & Azure DevOps)
│   └── prompts.py                  # Prompt système expert DataOps
├── data/raw_logs/                  # Jeux de logs d'erreurs réels (Delta, Spark OOM, ADLS)
├── notebooks/                      # Code PySpark et Agent déployés dans Databricks
│   ├── 01_ingest_sales_pipeline.py # Script simulant un crash de Schema Drift Delta Lake
│   └── 02_troubleshooter_agent.py  # Agent exécuté automatiquement sur échec
├── terraform/                      # Infrastructure as Code (IaC)
│   ├── provider.tf                 # Déclaration des providers (azurerm, databricks)
│   ├── variables.tf                # Variables (région eastus, nom du projet)
│   ├── main.tf                     # Définition des ressources Azure & Databricks Job
│   ├── outputs.tf                  # URL Databricks, Job ID & URL, Endpoints Foundry
│   └── .terraform.lock.hcl         # Lockfile des versions officielles des providers
├── azure-pipelines.yml             # Pipeline Azure DevOps CI/CD pour validation PySpark
├── .gitignore                      # Protection des états (.tfstate) et documentation
└── README.md                       # Documentation du projet
```

---

## 🚀 Prise en Main & Déploiement

### 1. Déploiement de l'Infrastructure (Terraform)
```powershell
cd terraform
terraform init
terraform apply
```

### 2. Déclenchement du Workflow sur Azure Databricks
1. Ouvrez l'interface Databricks via l'URL générée en output :  
   `https://<databricks_workspace_url>#job/<databricks_job_id>`
2. Cliquez sur le bouton **"Run now"**.
3. **Observation en direct** :
   * La tâche `ingest_sales_pipeline` s'exécute et échoue suite à la violation de schéma Delta (`AnalysisException`).
   * La condition `AT_LEAST_ONE_FAILED` active instantanément la tâche `troubleshoot_on_failure`.
   * Le notebook `02_troubleshooter_agent` s'exécute sur le cluster, interroge **Azure AI Foundry** (`gpt-4.1-mini`), diagnostique la cause racine, et déclenche les Tools de remédiation (Teams et Azure DevOps) !
