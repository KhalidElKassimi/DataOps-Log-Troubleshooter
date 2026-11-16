# DataOps Log Troubleshooter 🚀

> **AI-Powered Diagnostics for Azure Data Factory & Microsoft Fabric Pipelines**  
> Diagnostic automatisé et intelligent des pannes de pipelines de données grâce à **Microsoft Azure AI Foundry**.

---

## 🎯 Problématique & Objectif

Dans les environnements modernes de données (Modern Data Platform sous Azure / Microsoft Fabric), les échecs de pipelines d'ingestion et de transformation génèrent des logs d'erreurs denses, complexes et cryptiques (exceptions Java/Spark, ruptures de schéma, secrets Key Vault expirés, contraintes d'intégrité SQL).

Ce projet déploie une infrastructure Cloud complète et un agent d'IA capable de :
1. **Intercepter et parser** les logs d'échec d'activités (Copy, Notebook Spark, Stored Procedure).
2. **Identifier la cause racine exacte** (ex: *Schema Drift*, *Data Skew*, *Authentication 404*).
3. **Fournir un plan d'action immédiat** et le **snippet de code correctif** (SQL, PySpark, JSON de mapping ADF).

---

## 🏗️ Architecture du Projet

L'infrastructure est 100% automatisée via **Terraform** :

```
┌────────────────────────────────────────────────────────────────────────┐
│               Azure Resource Group (East US - Low Cost)                │
│                                                                        │
│  1. [Azure Data Lake Storage Gen2] ──> Conteneur "bronze" pour l'ingest│
│                 ▲                                                      │
│                 │ (Authentification sans secret via Managed Identity)  │
│                 ▼                                                      │
│  2. [Azure Data Factory (ADF)]    ──> Exécution des pipelines data     │
│                                                                        │
│  3. [Microsoft Azure AI Foundry]  ──> Compte unifié Azure OpenAI       │
│        └── Modèle : gpt-4.1-mini  ──> Inférence GlobalStandard (10k TPM│
└────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Structure du Répertoire

```text
DataOps-Log-Troubleshooter/
├── terraform/                      # Infrastructure as Code (IaC)
│   ├── provider.tf                 # Déclaration du provider azurerm
│   ├── variables.tf                # Variables (région eastus, noms de projet)
│   ├── main.tf                     # Définition des 8 ressources Azure
│   ├── outputs.tf                  # Endpoints et métadonnées générés
│   └── .terraform.lock.hcl         # Lockfile des versions officielles des providers
├── .gitignore                      # Protection des états (.tfstate) et des binaires
└── README.md                       # Documentation du projet
```

---

## 🚀 Prise en Main & Déploiement

### Prérequis
* Un compte Microsoft Azure avec une souscription active.
* [Azure CLI](https://learn.microsoft.com/fr-fr/cli/azure/install-azure-cli) installé.
* [Terraform](https://developer.hashicorp.com/terraform/install) (v1.5+).

### 1. Connexion au compte Azure
```powershell
az login
az account set --subscription "<VOTRE-SUBSCRIPTION-ID>"
```

### 2. Déploiement de l'Infrastructure avec Terraform
```powershell
cd terraform

# Initialisation des providers
terraform init

# Simulation du plan de déploiement (Lecture seule)
terraform plan

# Déploiement effectif sur Azure
terraform apply
```

### 3. Nettoyage des ressources (Fin de session)
Pour détruire l'ensemble des ressources de test et éviter tout coût résiduel :
```powershell
terraform destroy
```

---

## 🔐 Bonnes Pratiques Appliquées
* **Zéro Secret en clair :** Connexion entre ADF et le Storage Account sécurisée par **Managed Identity** et rôle RBAC `Storage Blob Data Contributor`.
* **Optimisation des coûts :** Déploiement dans la région `eastus` avec un dimensionnement raisonné à 10k TPM sur le modèle `gpt-4.1-mini` sous le palier `GlobalStandard`.
* **Reproductibilité totale :** Toute l'infrastructure est versionnée dans Git via Terraform.
