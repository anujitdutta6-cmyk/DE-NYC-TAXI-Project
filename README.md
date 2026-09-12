# End-to-End NYC Taxi Data Engineering Pipeline

[![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-Orchestration-0078D4)](https://learn.microsoft.com/azure/data-factory/)
[![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-PySpark%20%7C%20Delta%20Lake-EF3B2D?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-Data%20Lake-0078D4)](https://azure.microsoft.com/products/storage/data-lake-storage)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-0F5BFF)](https://delta.io/)

## Project Overview

This project builds an end-to-end **NYC Taxi data engineering pipeline on Azure**.

The architecture separates ingestion, cloud storage, distributed transformation and governed analytical tables:

**NYC Taxi API / Source → Azure Data Factory → ADLS Gen2 Bronze → Azure Databricks / PySpark → Silver → Gold Delta Tables → Unity Catalog**

The project also uses **Databricks Asset Bundles (DAB)** to organize the Databricks workload into a deployable, version-controlled structure.

---

# Architecture

```text
                         NYC Taxi API / Source
                                  │
                                  │ 1. Ingestion
                                  ▼
                     ┌─────────────────────────┐
                     │    Azure Data Factory   │
                     │                         │
                     │ • REST/API source      │
                     │ • Parameters           │
                     │ • ForEach              │
                     │ • Conditional logic    │
                     │ • Copy Activity        │
                     └────────────┬────────────┘
                                  │
                                  │ 2. Raw landing
                                  ▼
                     ┌─────────────────────────┐
                     │       ADLS Gen2         │
                     │                         │
                     │       BRONZE            │
                     │   Raw source data       │
                     └────────────┬────────────┘
                                  │
                                  │ 3. Secure Databricks access
                                  ▼
             ┌──────────────────────────────────────────┐
             │             Azure Databricks              │
             │                                          │
             │  Workspace                              │
             │     │                                    │
             │     ├── Account / Workspace permissions  │
             │     ├── Unity Catalog / Metastore        │
             │     ├── Storage Credential               │
             │     └── External Location / ABFS path    │
             │                                          │
             │              DAB: nyc_taxi_dab            │
             │                     │                    │
             │             ┌───────┴────────┐            │
             │             │                │            │
             │           SILVER           GOLD           │
             │             │                │            │
             │       Read Bronze      Read Silver       │
             │       Transform        Transform         │
             │       Validate         Curate            │
             │             │                │            │
             │             └───────┬────────┘            │
             │                     ▼                    │
             │              Delta Tables                │
             └─────────────────────┬────────────────────┘
                                   │
                                   ▼
                          Unity Catalog
                         Catalog → Schema
                              → Tables
```

---

# Architecture Flow — What / Why / When / How

## 1. API → ADLS Bronze using Azure Data Factory

### What

The NYC Taxi source data is accessed through the source/API and ingested using **Azure Data Factory (ADF)**.

ADF copies the source data into **ADLS Gen2 Bronze**, where the raw/source-aligned data is retained.

### Why

ADF is used as the ingestion and orchestration layer, while ADLS is used as the storage layer.

This keeps responsibilities separate:

```text
ADF  = controls data movement
ADLS = stores data
Databricks = processes data
```

### When

This pattern is useful when data arrives from APIs, files or external systems and needs managed ingestion into a cloud data lake.

### How

The ADF pipeline uses parameters and a `ForEach` loop to process the required month values. The source naming/path logic changes for months greater than 9, so the pipeline contains conditional routing for the two source patterns.

```text
Month list
    │
    ▼
ForEach(month)
    │
    ├── month <= 9 ──► source pattern A
    │
    └── month > 9  ──► source pattern B
                         │
                         ▼
                    Copy Activity
                         │
                         ▼
                    ADLS Bronze
```

### Logic

Instead of creating separate pipelines for every month, one reusable pipeline receives the month as a parameter and dynamically builds the required source/target path.

---

# 2. Azure Storage → Databricks Access Connector

## What

The **Azure Databricks Access Connector for Azure Storage** provides an Azure managed identity that can be used to give Databricks controlled access to Azure Storage.

### Why

Databricks needs permission to read Bronze data and write/read Silver and Gold data in ADLS.

The access connector provides an identity-based way to connect Databricks with Azure Storage instead of putting storage credentials directly inside notebooks.

### When

Use this pattern when Databricks needs secure access to ADLS Gen2 in an Azure environment and the organization wants identity/RBAC-based access rather than embedded account keys.

### How

The high-level flow is:

```text
Azure Databricks
      │
      ▼
Access Connector
      │
      ▼
Managed Identity
      │
      ▼
Azure RBAC / Storage permissions
      │
      ▼
ADLS Gen2
```

The exact permissions depend on whether the identity needs read, write or broader storage access.

---

# 3. Databricks Workspace Creation and Account-Level Configuration

## What

After creating the Azure Databricks workspace, account-level configuration is used to manage access, workspace permissions and Unity Catalog resources.

### Why

A Databricks workspace provides the development/processing environment, while account-level governance controls identities, permissions and Unity Catalog resources.

### When

This configuration is required when the workspace will use Unity Catalog for centralized governance of catalogs, schemas, tables and storage access.

### How

The project setup follows this logical sequence:

```text
Azure Databricks Workspace
          │
          ▼
Databricks Account
          │
          ├── Users / Groups / Permissions
          │
          └── Unity Catalog
                  │
                  ▼
               Metastore
```

The workspace is associated with the appropriate Unity Catalog metastore, and required users/groups receive the permissions needed to work with catalogs, schemas and data.

---

# 4. Unity Catalog — Metastore and ADLS Storage Connection

## What

**Unity Catalog** provides the governance layer for Databricks data objects. The **metastore** is the top-level container that holds metadata for catalogs and their schemas/tables.

A storage credential and external location can be configured to provide governed access to ADLS paths.

### Why

The goal is to avoid treating every ADLS path as an unmanaged direct connection from individual notebooks. Unity Catalog provides a centralized place to manage data access and permissions.

### When

Use Unity Catalog when multiple Databricks workloads, users or environments need governed access to shared cloud data.

### How

Conceptually:

```text
Unity Catalog Metastore
        │
        ▼
Storage Credential
        │
        ▼
Access Connector / Managed Identity
        │
        ▼
External Location
        │
        ▼
ADLS Gen2 path
```

A typical ADLS Gen2 URI has this structure:

```text
abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
```

For example:

```text
abfss://databricksmetastore@<storage-account>.dfs.core.windows.net/
```

`databricksmetastore` in this example is the **container name**. It is not the Unity Catalog metastore itself.

### Important separation

```text
Unity Catalog Metastore
        ≠
ADLS Container
```

The Unity Catalog metastore manages metadata/governance, while ADLS stores the physical data.

---

# 5. Re-open / Refresh the Databricks Workspace

## What

After account-level Unity Catalog configuration and workspace assignment, the Databricks workspace may need to be refreshed or reopened so the updated catalog configuration and permissions are available in the workspace UI.

### Why

The workspace needs to recognize the current account-level configuration and permissions.

### When

Do this after completing workspace/metastore assignment or permission changes when the updated catalog resources are not immediately visible.

### How

```text
Configure account / Unity Catalog
          │
          ▼
Assign permissions / metastore
          │
          ▼
Refresh or reopen workspace
          │
          ▼
Check Catalog Explorer / data access
```

This is a setup/administration step rather than a transformation step.

---

# 6. Bronze Layer

## What

Bronze contains the **raw/source-aligned data** landed from the source through ADF.

### Why

Bronze preserves the source data before business transformations. It acts as a stable starting point for downstream processing and reprocessing.

### When

Use Bronze when raw source data needs to be retained separately from cleaned and business-ready data.

### How

```text
NYC Taxi API
     │
     ▼
ADF
     │
     ▼
ADLS Gen2 / Bronze
```

The main principle is:

**Ingestion first; transformation later.**

This makes it easier to trace downstream results back to the original source data.

---

# 7. Silver Layer — Read Bronze and Transform

## What

Silver is the **refined/processed layer** created from Bronze data using Azure Databricks and PySpark.

### Why

Raw data is usually not ready for reliable downstream use. Silver applies technical cleansing, standardization and transformation logic.

### When

Use Silver when raw data needs to become structurally consistent and reusable for multiple downstream workloads.

### How

The Databricks Silver process follows this logic:

```text
ADLS Bronze
     │
     ▼
Read source data
     │
     ▼
Schema / datatype handling
     │
     ▼
Clean / validate
     │
     ▼
Transform columns
     │
     ▼
Apply required business/technical rules
     │
     ▼
Write Silver
```

Typical transformation operations can include filtering, column standardization, datatype conversion, null handling, joins, derived columns and aggregations where required by the dataset.

### Logic

Silver should improve the quality and usability of the data without mixing all final business-consumption logic into the raw layer.

---

# 8. Gold Layer — Silver → Catalog → Delta Tables

## What

Gold is the **curated analytical layer**. It is built from Silver data and organized as governed tables through Unity Catalog.

### Why

Silver is refined technical data. Gold is designed for stable analytical consumption and business-oriented structures.

### When

Use Gold when downstream consumers need curated tables, metrics, aggregations or analytical datasets rather than raw/refined source structures.

### How

```text
Silver data
    │
    ▼
Read Silver
    │
    ▼
Apply Gold transformations
    │
    ▼
Unity Catalog
    │
    ├── Catalog
    │      │
    │      └── Schema
    │              │
    │              └── Delta Tables
    │
    ▼
Gold analytical data
```

The project uses the Databricks catalog/schema configuration through the DAB pipeline resource, while the Gold processing code is maintained under:

```text
nyc_taxi_dab/src/gold/
```

---

# 9. Catalog → Schema → Delta Table

## What

Unity Catalog organizes governed data objects using a hierarchy such as:

```text
Metastore
   │
   └── Catalog
          │
          └── Schema
                 │
                 └── Table
```

The project DAB configuration exposes `catalog` and `schema` as deployment variables.

### Why

Separating catalog, schema and table levels makes data organization and permissions easier to manage than treating every dataset as an isolated file path.

### When

Use this structure when Databricks data needs centralized governance, discoverability and controlled access.

### How

The DAB pipeline resource defines the target catalog and schema through variables, allowing the same project structure to be deployed with different environment-specific values.

```yaml
catalog: ${var.catalog}
schema: ${var.schema}
```

The physical data can reside in ADLS while Unity Catalog manages the metadata and governance layer for the tables.

---

# 10. Delta Lake — CRUD, Versioning and Time Travel

## What

The Gold layer uses **Delta Lake tables** for lakehouse table management.

### Why

Parquet is a file format. Delta Lake adds table-management capabilities such as transaction logging, version history and reliable data modifications on top of data files.

### When

Use Delta when a lakehouse needs reliable table operations, updates/deletes, historical versions and reproducible data states.

### How

The logical flow is:

```text
Delta Table
    │
    ├── INSERT
    ├── UPDATE
    ├── DELETE
    ├── MERGE
    │
    └── Transaction Log
             │
             ├── Version 0
             ├── Version 1
             ├── Version 2
             └── ...
```

Because the transaction log records committed table changes, Delta can expose different table versions for supported operations such as Time Travel.

### Time Travel logic

```text
Current Delta Table
        │
        ▼
Transaction Log
        │
        ├── Version 0 → original state
        ├── Version 1 → after change
        ├── Version 2 → after another change
        └── Version N → current state
```

This allows historical table states to be queried when the required Delta history is still available.

---

# 11. Databricks Asset Bundle — `nyc_taxi_dab`

## What

The Databricks workload is organized as a **Databricks Asset Bundle (DAB)** named `nyc_taxi_dab`.

### Why

The bundle keeps Databricks configuration, resources, source code and tests together in a version-controlled project structure.

### When

Use DAB when Databricks workloads need repeatable deployment between environments and should be managed as project code rather than manually configured resources only through the UI.

### How

```text
nyc_taxi_dab/
│
├── databricks.yml
├── resources/
│   ├── nyc_taxi_dab_etl.pipeline.yml
│   └── sample_job.job.yml
│
├── src/
│   ├── silver/
│   │   └── silver.ipynb
│   └── gold/
│       └── gold.ipynb
│
├── tests/
├── pyproject.toml
└── fixtures/
```

The DAB configuration defines environment targets such as `dev` and `prod`, while catalog/schema values are supplied as variables.

The project pipeline resource is configured as a **serverless Databricks pipeline**.

---

# 12. Development Environment

## What

The development environment is used to build and validate the pipeline before introducing production deployment practices.

### Why

Development should be isolated from production data objects and deployment configuration so experimentation does not directly affect production workloads.

### When

Use the `dev` target during development, testing and iterative transformation work.

### How

The current DAB project contains a development target with `mode: development` and separate catalog/schema variables.

```text
Developer
   │
   ▼
Git repository
   │
   ▼
nyc_taxi_dab / dev
   │
   ▼
Databricks
   │
   ▼
Silver / Gold development objects
```

Development is where transformation logic, schema changes and pipeline behavior are validated before a production deployment.

---

# 13. Basic Production Example

The production architecture can follow the same logical design while adding stronger operational controls.

```text
                         PRODUCTION

Source / API
     │
     ▼
Azure Data Factory
     │
     ▼
ADLS Gen2 Bronze
     │
     ▼
Databricks + Unity Catalog
     │
     ├── Silver
     │     │
     │     └── validation / transformation
     │     │
     │     ▼
     └── Gold Delta Tables
              │
              ▼
       Governed Catalog / Schema
```

### Production additions

```text
CI/CD
  │
  ├── Source control
  ├── Automated validation/tests
  ├── DAB deployment
  └── Environment-specific configuration

Security
  │
  ├── Managed Identity / Service Principal where appropriate
  ├── Azure RBAC
  ├── Unity Catalog permissions
  └── Secret management such as Azure Key Vault

Operations
  │
  ├── Scheduling / triggers
  ├── Monitoring
  ├── Logging
  ├── Alerts
  ├── Data quality checks
  └── Failure/retry handling
```

The current repository demonstrates the core development architecture. These production additions are the natural next layer for a larger enterprise implementation.

---

# End-to-End Logic in One Flow

```text
1. NYC Taxi API / Source
          │
          ▼
2. Azure Data Factory
   - parameterize month
   - ForEach
   - conditional source path
          │
          ▼
3. ADLS Gen2 Bronze
   - raw/source-aligned data
          │
          ▼
4. Azure Databricks access
   - Access Connector
   - Managed Identity
   - Storage permissions
          │
          ▼
5. Unity Catalog setup
   - Metastore
   - Storage Credential
   - External Location
   - Catalog
   - Schema
          │
          ▼
6. Databricks / PySpark Silver
   - read Bronze
   - transform
   - validate
   - write Silver
          │
          ▼
7. Gold processing
   - read Silver
   - curate / aggregate
   - create Gold data
          │
          ▼
8. Unity Catalog
   - Catalog
   - Schema
   - Delta Tables
          │
          ▼
9. Delta Lake operations
   - CRUD
   - transaction log
   - versioning
   - Time Travel
          │
          ▼
10. DAB deployment
    - nyc_taxi_dab
    - dev / prod targets
```

---

# Repository Structure

```text
DE-NYC-TAXI-Project/
│
├── dataset/
│   ├── ds_nyctaxi_api_src.json
│   ├── ds_nyctaxi_datalake.json
│   └── ds_nyc_taxi_src_greaterthan_9.json
│
├── linkedService/
│   ├── ls_nyc_taxi_api.json
│   └── ls_nyc_taxi_datalakestorage.json
│
├── pipeline/
│   └── nycWebToDL.json
│
├── factory/
│   └── df-nyc-taxi-anuj.json
│
├── nyc_taxi_dab/
│   ├── databricks.yml
│   ├── resources/
│   │   ├── nyc_taxi_dab_etl.pipeline.yml
│   │   └── sample_job.job.yml
│   ├── src/
│   │   ├── silver/
│   │   │   └── silver.ipynb
│   │   └── gold/
│   │       └── gold.ipynb
│   ├── tests/
│   ├── fixtures/
│   └── pyproject.toml
│
├── taxi_zone_lookup.csv
├── publish_config.json
└── README.md
```

---

# Key Design Principles

| Area | Logic |
|---|---|
| Ingestion | ADF handles source-to-lake data movement |
| Storage | ADLS Gen2 stores the lake data |
| Bronze | Preserve raw/source-aligned data |
| Silver | Clean, validate and transform Bronze data |
| Gold | Create curated analytical datasets |
| Security | Databricks accesses ADLS through controlled identity/permissions |
| Governance | Unity Catalog manages catalogs, schemas, tables and access |
| Table format | Delta Lake provides reliable lakehouse table operations |
| Deployment | DAB organizes Databricks resources and environment configuration |
| Development | Dev target isolates development work |
| Production | CI/CD, security, monitoring and quality controls can be added around the same core architecture |

---

# Project Outcome

The project demonstrates a complete Azure lakehouse flow:

**API ingestion → ADF → ADLS Bronze → Databricks/PySpark → Silver → Gold → Unity Catalog → Delta Tables**

The architecture keeps **orchestration, storage, processing, governance and deployment** as separate concerns while allowing them to work together as one data engineering platform.

---

## Technologies

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark / Apache Spark
- Unity Catalog
- Delta Lake
- Databricks Asset Bundles
- Parquet
- Python / PyTest
- Git / GitHub
