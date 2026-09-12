# End-to-End NYC Taxi Data Engineering Pipeline

[![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-ETL%2FOrchestration-0078D4)](https://learn.microsoft.com/azure/data-factory/)
[![Databricks](https://img.shields.io/badge/Azure%20Databricks-PySpark%20%7C%20Delta%20Lake-EF3B2D?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-Data%20Lake-0078D4)](https://azure.microsoft.com/products/storage/data-lake-storage)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-0F5BFF)](https://delta.io/)

## Project Overview

This project implements an **end-to-end Azure data engineering pipeline for NYC Taxi trip data**, demonstrating how source data is ingested, stored, transformed and curated using modern cloud data engineering practices.

The solution combines **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark and Delta Lake** and follows a **Bronze → Silver → Gold medallion architecture**.

The repository contains version-controlled ADF artifacts and a Databricks project organized with **Databricks Asset Bundles (DAB)**, pipeline resources, Silver/Gold processing code and tests.

> **Interview focus:** This project is designed to demonstrate not only tool usage, but also the reasoning behind architecture choices: **What is used? Why is it used? When is it appropriate? How is it implemented? What trade-off does it solve?**

---

## Architecture

```text
                    NYC Taxi Data Source
                             │
                             ▼
                  ┌──────────────────────┐
                  │  Azure Data Factory  │
                  │                      │
                  │ • Parameters        │
                  │ • ForEach            │
                  │ • Conditional logic  │
                  │ • Copy Activity      │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │      ADLS Gen2       │
                  │                      │
                  │  Bronze / Raw        │
                  │       ↓              │
                  │  Silver / Refined    │
                  │       ↓              │
                  │  Gold / Curated      │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │ Azure Databricks     │
                  │                      │
                  │ • PySpark            │
                  │ • Spark              │
                  │ • Delta Lake         │
                  │ • Delta Tables       │
                  │ • DAB                │
                  └──────────────────────┘
```

### Architecture Decision — What / Why / When / How

| Question | Engineering decision |
|---|---|
| **What?** | A cloud lakehouse pipeline using ADF + ADLS Gen2 + Databricks + PySpark + Delta Lake. |
| **Why?** | Separates orchestration, storage and distributed processing so each layer can scale and evolve independently. |
| **When?** | Suitable when data arrives in files/APIs and needs repeatable ingestion, transformation and analytical curation. |
| **How?** | ADF orchestrates ingestion → ADLS stores the data → Databricks transforms it → Delta manages reliable analytical tables. |
| **Why this matters in interviews?** | It shows architectural thinking instead of simply listing Azure services. Interviewers can evaluate design decisions, trade-offs and implementation knowledge. |

---

## Technology Stack

| Layer / Capability | Technology | What it does | Why it is used |
|---|---|---|---|
| Source | NYC Taxi Web/API data | Provides source data | Represents an external data source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page|
| Orchestration | **Azure Data Factory** | Controls ingestion workflow | Managed orchestration, parameterization and scheduling |
| Storage | **ADLS Gen2** | Stores lake data | Scalable and cost-effective cloud object storage |
| Processing | **Azure Databricks** | Executes distributed processing | Suitable for large-scale transformation |
| Transformation | **PySpark / Python** | Cleans and transforms data | Distributed processing with flexible data logic |
| File format | **Parquet** | Stores columnar data | Efficient storage and analytical reads |
| Table format | **Delta Lake** | Manages lakehouse tables | Transactions, versioning and reliable updates |
| Architecture | **Medallion** | Separates data quality stages | Improves maintainability and reprocessing |
| Deployment | **Databricks Asset Bundles** | Packages Databricks resources | Repeatable, version-controlled deployment |
| Testing | **PyTest** | Tests project code | Helps catch logic defects before deployment |

---

# End-to-End Data Flow

## 1. Source → Azure Data Factory

### What?

The source is NYC Taxi data exposed through web/API-oriented source files. **Azure Data Factory (ADF)** is used as the orchestration layer.

### Why?

ADF is used to separate **pipeline orchestration from transformation processing**. It provides managed activities for ingestion, parameterization, looping, dependencies, monitoring and failure handling.

### When?

ADF is appropriate when the solution needs a managed cloud orchestration service for scheduled or event-driven data movement and workflow coordination.

### How?

The repository contains a `nycWebToDL` pipeline. It uses a **ForEach** activity to iterate through the required month range and passes the current month dynamically into the source dataset.

The pipeline also contains conditional routing because the source path/file naming pattern changes for months greater than 9.

### Engineering logic

```text
Month list
   │
   ▼
ForEach(month)
   │
   ├── Month <= 9  ──► source path pattern A
   │
   └── Month > 9   ──► source path pattern B
                         │
                         ▼
                    Copy to ADLS
```

### Why this logic instead of 12 pipelines?

Creating one pipeline per month would duplicate configuration and increase maintenance effort. A parameterized loop keeps the orchestration reusable and makes future changes easier.
demonstrates orchestration, parameterization, reusability and practical problem-solving.

---

## 2. Azure Data Factory → ADLS Gen2

### What?

ADF moves the source data into **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

### Why?

ADLS provides scalable cloud storage and separates storage from compute. Data can remain in the lake while different compute engines process it when required.

### When?

Use ADLS when the architecture needs a centralized data lake/lakehouse foundation for raw, refined and curated datasets.

### How?

ADF uses linked services and datasets to connect the source and target. Dynamic values are passed through pipeline parameters rather than hard-coding every month/file combination.

### Why separate orchestration and storage?

ADF controls **when and how data moves**. ADLS controls **where the data is stored**. This separation allows the compute layer to change without redesigning the storage layer.
tests cloud storage fundamentals, security awareness and architectural separation.

---

# 3. Bronze Layer — Raw Data

### What?

Bronze is the **raw/source-aligned layer** of the lakehouse.

### Why?

The primary purpose is to preserve source fidelity and provide a reliable recovery/reprocessing point.

### When?

Use a Bronze layer when raw source data needs to be retained before cleansing and business transformations.

### How?

Source data is landed in the data lake before downstream transformation. Transformation logic is kept separate from ingestion so the raw input remains available for debugging and reprocessing.

### Why not transform everything immediately?

If the source is transformed and the original data is discarded, debugging a downstream issue becomes harder. Keeping raw data gives engineers a reference point for reconciliation and replay.
demonstrates understanding of data lifecycle, lineage and recoverability.

---

# 4. Bronze → Silver with PySpark

### What?

Azure Databricks and **PySpark** process the raw data and create a cleaner, more reliable Silver layer.

### Why?

Spark is designed for distributed processing. It is useful when transformation workloads become large enough that distributed execution provides a performance and scalability advantage.

### When?

Use PySpark when transformations need distributed compute, complex data processing, joins, aggregations or scalable ETL/ELT execution.

### How?

Typical Silver processing includes:

- Schema and data-type handling
- Null and invalid-value handling
- Column standardization
- Deduplication where required
- Business-rule transformations
- Data quality validation
- Preparation for downstream analytical datasets

### Processing logic

```text
Bronze files
     │
     ▼
Read with Spark
     │
     ▼
Schema / type handling
     │
     ▼
Clean + validate
     │
     ▼
Transform / derive columns
     │
     ▼
Deduplicate where required
     │
     ▼
Silver dataset


---

# 5. Silver → Gold

### What?

Gold is the **business-ready and analytical layer**.

### Why?

Consumers should not need to understand raw-source complexity. Gold presents data in structures optimized for reporting, analytics and downstream consumption.

### When?

Use Gold when refined data needs business-level calculations, aggregations, metrics or consumer-oriented datasets.

### How?

Silver datasets are transformed into curated analytical structures using business rules, derived metrics and aggregations appropriate for downstream consumers.

### Why not expose Silver directly?

Silver is designed for reusable refined data. Gold provides a stable consumption layer so analytical users do not need to repeatedly implement the same business logic.
demonstrates the difference between technical data cleansing and business-facing data modeling.

---

# 6. Delta Lake

### What?

**Delta Lake** is the table/storage layer used for reliable lakehouse data management.

### Why?

Traditional Parquet files provide efficient columnar storage but do not by themselves provide the complete transactional table-management capabilities required for many production lakehouse workloads.

Delta adds capabilities such as transaction logging, versioning and reliable table operations.

### When?

Use Delta when the lakehouse requires reliable table updates, transactional behavior, version history, schema management or reproducible data operations.

### How?

Delta maintains a transaction log alongside data files. Table operations are recorded through the log, allowing the system to reason about table versions and committed changes.

### Core concepts

- ACID transactions
- Transaction log
- Data versioning
- Time Travel
- Schema management
- Reliable table updates

### Why Delta instead of only Parquet?

**Parquet = file format.**

**Delta = table/storage layer built on data files plus transaction-log capabilities.**

tests modern lakehouse fundamentals and whether the candidate understands what happens underneath the UI.

---

# 7. Databricks Asset Bundles (DAB)

### What?

The Databricks project is organized as a **Databricks Asset Bundle**, with configuration, resources, source code and tests separated into a version-controlled project structure.

### Why?

Manual notebook configuration does not scale well across development and production environments. Bundles allow Databricks resources and configuration to be treated more like deployable application code.

### When?

Use DAB when Databricks workloads need repeatable deployment, environment-specific configuration and source-controlled infrastructure/application definitions.

### How?

The repository contains:

```text
nyc_taxi_dab/
├── databricks.yml
├── resources/
├── src/
│   ├── silver/
│   └── gold/
├── tests/
└── pyproject.toml
```

The configuration includes development and production targets and pipeline resources.

### Why is this important?

The goal is to move from **"I ran a notebook"** toward **"I built a deployable data engineering project."**

### Interview selection value

Possible questions:

- Why use DAB?
- What is configuration vs code?
- How would dev and prod differ?
- How would you implement CI/CD?
- Where should secrets be stored?
- How would you roll back a deployment?


---

# 8. Parameterization and Reusability

### What?

Parameterization allows one pipeline/dataset definition to work with different values such as month, path or file name.

### Why?

It prevents hard-coded duplication and makes pipelines reusable.

### When?

Use parameterization whenever the same orchestration logic must process multiple datasets, partitions, dates, files or environments.

### How?

The ADF pipeline passes the current month through the ForEach loop into the source dataset and uses conditional logic where the source naming convention differs.

### Why is this better than hard-coding?

```text
BAD:
Pipeline_Jan
Pipeline_Feb
Pipeline_Mar
...
Pipeline_Dec

BETTER:
One reusable pipeline
        +
Month parameter
        +
ForEach
        +
Dynamic path/file logic
```
demonstrates reusable ETL design and reduction of operational maintenance.

---

# 9. Conditional Logic for Source Naming

### What?

The project uses conditional routing because the source path/file naming pattern changes for months greater than 9.

### Why?

Real-world source systems are not always perfectly standardized. The pipeline must adapt to the actual source contract rather than assuming one fixed pattern.

### When?

Conditional logic is useful when source behavior, schema, path or file naming varies based on a known business/technical rule.

### How?

```text
Current month
     │
     ▼
Condition
 ┌───┴────────┐
 │            │
 <= 9        > 9
 │            │
 ▼            ▼
Pattern A    Pattern B
 │            │
 └──────┬─────┘
        ▼
     Copy data
```

demonstrates ability to adapt orchestration logic to imperfect real-world systems.

---

# 10. Data Quality and Production Thinking

### What?

Data quality means validating that the dataset is structurally and logically usable before it becomes a trusted downstream dataset.

### Why?

A technically successful pipeline can still produce incorrect business data. Production data engineering requires both pipeline success and data correctness.

### When?

Quality checks should be applied at appropriate boundaries, especially before refined or consumer-facing datasets are published.

### How?

A production extension of this project could include:

- Null checks
- Data-type validation
- Duplicate checks
- Range validation
- Record-count reconciliation
- Source-to-target count checks
- Invalid-record quarantine
- Pipeline audit logging
- Alerting and monitoring


---

# 11. Security and Identity — Production Extension

### What?

Cloud data pipelines need controlled identities and access permissions for ADF, ADLS and Databricks resources.

### Why?

Credentials embedded directly in source code or configuration create security and operational risks.

### When?

Identity-based authentication should be preferred for production workloads wherever supported.

### How?

A production implementation can use managed identities/service principals, Azure RBAC, ACLs and a secrets-management solution such as Azure Key Vault according to organizational security requirements.

### Why this matters

Security should be part of architecture rather than an afterthought.

---

# 12. Testing

### What?

The Databricks project contains a `tests/` area and PyTest configuration.

### Why?

Data transformations are code and should be validated like software.

### When?

Testing should be part of development and deployment rather than performed only after a production failure.

### How?

Unit tests can validate transformation functions, expected schemas, edge cases and business rules. Integration/data-quality tests can validate interactions with actual datasets where appropriate.

### Why is testing valuable in Data Engineering?

A pipeline can run successfully while silently producing incorrect values. Automated tests reduce this risk.

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
│   ├── src/
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/
│   └── pyproject.toml
│
├── taxi_zone_lookup.csv
└── README.md
```
---

This project demonstrates the complete engineering lifecycle:

1. **Ingest** source data.
2. **Orchestrate** ingestion with Azure Data Factory.
3. **Store** data in ADLS Gen2.
4. **Preserve** source-aligned data in Bronze.
5. **Transform and validate** data using Databricks and PySpark.
6. **Curate** business-ready Gold datasets.
7. **Manage** lakehouse tables using Delta Lake.
8. **Deploy** Databricks resources using a structured bundle.
9. **Test** transformation code.
10. **Design for production** through security, monitoring, quality and recovery considerations.

The project therefore demonstrates more than a tutorial-style ETL flow. It shows how to reason about **architecture, scalability, maintainability, reusability, reliability and production readiness**.

---

# What an Interviewer Can Evaluate From This Repository

### 1. Azure knowledge

Can the candidate explain how ADF and ADLS work together and why cloud orchestration/storage are separated?

### 2. Data Engineering fundamentals

Can the candidate explain ETL/ELT, data lake architecture, partitioning, Parquet, schemas and data quality?

### 3. Spark knowledge

Can the candidate explain how PySpark executes transformations and how to troubleshoot performance problems?

### 4. Lakehouse knowledge

Can the candidate explain why Delta Lake is used and what the transaction log provides?

### 5. Engineering maturity

Can the candidate explain parameterization, testing, deployment, security, monitoring and recovery?

### 6. Problem-solving ability

Can the candidate explain **why a design was chosen**, what alternative approaches exist and what trade-offs each approach introduces?

> **Interview principle:** Do not memorize the README. Be able to explain every major decision using **What → Why → When → How → Trade-off**.

---

# Possible Production Enhancements

The current repository provides a strong project foundation. A production-grade implementation could additionally introduce:

- Metadata-driven ingestion
- Incremental / watermark-based processing where source attributes support it
- Data-quality framework
- Audit/control tables
- Idempotent pipeline design
- Centralized monitoring and alerting
- CI/CD through GitHub Actions or Azure DevOps
- Managed Identity and Azure RBAC
- Azure Key Vault integration
- Automated data reconciliation
- Dead-letter/quarantine handling for invalid records
- Partition and file-size optimization
- Cost monitoring and cluster/pipeline optimization

These are intentionally presented as **production extensions**, not as claims that every capability is already implemented in this repository.

---

# Skills Demonstrated

`Azure Data Factory` `ADLS Gen2` `Azure Databricks` `PySpark` `Python` `Apache Spark` `Delta Lake` `Parquet` `Medallion Architecture` `ETL/ELT` `Data Lakehouse` `API Ingestion` `Parameterization` `ForEach` `Databricks Asset Bundles` `PyTest` `Data Quality` `Cloud Data Engineering`

---

# Project Outcome

A structured Azure Data Engineering solution demonstrating:

**Source → ADF Orchestration → ADLS Gen2 → Bronze → PySpark/Databricks → Silver → Gold → Delta Lake**

with version-controlled Azure and Databricks engineering artifacts.

The strongest part of the project is not the dataset itself; it is the ability to explain **what was built, why each technology was selected, when each pattern is appropriate, how the implementation works, and how the design could be improved for production scale**.

---

## Official References

- Microsoft Learn — Medallion architecture: https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion
- Microsoft Learn — Azure Data Factory REST connector: https://learn.microsoft.com/en-us/azure/data-factory/connector-rest
- Microsoft Learn — Delta Lake architecture: https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/deployment-guide/delta-lake

---

### Author

**Anujit Dutta**  
Data Engineering | Azure | Databricks | PySpark | SQL
