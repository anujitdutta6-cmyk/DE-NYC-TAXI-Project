# End-to-End NYC Taxi Data Engineering Pipeline

[![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-ETL%2FOrchestration-0078D4)](https://learn.microsoft.com/azure/data-factory/)
[![Databricks](https://img.shields.io/badge/Azure%20Databricks-PySpark%20%7C%20Delta%20Lake-EF3B2D?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-Data%20Lake-0078D4)](https://azure.microsoft.com/products/storage/data-lake-storage)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-0F5BFF)](https://delta.io/)

## Project Overview

This project implements an **end-to-end Azure data engineering pipeline for NYC Taxi trip data**, demonstrating how source data can be ingested, stored, transformed and curated using modern Azure lakehouse technologies.

The solution combines **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark and Delta Lake** and follows a **Bronze → Silver → Gold medallion architecture**.

The repository contains version-controlled ADF artifacts as well as a Databricks project organized with **Declarative Automation Bundles (DAB)**, pipeline resources, Silver/Gold notebooks and tests.

> **Interview focus:** This project demonstrates orchestration, parameterization, dynamic ingestion, distributed processing, lakehouse design, Delta Lake concepts and deployment-oriented engineering practices.

## Architecture

```text
                  NYC Taxi Data Source
                         │
                         ▼
              ┌──────────────────────┐
              │  Azure Data Factory  │
              │                      │
              │ • Parameters         │
              │ • ForEach            │
              │ • Conditional logic  │
              │ • Copy Activity      │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │     ADLS Gen2        │
              │                      │
              │  Bronze / Raw        │
              │        ↓             │
              │  Silver / Refined    │
              │        ↓             │
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

The architecture follows the lakehouse pattern recommended by Microsoft: Bronze preserves raw source data, Silver validates and refines it, and Gold provides business-ready datasets.

## Technology Stack

| Layer / Capability | Technology | Role |
|---|---|---|
| Source | NYC Taxi Web/API data | Source ingestion |
| Orchestration | **Azure Data Factory** | Pipeline control and ingestion |
| Storage | **ADLS Gen2** | Cloud data lake |
| Processing | **Azure Databricks** | Distributed processing |
| Transformation | **PySpark / Python** | Data cleansing and business logic |
| File format | **Parquet** | Efficient columnar storage |
| Table format | **Delta Lake** | Reliable lakehouse tables |
| Architecture | **Medallion** | Bronze → Silver → Gold |
| Deployment | **Databricks Asset Bundles (DAB)** | Repeatable deployment |
| Testing | **PyTest** | Automated project tests |

## End-to-End Pipeline

### 1. Source → Azure Data Factory

Azure Data Factory orchestrates the ingestion of NYC Taxi data from the web/API source.

The `nycWebToDL` pipeline uses a **ForEach** activity over the required month range and passes the current month dynamically into the source dataset. The pipeline also contains conditional routing for the different source path/file patterns used by the project.

This avoids creating separate pipelines for each month and demonstrates a reusable parameter-driven ingestion pattern.

### 2. ADF → ADLS Gen2

ADF copies the source Parquet files into Azure Data Lake Storage Gen2.

The repository separates ADF artifacts into:

```text
dataset/
linkedService/
pipeline/
factory/
```

This makes the orchestration layer easier to understand, version and maintain.

### 3. Bronze Layer

The Bronze layer is the raw/source-aligned layer. Its purpose is to preserve the source data with minimal transformation so that downstream processing can be re-run when required.

Key principles:

- Preserve source fidelity
- Maintain historical/raw input
- Support reprocessing
- Keep ingestion separate from transformation

### 4. Bronze → Silver with PySpark

Azure Databricks and PySpark perform the transformation workload.

The Silver layer is responsible for producing a cleaner and more reliable dataset through operations such as:

- Schema and data-type handling
- Data cleansing
- Null and invalid-value handling
- Column standardization
- Deduplication where required
- Business-rule transformations
- Preparation for downstream analytical datasets

### 5. Silver → Gold

The Gold layer contains curated datasets intended for analytical use.

Typical Gold-layer responsibilities include:

- Business-level calculations
- Aggregations
- Derived metrics
- Optimized analytical structures
- Consumer-ready datasets

The repository keeps Silver and Gold processing separated under the Databricks project structure.

## Delta Lake

Delta Lake provides the lakehouse table layer for reliable data management.

Key concepts demonstrated or covered by the project walkthrough include:

- ACID transactions
- Delta transaction log
- Data versioning
- Time Travel
- Reliable table updates
- Reproducible data processing

Delta Lake adds transactional and version-management capabilities on top of cloud object storage, making it suitable for production-style lakehouse workloads.

## Key Engineering Features

### Dynamic / Parameterized Ingestion

The ADF pipeline uses a month parameter and a ForEach loop to process multiple monthly partitions through the same orchestration logic.

The current implementation also contains a conditional branch for months greater than 9 because the source path/file naming pattern differs for that range.

### REST / API-Oriented Ingestion

Azure Data Factory supports REST-based ingestion and pagination patterns, allowing API responses to be processed without creating a separate hard-coded pipeline for every request.

### Medallion Architecture

```text
                 RAW DATA
                    │
                    ▼
              ┌───────────┐
              │  BRONZE   │
              │   Raw     │
              └─────┬─────┘
                    │
                    ▼
              ┌───────────┐
              │  SILVER   │
              │ Validated │
              │ Refined   │
              └─────┬─────┘
                    │
                    ▼
              ┌───────────┐
              │   GOLD    │
              │ Business  │
              │  Ready    │
              └───────────┘
```

This separation improves maintainability, data quality, reprocessing and downstream usability.

### Databricks Asset Bundles

The Databricks project is organized as a Declarative Automation Bundle with:

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

The project configuration includes development and production targets, pipeline resources and a serverless Databricks pipeline definition.

This demonstrates a deployment-oriented approach rather than relying only on manually executed notebooks.

## Repository Structure

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

## Interview Discussion Points

### Azure Data Factory

- How do you build a parameterized pipeline?
- Why use ForEach instead of creating 12 separate pipelines?
- How do you dynamically construct source paths?
- How would you implement API pagination?
- How do you handle failures and retries?
- How would you make the pipeline metadata-driven?

### Azure Data Lake / Medallion Architecture

- Why keep raw data in Bronze?
- What transformations belong in Silver?
- Why should Gold be business-oriented?
- How would you handle schema evolution?
- How would you reprocess historical data?

### Databricks / PySpark

- Why use Spark for large datasets?
- How does Spark distribute transformations?
- What is the difference between transformation and action?
- How would you optimize a slow Spark job?
- How do partitioning and file size affect performance?

### Delta Lake

- What is the Delta transaction log?
- How does Delta provide ACID transactions?
- What is Time Travel?
- How would you recover an older table version?
- Delta vs Parquet — when would you choose each?

### Deployment / Engineering Practices

- Why use Databricks Asset Bundles?
- How do development and production targets differ?
- How should secrets and credentials be managed?
- How can the project be tested automatically?
- How would you integrate CI/CD into this architecture?

## Why This Project Is Industry-Relevant

The project demonstrates the complete engineering lifecycle:

1. **Ingest** source data.
2. **Orchestrate** ingestion using Azure Data Factory.
3. **Store** raw data in ADLS Gen2.
4. **Process** data using Azure Databricks and PySpark.
5. **Refine** data through Bronze, Silver and Gold layers.
6. **Manage** analytical tables with Delta Lake.
7. **Deploy** Databricks resources using a structured bundle.
8. **Test** project code using PyTest.

The focus is therefore not just on a dataset, but on the **architecture, engineering decisions and operational patterns used to build a maintainable cloud data pipeline**.

## Learning / Project Walkthrough

The following timestamps are included as navigation references for the accompanying end-to-end walkthrough.

| Topic | Timestamp |
|---|---:|
| Project introduction / architecture | **0:00** |
| Medallion architecture | **9:06** |
| ADF pipeline and orchestration | **47:59** |
| Dynamic ingestion / API processing | **1:37:44** |
| Data transformation with PySpark | **1:55:49** |
| Service Principal / secure access scenario | **1:56:49** |
| Azure Databricks processing | **2:04:34** |
| Data warehousing / analytical layer | **3:18:23** |
| Delta Lake concepts | **3:43:50** |
| Delta Log / versioning | **3:50:55** |
| Time Travel | **4:00:00** |

**Walkthrough:** https://www.youtube.com/watch?v=LQY2fvEv4cM

## Official References

- Microsoft Learn — Medallion architecture: https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion
- Microsoft Learn — Azure Data Factory REST connector: https://learn.microsoft.com/en-us/azure/data-factory/connector-rest
- Microsoft Learn — Delta Lake architecture: https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/deployment-guide/delta-lake

## Skills Demonstrated

`Azure Data Factory` `ADLS Gen2` `Azure Databricks` `PySpark` `Python` `Apache Spark` `Delta Lake` `Parquet` `Medallion Architecture` `ETL/ELT` `Data Lakehouse` `API Ingestion` `Parameterization` `Databricks Asset Bundles` `PyTest`

## Project Outcome

A complete cloud data engineering workflow that demonstrates **source ingestion → orchestration → data lake storage → distributed transformation → medallion architecture → Delta Lake → curated analytical datasets**, with the implementation maintained as version-controlled engineering artifacts.

---

### Author

**Anujit Dutta**  
Data Engineering | Azure | Databricks | PySpark | SQL
