# 🏥 Hospital Data Engineering Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)]()

> An end-to-end data engineering solution for hospital operations — ETL orchestration, data quality monitoring, analytics, and temporal dimension handling.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)  
- [Architecture & Workflow](#architecture--workflow)  
- [Features & Components](#features--components)  
- [Repository Structure](#repository-structure)  
- [Prerequisites](#prerequisites)  
- [How to Run](#how-to-run)  
- [Data Quality & Monitoring](#data-quality--monitoring)  
- [Slowly Changing Dimensions (SCD Type 2)](#slowly-changing-dimensions-scd-type-2)  
- [Testing Strategy](#testing-strategy)  
- [Future Enhancements](#future-enhancements)  
- [Credits & References](#credits--references)

---

## 🚀 Project Overview

This pipeline handles hospital domain data to provide:

- Real-time tracking of ETL load status  
- Validation and monitoring of data quality  
- Analysis of room occupancy and department revenue  
- Verification of ETL data loads  
- Implementation and testing of **Slowly Changing Dimensions Type 2 (SCD Type 2)**  

The goal is to enable robust, reliable analytics and reporting for hospital operations, with lineage, observability, and data correctness built in.

---

## 🏗 Architecture & Workflow

```

Source Systems → Ingestion Layer → Staging / Bronze Zone
↓
Transformation → Silver / Clean Zone
↓
SCD / Historical Modeling → Gold / Analytics Zone
↓
Monitoring, Reports & Dashboards

```

- **Ingestion / ETL:** Load data from source systems (e.g. hospital DBs or CSVs) into staging  
- **Transformation:** Clean, normalize, enrich data  
- **SCD Type 2:** Maintain historical changes in dimension tables  
- **Analytics zone:** Fact / dimension tables ready for reporting  
- **Monitoring & Quality:** Dashboards or scripts to alert on load failures, anomalies  

---

## ✅ Features & Components

- ETL orchestration (batch or scheduled)  
- Status tracking of ETL loads (success, failure, duration)  
- Data quality checks (nulls, duplicates, referential integrity)  
- Hospital room occupancy metrics  
- Departmental revenue analytics  
- Verification of data loads (cross-checks between layers)  
- SCD Type 2 (history retention) for critical dimension tables  
- Automated testing (unit / integration)  

---

## 📂 Repository Structure

```

hospital-data-engineering-pipeline/
├── ingestion/               # Scripts / notebooks for ingestion / staging
├── transformation/          # Cleaning, normalization, enrichment scripts
├── scd/                      # SCD Type 2 logic / scripts
├── analytics/                # Final models, fact & dimension definitions
├── monitoring/               # Data quality and pipeline monitoring dashboards / scripts
├── tests/                    # Test cases, data validations, integration tests
├── docs/                     # Architecture diagrams, data dictionary
└── README.md                 # This file

```

---

## 🛠 Prerequisites

Before running, you need:

- A relational database or data source (e.g. SQL Server, MySQL, or CSV files)  
- A target database or warehouse for analytics  
- Python (3.8+), or your ETL engine of choice  
- Necessary libraries / dependencies (e.g. pandas, SQLAlchemy, etc.)  
- Credentials / access to data sources and target  
- (Optional) Scheduling / orchestration tool (e.g. Airflow, Azure Data Factory)

---

## ▶ How to Run

1. **Set up configuration** (connection strings, credentials, parameters)  
2. **Ingestion / staging:** Run scripts under `ingestion/` to load raw data  
3. **Transformation:** Run cleaning & enrichment under `transformation/`  
4. **Apply SCD logic:** Execute scripts in `scd/` to update dimension history  
5. **Load into analytics zone:** Build fact & dimension tables in `analytics/`  
6. **Monitor & validate:** Run scripts in `monitoring/` to check loads & quality  
7. **Run tests:** Execute test cases from `tests/` to ensure correctness  

You may build these as one pipeline or modular steps.

---

## 📊 Data Quality & Monitoring

- Check for missing / null values in key columns  
- Deduplication and uniqueness constraints  
- Referential integrity between fact / dimension tables  
- Anomaly detection (outliers, abrupt revenue changes)  
- Load status logging (time taken, errors)  
- Alerts or dashboards to surface issues  

---

## ⏳ Slowly Changing Dimensions (SCD Type 2)

- Dimensions that track historical changes (e.g. patient address changes, room reassignments)  
- Maintain **effective start / end dates**, **current flag**, and **versioning**  
- Logic to **detect changes**, **insert new versions**, and **expire old rows**  
- Integration with fact tables via stable surrogate keys  

---

## 🧪 Testing Strategy

- **Unit tests**: Validate small pieces (e.g. transformation functions)  
- **Integration tests**: Validate end-to-end flow between layers  
- **Comparative checks**: Cross-compare raw vs processed data for row counts and sums  
- **Edge tests**: Unexpected data (nulls, duplicates, schema changes)  
- **Regression tests**: Ensure changes don’t break historical behavior  

---

## 🔮 Future Enhancements

- Add **real-time / streaming ingestion** (Kafka, Event Hubs)  
- Automate orchestration (Airflow, Prefect, Azure Data Factory)  
- Add **data lineage** and governance (e.g. OpenLineage, Data Catalog)  
- Incorporate **data versioning / Delta Lake / Iceberg**  
- Add **self-service dashboards** for stakeholders  
- Implement **alerting / notifications** for pipeline failures  
- Expand test coverage, benchmarking, optimization  

---

## 👏 Credits & References

- Author / Maintainer: **Muhammad Mehmaam (Mehmaam99)**  
