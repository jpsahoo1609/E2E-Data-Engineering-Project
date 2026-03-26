# NYC Taxi Azure Data Engineering Project
**ADF • ADLS Gen2 • Databricks • Delta Lake • Power BI**

# Project Overview
This is an end‑to‑end **modern data engineering pipeline** built on Azure using the **Medallion Architecture (Bronze → Silver → Gold)**.\
It ingests NYC Taxi trip data from the public API, performs scalable transformations using Databricks (PySpark + Delta Lake), and serves analytics to Power BI.

This project replicates how real enterprise data engineering teams build production‑ready data systems.
# Architecture Diagram
<img width="613" height="410" alt="Achitecture" src="https://github.com/user-attachments/assets/b19c6f8b-678a-4aa8-9230-e593aec04ff4" />

# Key Highlights
**1. Ingestion Layer(Azure Data Factory)**
- Fetches data from the official NYC Taxi API
- Stores raw Parquet files in Bronze layer (ADLS Gen2)
- Implements initial load + incremental load strategy
- Uses Service Principal authentication
  
**2. Transformation Layer (Azure Databricks)**

- Reads Bronze layer data using PySpark
- Cleans schema inconsistencies, handles corrupt files
- Performs business transformations
- Writes optimized Delta tables to **Silver layer**

**3. Gold Layer (Delta Lake)**

- Creates curated Delta tables for analytics
- Implements **Delta Time Travel**, versioning & optimization
- Supports Power BI with fast query performance


**3. Analytics Layer (Power BI)**

- Built dashboards on:
   - Vendor Id and fare amount
    - Trip month vs trip distance
    - Borough by service zone
