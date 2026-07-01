# 🚀 Serverless Airbnb Sales ETL Pipeline

An automated, serverless data engineering pipeline that ingests raw CSV sales data from AWS S3, cleans and transforms it using AWS Glue, and loads optimized Parquet files into Databricks for multi-layer Delta Live Tables (DLT) analysis.

---

## 📐 Architecture Overview

1. **Ingestion Trigger:** An AWS Lambda function intercepts S3 `ObjectCreated` events whenever a new raw CSV file lands in the `airbnb_data/` directory.
2. **Serverless Compute:** Lambda dynamically extracts the file metadata and passes the exact target path to an AWS Glue PySpark ETL job, ensuring single-file incremental processing.
3. **Data Transformation:** The AWS Glue script standardizes date/timestamp schemas, handles data quality metrics (e.g., pre-computing financial metrics and handling null values), and outputs optimized Parquet configurations.
4. **Lakehouse Analysis:** Databricks Auto Loader (`cloudFiles`) detects the newly appended Parquet structure from the unified storage bucket and processes it through incremental Bronze and Silver Delta Live Tables (DLT) layer states.

---<img width="1408" height="768" alt="image_178b5313" src="https://github.com/user-attachments/assets/4c821f0b-55dc-422f-89d1-070400c0e509" />


## 📂 Project Repository Structure

*   `lambda_trigger.py` - Python script deployed to AWS Lambda to handle automated S3 event mapping.
*   `glue_etl.py` - PySpark script utilized by AWS Glue to perform target feature engineering and type casting.
*   `databricks_dlt.py` - Declarative Delta Live Tables pipeline processing framework using Medallion architecture configurations.
*   `README.md` - Technical project architecture and implementation reference.

---

## 🛠️ Detailed Component Specs

### 1. AWS Lambda (`lambda_trigger.py`)
*   **Runtime:** Python 3.12+
*   **Dependencies:** `boto3`, `urllib`
*   **Core Feature:** Utilizes JSON event metadata routing to ensure zero idle compute overhead. Only invokes active execution pipelines when new ingestion targets exist.

### 2. AWS Glue (`glue_etl.py`)
*   **Engine:** PySpark (Glue 4.0 / Spark 3.3+)
*   **Transforms Applied:**
    *   Dynamic schema mapping and CSV delimiter header isolation.
    *   Calculates `total_revenue` (`booking_amount` + `cleaning_fee` + `service_fee`).
    *   Generates analytical date partitions (`booking_year`, `booking_month`, `booking_day_of_week`).
    *   Implements data filtering rules (`nights_booked > 0`).

### 3. Databricks DLT (`databricks_dlt.py`)
*   **Framework:** Delta Live Tables Core Pipeline
*   **Bronze Layer:** Configured with Auto Loader to isolate state parsing logs dynamically.
*   **Silver Layer:** Applies data-quality assertions (`dlt.expect_or_drop`) to ensure downstream transactional integrity without reprocessing legacy partitions.

## Jobs & Pipelines
<img width="310" height="235" alt="image" src="https://github.com/user-attachments/assets/79770e1e-5299-4fe1-9bfe-1f1c0df55c5f" />
---

## ⚙️ Deployment & Cost Optimizations

To keep implementation operations highly cost-effective, the following patterns were built into the infrastructure design:
*   **Targeted Processing:** Instead of re-scanning the entire S3 history on every execution cycle, the workflow targets a single runtime path parameter string (`--new_s3_file`), driving Glue compute intervals to absolute minimums.
*   **Minimal Worker Profiles:** Configured to run on minimal scalable architectures using explicit worker thresholds (G.1X / 2 Workers) to maintain a low billing profile.
*   **Triggered Pipeline Semantics:** Databricks pipelines utilize explicit triggered scheduler states instead of continuous active configurations to eliminate idle runtime server hours.



