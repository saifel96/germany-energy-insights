# Germany Energy Insights Dashboard — End-to-End Pipeline

An end-to-end batch data pipeline that ingests, processes, and visualizes Germany's electricity generation data from the SMARD API (Bundesnetzagentur), covering 15 energy sources at 15-minute resolution from 2019 to 2026.

---

## Problem Statement

Germany's energy transition (*Energiewende*) is one of the most ambitious in the world, yet making sense of the raw generation data — across wind, solar, coal, gas, nuclear and more — requires significant processing effort. This project answers the question:

> **How is Germany's electricity mix evolving, and what share of demand is covered by renewables over time?**

The pipeline automates the full journey from raw API data to an interactive dashboard showing renewable share trends, generation mix by fuel type, and seasonal/hourly consumption patterns.

---

## Architecture

```
SMARD API (Bundesnetzagentur)
   │
   ▼
Python Ingestion Script          ← ingestion/get_data.py
   │  (weekly JSON files)
   ▼
GCS Raw Bucket
   │
   ▼
PySpark Transformation           ← spark/transform_smard.py
   │  (JSON → Parquet, partitioned by year/month)
   ▼
GCS Processed Bucket
   │
   ▼
BigQuery                         ← partitioned by month, clustered by category
   │
   ▼
dbt                              ← smard_analytics/
   │  (staging → facts → marts)
   ▼
Looker Studio Dashboard
```

All steps are orchestrated by **Kestra** (daily schedule). Cloud infrastructure is provisioned with **Terraform**.

![Pipeline Workflow](screens/workflow.png)

---

## Technologies

| Layer | Tool |
|---|---|
| Cloud | Google Cloud Platform (GCP) |
| Infrastructure as Code | Terraform |
| Workflow Orchestration | Kestra |
| Data Lake | Google Cloud Storage (GCS) |
| Batch Processing | Apache Spark (PySpark) |
| Data Warehouse | Google BigQuery |
| Transformations | dbt (data build tool) |
| Dashboard | Looker Studio |

---

## Dataset

**Source:** [SMARD — Strommarktdaten (Bundesnetzagentur)](https://www.smard.de/)

- **Resolution:** 15-minute intervals
- **Region:** DE-LU (Germany + Luxembourg)
- **Period:** January 2019 – April 2026
- **15 energy categories tracked:**

| Category | Type |
|---|---|
| Wind Onshore | Renewable |
| Wind Offshore | Renewable |
| Photovoltaik (Solar) | Renewable |
| Biomasse | Renewable |
| Wasserkraft (Hydro) | Renewable |
| Sonstige Erneuerbare | Renewable |
| Braunkohle (Lignite) | Fossil |
| Steinkohle (Hard Coal) | Fossil |
| Erdgas (Gas) | Fossil |
| Kernenergie (Nuclear) | Conventional |
| Pumpspeicher (Pumped Storage) | Storage |
| Sonstige Konventionelle | Conventional |
| Gesamt / Netzlast (Total Load) | Consumption |
| Residuallast (Residual Load) | Consumption |

---

## Pipeline Details

### 1. Ingestion (`ingestion/get_data.py`)

Queries the SMARD API in two steps:
1. Fetch the timestamp index for each energy category (filter ID)
2. Download each weekly JSON file and save to `data/` locally

### 2. Upload Raw to GCS (`ingestion/upload_data_to_gcs.py`)

Uploads all local JSON files to the **GCS Raw bucket** using the Google Cloud Storage Python client.

### 3. Spark Transformation (`spark/transform_smard.py`)

PySpark reads all raw JSON files, explodes the nested `series` array (one row per 15-minute interval), maps `filter_id` to human-readable `category` names, and writes the result as **Parquet partitioned by `year` and `month`**.

### 4. Upload Processed to GCS (`spark/upload_processed_data_to_gcs.py`)

Uploads the partitioned Parquet files to the **GCS Processed bucket**, preserving the Hive-style partition structure.

### 5. BigQuery

Processed Parquet files are loaded into BigQuery with a two-dataset separation:
- **`smard_data`** — raw ingestion layer: external table pointing to GCS, partitioned by `TIMESTAMP_TRUNC(timeseries, MONTH)`, clustered by `category`
- **`dbt_smard`** — analytics layer: production-ready tables and views built by dbt, cleanly separated from raw ingestion

The partition strategy eliminates full-table scans for time-range queries; clustering reduces bytes read when filtering by energy type.

### 6. dbt Transformations (`smard_analytics/`)

A layered dbt project with four models:

```
smard_partitioned (BigQuery source)
       │
       ▼
stg_energy_data        ← view: renames columns, extracts date/hour
       │
       ▼
fct_renewable_share    ← table: PIVOTs all 15 categories into columns,
       │                         computes total_renewables, total_fossils,
       │                         and renewable_share_pct
       ├──► mrt_daily_energy_stats   ← table: daily avg renewable share + day_type label
       └──► mrt_hourly_patterns      ← table: avg solar, wind, coal, load by hour-of-day
```

---

## Orchestration

All pipeline steps run automatically via **Kestra** on a daily schedule (`kestra/kestra.yaml`):

| Step | Task ID | Method |
| :--- | :--- | :--- |
| 1. Fetch raw data from SMARD API | `1_download_smard` | Kestra → Python |
| 2. Upload JSON to GCS Raw | `2_upload_raw_to_gcs` | Kestra → Python |
| 3. PySpark transformation | `3_spark_transform` | Kestra → PySpark |
| 4. Upload Parquet to GCS Processed | `4_upload_processed_to_gcs` | Kestra → Python |
| 5. Run dbt models | `5_run_dbt` | Kestra → dbt |

GCP credentials are stored as a Kestra KV secret (`gcp_creds`) — no keys are hardcoded.

---

## Dashboard

Built in **Looker Studio** with three pages covering the two required tile types:

- **Categorical distribution:** generation mix by fuel type (bar chart)
- **Temporal distribution:** daily renewable share trend line (2019–2026)

**[View Live Dashboard](https://datastudio.google.com/reporting/6b7675eb-10f5-47d8-973c-548781d10a82)**

### Overview
![Overview](screens/overview.png)

### Fuel Trends
![Fuel Trends](screens/fuel_trends.png)

### Patterns & Seasonality
![Patterns and Seasonality](screens/patterns_seasonality.png)

---

## Infrastructure (Terraform)

```hcl
google_storage_bucket   "raw_data"        # mastr-pipeline-de-raw-data
google_storage_bucket   "processed-data"  # mastr-pipeline-de-processed-data
google_bigquery_dataset "smard_dataset"   # smard_data
```

---

## Reproducibility

### Prerequisites

- Python 3.10+
- Java 17 (required for PySpark compatibility)
- GCP project with a service account key
- Terraform CLI
- Kestra instance (see `docker-compose.yml`)

### Setup

**1. Clone the repo**

```bash
git clone https://github.com/saifel96/germany-energy-insights.git
cd germany-energy-insights
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**2. Configure credentials**

```bash
cp .env.example .env
# Fill in your GCP_PROJECT, GCP_REGION, and path to your service account key
```

**3. Provision infrastructure**

```bash
cd terraform
terraform init
terraform apply -var="project=<your-gcp-project-id>"
```

**4. Start Kestra**

```bash
docker compose up -d
```

Open `http://localhost:8080`, import `kestra/kestra.yaml`, add your GCP service account JSON as a KV secret named `gcp_creds`, then trigger the pipeline.

**5. Run manually (alternative)**

```bash
# Ingest
python ingestion/get_data.py

# Transform (requires Java 17)
cd spark && spark-submit transform_smard.py

# Upload to GCS
python spark/upload_processed_data_to_gcs.py

# Run dbt
cd smard_analytics
dbt deps
dbt run
dbt test
```

**6. Connect Looker Studio**

Connect to BigQuery and use `mrt_daily_energy_stats` and `mrt_hourly_patterns` as data sources.

---

## Project Structure

```
germany-energy-insights/
├── ingestion/
│   ├── get_data.py                      # SMARD API ingestion
│   └── upload_data_to_gcs.py            # Upload raw JSON to GCS
├── spark/
│   ├── transform_smard.py               # PySpark transformation
│   └── upload_processed_data_to_gcs.py  # Upload Parquet to GCS
├── smard_analytics/                     # dbt project
│   ├── models/
│   │   ├── source.yaml
│   │   ├── stg_energy_data.sql
│   │   ├── fct_renewable_share.sql
│   │   ├── mrt_daily_energy_stats.sql
│   │   └── mrt_hourly_patterns.sql
│   └── dbt_project.yml
├── terraform/
│   ├── main.tf
│   └── variables.tf
├── kestra/
│   └── kestra.yaml                      # Kestra pipeline definition
├── screens/                             # Dashboard screenshots
├── requirements.txt
├── docker-compose.yml
└── .env.example
```

---

## Engineering Challenges

### 1. JVM & Hadoop Compatibility (PySpark)
Kestra's environment (Java 21+) removed `SecurityManager`, which Hadoop's `UserGroupInformation.getCurrentUser()` relies on. No JVM flag can restore this in Java 21+. Resolution: the Kestra Spark task installs `openjdk-17` at runtime and overrides `JAVA_HOME` before invoking PySpark, ensuring Spark-Hadoop connector stability.

### 2. Orchestration & Credential Isolation
Running dbt inside a Kestra Process Runner required secure credential injection without hardcoding. Resolution: the GCP service account JSON is stored as a Kestra KV secret (`gcp_creds`), injected into the task environment, and written to `/tmp/gcp_creds.json` at runtime — giving dbt access without exposing keys in the repository.

### 3. Full-History Data Integrity (2019–2026)
Incremental dbt models initially skipped historical years (2019–2022) due to BigQuery metadata caching of external table partitions. Resolution: a targeted `dbt run --full-refresh` dropped existing relations and rebuilt the entire 7-year lineage, resulting in over **245,000 rows** of validated energy metrics.

---

## Results

- **Data volume:** 245,000+ rows at 15-minute resolution
- **Time range:** January 2019 – April 2026
- **Average renewable share:** ~49.7%, consistent with official *Energiewende* benchmarks
- **Notable trend:** Nuclear energy contributed 8.4% of the mix until its phase-out in April 2023; Wind Onshore has since grown to 22.9%, becoming the single largest generation source

