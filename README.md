# 🚢 Enterprise Tri-Modal Logistics Lakehouse

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00A5CF?style=for-the-badge&logo=delta-lake&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3D84B8?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

A production-grade, automated tri-modal logistics and risk-aggregation data lakehouse built on **Databricks**. This project ingests, cleans, and transforms heterogeneous real-time telemetry feeds across Air, Maritime, Terrestrial Traffic, and Weather domains into a unified Medallion Architecture managed via **Databricks Asset Bundles (DABs)** and version-controlled on GitHub.

---

## 🏗️ Architecture & Data Flow

The project follows the standard **Medallion Architecture**, implemented entirely within Databricks Unity Catalog:

1. **Bronze Layer (Raw Landing):** Raw JSON and API payloads ingested from multiple external transport sources and stored securely in Unity Catalog Volumes (`/Volumes/logistics_prod/raw_landing/`).
2. **Silver Layer (Cleaned & Flattened):** PySpark transformation pipelines that flatten nested arrays (e.g., OpenSky state vectors), handle schema enforcement, apply explicit `.mode("overwrite").option("overwriteSchema", "true")` patterns, and write out optimized Delta tables (`silver_flight_telemetry`, `silver_maritime_ais`, `silver_terrestrial_traffic`, `silver_weather_marine`).
3. **Gold Layer (Aggregated Risk & Intelligence):** A unified analytical risk-aggregation engine (`gold_risk_agg`) that joins cross-domain telemetry to calculate active transit counts, average speeds, weather conditions, and congestion indices.

---

## 📊 Data Sources

* **Air:** OpenSky Network API (Flight telemetry, altitude, and velocity vectors).
* **Maritime:** AISstream API (Vessel tracking, MMSI, maritime coordinates, and headings).
* **Terrestrial:** TomTom Traffic API (Road segment congestion indices and incident reporting).
* **Weather:** Open-Meteo API (Marine and regional meteorological conditions).

---

## 🛠️ Tech Stack & Tools

* **Data Processing & Orchestration:** Databricks, PySpark, Delta Lake, Unity Catalog, Databricks Jobs.
* **Deployment & CI/CD:** Databricks Asset Bundles (DABs).
* **Languages & Scripting:** Python, SQL.
* **Version Control:** Git & GitHub.

---

## 🚀 Key Engineering Challenges Solved

* **Heterogeneous Schema Normalization:** Handled deeply nested and variable structure JSON files from diverse public transport APIs by writing robust PySpark extraction and explosion logic.
* **Automated Table Initialization & Task Dependencies:** Eliminated `TABLE_OR_VIEW_NOT_FOUND` race conditions during multi-task DAG executions by programmatically verifying schema existence and ensuring reliable fallback data insertions in Unity Catalog.
* **Infrastructure-as-Code (IaC) Deployment:** Configured, deployed, and managed pipeline tasks and job triggers seamlessly using Databricks Asset Bundles (`databricks.yml`).

---

## ⚙️ Getting Started & Deployment

To deploy and run this project using Databricks Asset Bundles:

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/matolengwe96/enterprise-logistics-lakehouse.git](https://github.com/matolengwe96/enterprise-logistics-lakehouse.git)
   cd enterprise-logistics-lakehouse/enterprise_logistics_lakehouse