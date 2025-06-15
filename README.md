# 📡 Telecom Big-Data Pipeline – Mediation → Rating → Billing → Reporting

> **Goal:** Convert raw network usages (CDR/EDR) into reliable revenue and business KPIs  
> **Stack:** Kafka | Apache Spark (Structured Streaming + Batch) | PostgreSQL | Parquet | Power BI  

---

## 🌐 End-to-End Architecture

```mermaid
flowchart LR
  A[Kafka<br>CDR topic] --> B(Spark Streaming<br>Mediation & Clean)
  B -->|Parquet /record_type| C[cleaned_cdrs]
  C --> D(Spark Batch<br>Rating Engine)
  subgraph PostgreSQL
    E[customers]
  end
  subgraph ressources
    F[rate_plans]
    G[product_catlog]
  end
  D --JDBC--> E
  D --CSV--> F
  D --CSV--> G
  D -->|Parquet| H[rated_cdrs/]
  H --> I(Spark Batch<br>Billing Engine)
  I -->|Parquet| J[facturation_complete/]
  J --> K[Power BI Dashboard]
```
## 📂 Project Layout
```
project-root/
├── conf/                                         # External dependencies (JARs, configs)
├── src/
│   ├── synthetic_data_generation.py              # Generates synthetic CDRs
│   ├── streaming_mediation.py                    # Spark Structured Streaming mediation
│   ├── rating_engine.ipynb                       # Spark batch job – Rating Engine
│   ├── billing_engine.ipynb                      # Spark batch job – Billing Engine
│   ├── kafka_streaming.py                        # Kafka producer
│   ├── initialise_pg.py                          # PostgreSQL schema & seed initialisation
│   └── reporting.ipynb                           # Spark job → CSV export for BI
├── resources/
│   ├── product_catalog.csv
│   └── rate_plans.csv
├── cleaned_cdrs/                                 # Parquet output from mediation stage
├── rated_cdrs/                                   # Parquet output from rating stage
├── billing/                                      # Parquet output from billing stage
└── report/                                       # Final CSVs ready for Power BI
```
## ⚙️ Prerequisites
| Tool                 | Version tested               |
| -------------------- | ---------------------------- |
| **Python**           | 3.9                         |
| **Spark**            | 3.5.x (⚠ driver JDBC ajouté) |
| **Kafka**            | 3.x                          |
| **PostgreSQL**       | ≥ 12                         |

## Installation

1. Clone the repository and navigate to the project folder:
    ```bash
    git clonehttps://github.com/Houssam-Ibnchakroune/Telecom-Data-Pipeline.git
    cd Telecom-Data-Pipeline
    ```


## Configuration

- **Kafka**: Ensure Kafka is installed and running locally.
- **PySpark**:  Modify `bootstrap.servers` in the spark configuration if necessary.
- **PostgreSQL**: Update PostgreSQL connection settings (host, user, password, database) in the code to match your local PostgreSQL setup.

## Usage

1. **Start zookeper**:
    ```bash
    # In one terminal
    .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
    ```
    
2. **Start Kafka-server**:
   ```bash
    # In one terminal
    .\bin\windows\kafka-server-start.bat .\config\server.properties
    ```
   
3. **run the CDR producer**:
   ```bash
    # In one terminal (Python 3.9+ active)
    python kafka_streaming.py
   
4. **run the job Spark Streaming (médiation)**:
   ```bash
    # In one terminal (or in IDE like vs code)
    python Streaming-Based_Mediation.py
   
5. **run the notebooks/ scripts batch de rating,billing and reporting **:
   ```bash
    # In one terminal (or in IDE like vs code)
    python run_pipline.py
6. **Visualize in Power BI**:
    - Start Power BI and read the CSVs files in report/ directory as the data source.
    - Create dashboards for  visualization.


## Example Dashboard in Grafana

In Power BI, create panels to visualize metrics like **Daily Revenue**, **Revenue by Plan and region**, and **Top 10 Data Consumers**.



## Contributeurs

- [Ibnchakroune Houssam](https://github.com/houssam-ibnchakroune) — Lead developer  
- [Kamal Salma](https://github.com/SALMAKAMAL21) — Lead developer
  
---

**Note :** This project is for educational and demonstration purposes. Results may vary depending on the quality and quantity of generated data.
