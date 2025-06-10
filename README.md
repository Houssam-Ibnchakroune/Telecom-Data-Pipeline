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

├─ conf/                                         # JARs
├─ src/
│  ├─ Synthetic_Data_Generation.py               # Faker CDR producer → Kafka
│  ├─ Streaming-Based_Mediation.py               # Spark Structured Streaming
│  ├─ RatingEngine.ipynb                         # Spark Batch (Rating Engine)
│  ├─ BillingEngine.ipynb                        # Spark Batch (Billing Engine)
│  ├─ kafka_streaming.py                         # Kafka Producer
│  ├─ initaisation_pg.py                         # Initialization en postgresql
│  └─ reporting.ipynb                            # Spark job -> CSV for BI
├─ resources/
│  ├─ product_catalog.csv
│  └─ rate_plans.csv
├─ cleaned_cdrs/                                 # output Médiation (Parquet)
├─ rated_cdrs/                                   # output Rating     (Parquet)
├─ billing/
│  └─ facturation_complete/                      # output Billing   (Parquet)
└─ report/                                       # CSV prêts pour Power BI
