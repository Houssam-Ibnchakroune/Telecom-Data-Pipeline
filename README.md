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
  I -->|Parquet| J[invoices_final/]
  J --> K[Power BI Dashboard]
