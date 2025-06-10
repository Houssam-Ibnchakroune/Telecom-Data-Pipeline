# 📡 Telecom Big-Data Pipeline – Mediation → Rating → Billing → Reporting

> **Goal:** Convert raw network usages (CDR/EDR) into reliable revenue and business KPIs  
> **Stack:** Kafka | Apache Spark (Structured Streaming + Batch) | PostgreSQL | Parquet | Power BI  

---

## 🌐 End-to-End Architecture

```mermaid
flowchart LR
  A[Kafka<br>CDR topic] --> B(Spark Streaming<br>Mediation & Clean)
  B -->|Parquet /record_type| C[Data Lake<br>cleaned_cdrs/]
  C --> D(Spark Batch<br>Rating Engine)
  subgraph PostgreSQL
    E[customers] 
    F[rate_plans] 
  end
  D --JDBC--> E
  D --CSV--> F
  D -->|Parquet| G[rated_cdrs/]
  G --> H(Spark Batch<br>Billing Engine)
  H -->|Parquet| I[invoices_final/]
  I --> J[Power BI Dashboard]
