# Store Sales Analytics Platform  
End-to-End ETL & ELT Analytics Project (AWS, Lambda, dbt, Athena, Looker Studio)

## Project Overview
This project demonstrates a **modern analytics data platform** built using AWS services and dbt, covering the full lifecycle from **raw transactional data ingestion (ETL)** to **business-ready analytics and dashboards (ELT)**.  

**Dataset:** Fully **synthetic transactional order data** generated programmatically (no personal data).

---

## Architecture

High-level flow:

Raw JSON Orders  
→ Amazon S3 (Raw Layer)  
→ AWS Lambda (ETL: flatten & standardize)  
→ Amazon S3 (Parquet / Processed Layer)  
→ AWS Glue Data Catalog  
→ Amazon Athena  
→ dbt Cloud (ELT: Staging & Analytics Marts)  
→ Looker Studio (Dashboards)

### Architecture Principles
- **ETL** is used to handle semi-structured JSON and produce query-efficient Parquet
- **ELT** is used for analytics transformations and business logic
- Clear separation between ingestion, transformation, and consumption layers
- BI tools consume only curated analytics marts

---
<img width="1024" height="1536" alt="architecture_diagram" src="https://github.com/user-attachments/assets/8de5eb7b-40bb-471b-945d-72a487108eeb" />


## Repository Structure

```text
store-sales-analytics-pipeline/
├── README.md
├── LICENSE
├── architecture/
│   └── architecture_diagram.png
├── data_generation/
│   └── generate_store_sales_data.py
├── lambda/
│   └── flatten_orders_to_parquet.py
├── athena/
│   ├── base_views.sql
│   ├── bi_views.sql
│   └── product_views.sql
├── data/
│   └── analytics_outputs/
│       ├── monthly_revenue.csv
│       ├── revenue_mom.csv
│       ├── cohort_retention_first_6_months.csv
│       ├── monthly_churn.csv
│       ├── category_sales_monthly.csv
        └── monthly_active_customers.csv
├── dbt/
│ ├── models/
│ │ ├── staging/
│ │ │ ├── stg_fact_orders.sql
│ │ │ ├── stg_fact_order_items.sql
│ │ │ └── staging.yml
│ │ ├── marts/
│ │ │ ├── mart_monthly_revenue.sql
│ │ │ ├── mart_revenue_mom.sql
│ │ │ ├── mart_category_sales_monthly.sql
│ │ │ ├── mart_cohort_retention.sql
│ │ │ ├── mart_monthly_churn.sql
│ │ │ └── marts.yml
│ └── dbt_project.yml
│
├── dashboards/
│ └── looker/
│ ├── store_sales_dashboard.pdf
│ └── screenshots/
│
├── ci_cd/
│ └── dbt_cloud_job_success.png
└── README.md
```
## Data Model

### Fact Tables

**fact_orders**
- One row per order
- Partitioned by `order_year` and `order_month` (S3 partitions, stored as strings)

**fact_order_items**
- One row per item per order
- Partitioned by `order_year` and `order_month`

### Processed S3 Layout (Example)

```text
s3://<bucket>/processed/store_sales/fact_orders/order_year=2025/order_month=01/part-00000.parquet
s3://<bucket>/processed/store_sales/fact_order_items/order_year=2025/order_month=01/part-00000.parquet
```
## Synthetic Data Generation

This project uses synthetic transactional order data generated programmatically to ensure:

- No personal or sensitive data is used

- Full reproducibility of the pipeline

- Realistic support for BI and product analytics patterns

## Data Characteristics

- Time period: January 2025 onward (monthly files)

- Global customer base (e.g., US, UK, DE, FR, CA, IN)

## Nested JSON structure:

- orders

- customers

- payments

- order items

## Data Generation Script

The Python script that generates the synthetic data is located at:
```text
data_generation/generate_store_sales_data.py
```

It produces monthly files in the following format:
```text
orders_YYYY_MM.json
```
These files are uploaded incrementally to Amazon S3 and processed by the pipeline.

## Analytics Questions Answered
### Business Intelligence

- How does monthly revenue and AOV change over time?

- Which product categories contribute the most revenue?

- What is the month-over-month revenue growth?

### Product Analytics

- How many customers are active each month?

- How does customer retention evolve across cohorts?

- What is the monthly churn rate?

- Are engagement patterns consistent across geographies?

## Athena Analytics Views

Athena views form a reusable analytics layer and mirror dbt-style modeling.

### Base / Standardization

- ```text v_fact_orders_base ```
Standardizes partition columns and derives month-level keys.

### BI Views

- ```text v_monthly_revenue ```

- ```text v_revenue_mom ```
(window functions: LAG, running SUM)

- ```text v_category_sales_monthly ```

### Product Analytics Views

- ```text v_monthly_active_customers ```

- ```text v_cohort_retention ```

- ```text v_monthly_churn ```
(window function: LEAD)

SQL definitions are stored in:
```text
athena/
├── base_views.sql
├── bi_views.sql
└── product_views.sql
```
## Tech Stack

| Layer | Technology |
|-----|-----------|
| Object Storage | Amazon S3 |
| ETL Processing | AWS Lambda |
| Metadata Catalog | AWS Glue |
| Query Engine | Amazon Athena |
| Analytics Transformations (ELT) | dbt Cloud |
| Visualization | Looker Studio |
| CI/CD | dbt Cloud Jobs |
| Languages | SQL, Python |

---
## ELT: Analytics Modeling with dbt

### Source Layer
- Parquet datasets are registered in AWS Glue
- Declared as dbt sources

### Staging Layer
Staging models standardize and clean data:
- Type casting
- Date normalization
- Stable grains
- Flattened schemas

Models:
- `stg_fact_orders`
- `stg_fact_order_items`

### Analytics Mart Layer

| Mart | Description |
|----|------------|
| `mart_monthly_revenue` | Monthly revenue, order count, and AOV |
| `mart_revenue_mom` | Month-over-month growth and cumulative revenue |
| `mart_category_sales_monthly` | Category-level sales performance |
| `mart_cohort_retention` | Customer retention by acquisition cohort |
| `mart_monthly_churn` | Monthly customer churn metrics |

Data quality is enforced using dbt tests (not null, uniqueness).
---

## Dashboards (Looker Studio)

Analytics marts are visualized using **Looker Studio**, ensuring dashboards consume **only curated dbt models**.

Dashboard views include:
- Executive revenue overview
- Growth and month-over-month trends
- Category performance analysis
- Customer retention and churn metrics

A PDF export of the dashboard is included for offline review.

---

## CI/CD (dbt Cloud)

A dbt Cloud **Deployment Environment** is configured with a scheduled job that:

```bash
dbt run
dbt test
```
---
## How to Run (High Level)

- Generate synthetic monthly JSON files using:
```text
data_generation/generate_store_sales_data.py
```

- Upload JSON files to the S3 raw bucket

- AWS Lambda processes files and writes partitioned Parquet outputs

- Run Glue crawler to update partitions

- Query tables and views in Athena
