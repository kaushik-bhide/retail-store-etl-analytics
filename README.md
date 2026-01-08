# Store Sales ETL Analytics Pipeline (AWS S3 + Lambda + Glue + Athena + dbt)

## Project Overview
This project demonstrates an **end-to-end analytics ETL pipeline** built on AWS. Raw monthly transactional data lands in Amazon S3, is transformed into analytics-ready Parquet using AWS Lambda, cataloged with AWS Glue, queried in Amazon Athena, and modeled into BI/Product analytics outputs.  

**Dataset:** Fully **synthetic transactional order data** generated programmatically (no personal data).

---

## Architecture
**Flow**
1. **S3 (Raw layer)** — monthly nested JSON files (orders)
2. **AWS Lambda** — flattens nested JSON + explodes order items
3. **S3 (Processed layer)** — Parquet fact datasets partitioned by month
4. **AWS Glue Crawler** — creates Data Catalog tables and partitions
5. **Amazon Athena** — analytics views (BI + Product metrics)
6. **dbt (planned / in progress)** — staging + marts + tests
7. **CI/CD (planned / in progress)** — GitHub Actions to run `dbt build`

---
<img width="1024" height="1536" alt="ChatGPT Image Dec 21, 2025, 09_01_01 PM" src="https://github.com/user-attachments/assets/c5bbb088-c1ea-4efb-9ab3-dda6f391fde9" />

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

- AWS: S3, Lambda, Glue Crawler, Athena

- Python: pandas

- SQL: window functions, cohort analysis, churn logic

- dbt: analytics engineering (planned / in progress)

- CI/CD: GitHub Actions (planned / in progress)

## How to Run (High Level)

- Generate synthetic monthly JSON files using:
```text
data_generation/generate_store_sales_data.py
```

- Upload JSON files to the S3 raw bucket

- AWS Lambda processes files and writes partitioned Parquet outputs

- Run Glue crawler to update partitions

- Query tables and views in Athena

## Future Improvements

- Full dbt implementation (staging + marts)

- dbt tests and documentation

- CI/CD enforcement with GitHub Actions

- BI dashboards (Tableau/Looker Studio)
