# RetailQ Data Lakehouse
### Medallion Architecture ELT Pipeline on Databricks
#### Incremental Ingestion · Full Transformation · Star Schema · AI/BI Dashboard

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)

---

## Overview

You've sat in a meeting where someone questioned the numbers. Not because the data was wrong — but because it came from three different places and nobody could agree on which version was right.
That's not a data problem. That's a pipeline problem.

RetailQ is a data lakehouse built for retail businesses running on fragmented systems. Sales data in a database. Customer relationships in a CRM. Transactions in spreadsheets on someone's desktop. RetailQ ingests all three, transforms them into a single governed layer, and makes the numbers consistent — so when someone asks "what was revenue last month," there's one answer, not three.

The pipeline runs every morning without anyone touching it. Raw data from PostgreSQL, Salesforce CRM, and cloud file storage flows through a bronze ingestion layer, gets cleaned and validated in silver, and lands in a dimensional star schema in gold — ready for analysis. A semantic layer sits on top, so analysts can ask questions in plain English and get answers without writing SQL.

Every component is production-grade. Incremental ingestion. Data quality rules that drop bad records before they reach dashboards. A fully orchestrated workflow with dependency management. A live AI-powered dashboard that refreshes automatically.

Built on Databricks. End to end.

---

## Architecture

![Pipeline Architecture](Architecture/Pipeline%20Architecture)


```
```
---

## Tech Stack

| Layer | Technology |
|---|---|
| Platform | Databricks (Free Edition) |
| Storage Format | Delta Lake |
| Transformation | Lakeflow Spark Declarative Pipelines (DLT) |
| Ingestion | Lakeflow Connect, Auto Loader |
| Orchestration | Databricks Workflows |
| Governance | Unity Catalog |
| Semantic Layer | Databricks AI/BI Metric Views |
| Consumption | Databricks AI/BI Dashboard, Genie |
| Language | Python (PySpark), SQL |
| Source Systems | PostgreSQL (Neon), Salesforce CRM, Azure Blob Storage (CSV) |

---

## Data Sources

| Source | Type | Tables | Ingestion Method |
|---|---|---|---|
| PostgreSQL (Neon) | Relational Database | `product_catalog`, `inventory` | Lakeflow Connect (SCD2) |
| Salesforce CRM | SaaS CRM | `account`, `opportunity` | Lakeflow Connect (SCD2/SCD1) |
| Blob Storage | Cloud Storage (CSV) | `transactions` | Auto Loader |

### SCD Handling
- **SCD Type 2** — `product_catalog`, `account`: full history preserved with `__START_AT` / `__END_AT` timestamps
- **SCD Type 1** — `opportunity`, `inventory`: overwrites on change, no history kept

---

## Pipeline Structure

```
retail-databricks-pipeline/
├── Architecture/
│   ├── Pipeline Architecture       ← end-to-end architecture diagram
│   └── Data Model                  ← Gold layer star schema diagram
├── Ingestion/
│   └── 01_blob_to_bronze.py        ← Auto Loader CSV ingestion to Bronze
├── Transformation/
│   ├── bronze_to_silver/
│   │   ├── Account.py              ← Salesforce account → Silver
│   │   ├── inventory.py            ← PostgreSQL inventory → Silver
│   │   ├── opportunity.py          ← Salesforce opportunity → Silver
│   │   ├── product_catalog.py      ← PostgreSQL product catalog → Silver
│   │   └── transactions.py         ← CSV transactions → Silver
│   └── silver_to_gold/
│       ├── dim_calendar.py         ← Generated calendar dimension
│       ├── dim_customer.py         ← Customer dimension from Silver account
│       ├── dim_product.py          ← Product dimension from Silver product_catalog
│       └── fact_sales.py           ← Fact table with two-hop CRM join
├── Gold/
│   ├── 02_Gold_Views.sql           ← dim_customer, dim_product, fact_inventory views
│   └── 03_calender.py              ← Calendar dimension notebook
├── Semantics/
│   └── 04_Metric_View.py           ← AI/BI metric view definition (YAML)
├── LICENSE
└── README.md
```

---

## Medallion Architecture

### Bronze Layer — Raw Ingestion
Raw data lands as-is from source systems. No transformations applied. Audit columns added (`_ingestion_timestamp`, `_source_file`). SCD2 history preserved via Lakeflow Connect.

| Table | Schema | Source | Rows |
|---|---|---|---|
| `transaction` | `retail_q.blob_bronze` | CSV / Auto Loader | ~1,100 |
| `product_catalog` | `retail_q.postgres_bronze` | PostgreSQL | 21 (with history) |
| `inventory` | `retail_q.postgres_bronze` | PostgreSQL | 40 |
| `account` | `retail_q.salesforce_bronze` | Salesforce | 51 (with history) |
| `opportunity` | `retail_q.salesforce_bronze` | Salesforce | 100 |

### Silver Layer — Cleaned & Validated
Type casting, null handling, derived columns, data quality expectations. SCD2 columns renamed and `is_active` re-derived from `__END_AT IS NULL`.

| Table | Key Transformations | DQ Rules |
|---|---|---|
| `transactions` | Timestamp parse (`dd-MMM-yyyy hh.mm.ss a`), gross/net derived | NULL transaction_id/product_id → drop |
| `product_catalog` | `product_segment` derived from price tiers, `is_active` from SCD2 | NULL product_id/name/price → drop |
| `account` | PascalCase → snake_case, SCD2 preserved | NULL id → drop |
| `opportunity` | `deal_size` derived, 7 valid stages validated | NULL id → drop |
| `inventory` | 3-state `inventory_status` derived | NULL ids → drop |

**Data Quality Results:** 3 transactions dropped (NULL product_id), all other records passed.

### Gold Layer — Star Schema
Dimensional model optimised for analytics. All tables are DLT Materialised Views targeting `retail_q.retail_gold`.

![Data Model](Architecture/Data%20Model)

| Table | Type | Grain | Rows |
|---|---|---|---|
| `dim_customer` | Dimension | One row per active customer | 50 |
| `dim_product` | Dimension | One row per active product | 20 |
| `dim_calendar` | Dimension | One row per day (2026) | 365 |
| `fact_sales` | Fact | One row per transaction | 1,076 |

**Key design decision:** `fact_sales.customer_id` is resolved via a two-hop join:
```
transactions.opportunity_name
    → opportunity.name
    → opportunity.account_id (= customer_id)
```

---

## Semantic Layer

Defined in `Semantics/04_Metric_View.py` using Databricks AI/BI Metric View syntax (`WITH METRICS LANGUAGE YAML`).

**Source:** `retail_q.retail_gold.fact_sales`
**Joins:** `dim_product`, `dim_calendar`, `dim_customer`

### Measures (8)

| Measure | Expression | Format |
|---|---|---|
| Transaction Count | `COUNT(1)` | Number |
| Total Revenue | `SUM(net_sales_amount)` | Currency (INR) |
| Total Gross Amount | `SUM(gross_amount)` | Currency (INR) |
| Total Quantity Sold | `SUM(quantity)` | Number |
| Total Discount | `SUM(discount_amount)` | Currency (INR) |
| Average Transaction Value | `SUM(net_sales_amount) / COUNT(1)` | Currency (INR) |
| Unique Customers | `COUNT(DISTINCT customer_id)` | Number |
| Unique Products | `COUNT(DISTINCT product_id)` | Number |

### Dimensions (17)
Transaction Date, Year, Quarter, Month Name, Is Weekend, Product Category, Product Subcategory, Product Segment, Product Brand, Payment Mode, Sales Channel, Customer Type, Customer Name, City, State, Country, Industry

---

## Dashboard

**Retail Sales Analysis Dashboard** built using Databricks AI/BI Dashboard, generated via Genie Code and manually configured.

**KPI Row:** Total Revenue · Transaction Count · Total Quantity Sold · Total Discount · Avg Transaction Value · Unique Customers

**Sections:**
- Revenue Analysis — by Month, Quarter, Over Time
- Product Analysis — by Category, Segment, Top Products
- Customer Analysis — by Type, Top Customers, City, State
- Transaction Analysis — by Payment Mode, Sales Channel, Weekend vs Weekday

---

## Orchestration

**Job:** RetailQ End to End Job
**Schedule:** Daily at 04:00 AM (Africa/Lagos)
**Compute:** Serverless

```
blob_to_bronze        ┐
postgres_to_bronze    ├──→ silver ──→ gold ──→ Dashboard_refresh
salesforce_to_bronze  ┘
```

All tasks use `Run if: All succeeded` — downstream tasks are blocked if any upstream task fails.

---

## Key Metrics (Sample Data)

| Metric | Value |
|---|---|
| Total Revenue | ₹188M+ |
| Total Transactions | 1,076 |
| Total Quantity Sold | 3,296 |
| Products | 20 |
| Customers | 50 |
| Date Range | Jan 2026 – Mar 2026 |

---

## Known Limitations & Future Improvements

| Area | Current State | Future Improvement |
|---|---|---|
| Transformation | Full load on every run | Incremental using `dlt.read_stream()` and Delta MERGE |
| Unique Customers | 3 resolved via CRM join | Full customer resolution requires direct transaction-customer mapping |
| Product Names | `initcap()` side effects (e.g. "Iphone 15") | Custom capitalisation mapping |
| Negative quantities | Passed through as DQ warnings | Business rule to classify as returns |
| Free tier limits | 1 concurrent managed ingestion pipeline | Sequential Bronze ingestion as workaround |

---

## Project Context

Built as a guided learning project following a structured Databricks tutorial, with production-grade improvements applied throughout:

- Separated Silver and Gold into distinct DLT pipelines (tutorial used one)
- Used DLT Materialised Views for Gold instead of SQL views
- Fixed incorrect column references in the semantic layer (`amount` → `net_sales_amount`)
- Corrected currency to INR (tutorial used USD for Indian rupee data)
- Used integer `date_key` (YYYYMMDD) for calendar join instead of date type
- Re-derived `is_active` from SCD2 `__END_AT` instead of trusting source boolean
- Added `gross_amount` and `net_sales_amount` derived columns in Silver

---

## Author

**Nte Daniel** — Founder & Facilitator, DataWithDanny

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/daniel-nte-daniel)
[![YouTube](https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://youtube.com/@talkdatawithdanny)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/datawithdany)

---

*Built with Databricks Free Edition · Delta Lake · Lakeflow Spark Declarative Pipelines · Unity Catalog*
