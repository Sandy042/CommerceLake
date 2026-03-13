# 🛒 ComerceLake — Brazilian E-Commerce Data Lakehouse

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)

---

## 📋 Project Overview

**ComerceLake** is a production-grade **Data Lakehouse** built on Databricks, processing the **Olist Brazilian E-Commerce dataset** — one of the most comprehensive public e-commerce datasets available, containing ~100,000 orders across 9 interconnected datasets.

The project demonstrates end-to-end data engineering using the **Medallion Architecture** (Bronze → Silver → Gold), employing industry best practices including parameterized ingestion, incremental processing, CDC handling, data quality enforcement, and business-level aggregations.

| Property | Detail |
|---|---|
| **Dataset** | Olist Brazilian E-Commerce (Kaggle) |
| **Records** | ~100,000 orders, 9 CSV files |
| **Platform** | Databricks (Serverless) |
| **Storage** | Unity Catalog (Delta Lake) |
| **Orchestration** | Databricks Workflows |
| **Author** | Sandesh M S |
| **Date** | March 2026 |

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ComerceLake Architecture                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  📁 Raw CSV Files (9 files)                                     |
│  /Volumes/main/dev_storage/commercelake_raw/                    │
│         │                                                       │
│         ▼                                                       │
│  🥉 BRONZE LAYER (main.dev_bronze)                              │
│  Auto Loader + Parameterized PySpark Notebooks                  │
│  9 Tables — Raw data, no transformations                        │
│  + Audit columns (ingestion_date, file_path, file_modify_time)  │
│         │                                                       │
│         ▼                                                       │
│  🥈 SILVER LAYER (main.dev_silver)                              │
│  Lakeflow DLT — Python + apply_changes (SCD Type 1)             │
│  9 Tables — Cleansed, typed, deduplicated                       │
│         │                                                       │
│         ▼                                                       │
│  🥇 GOLD LAYER (main.dev_gold)                                  │
│  Lakeflow DLT — SQL Materialized Views                          │
│  4 Tables — Business aggregations & metrics                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Pipeline DAG

```
Raw Files
    │
    ├── customers.csv ──► bronze_customers ──► [view] ──► silver_customers ──► gold_revenue_trends
    ├── orders.csv ──────► bronze_orders ───► [view] ──► silver_orders ────► gold_delivery_sla
    ├── orderlist.csv ───► bronze_orderlist ► [view] ──► silver_orderlist ──► gold_product_sales
    ├── payments.csv ────► bronze_payments ─► [view] ──► silver_payments ──► gold_review_analysis
    ├── orderreview.csv ─► bronze_orderreview► [view] ──► silver_orderreview
    ├── products.csv ────► bronze_products ─► [view] ──► silver_products
    ├── seller.csv ──────► bronze_seller ───► [view] ──► silver_seller
    ├── geolocation.csv ─► bronze_geolocation► [view] ──► silver_geolocation
    └── productcategory ─► bronze_productcat ► [view] ──► silver_productcat
```

### Workflow Orchestration

```
ComerceLake_ETL_Job
│
├── Phase 1: Bronze Ingestion (Max 4 parallel tasks)
│       ├── bronze_customers_ingestion
│       ├── bronze_geolocation_ingestion
│       ├── bronze_orderlist_ingestion
│       ├── bronze_orders_ingestion
│       ├── bronze_payments_ingestion
│       ├── bronze_product_category_ingestion
│       ├── bronze_products_ingestion
│       ├── bronze_review_ingestion
│       └── bronze_seller_ingestion
│
└── Phase 2: DLT Pipeline (after all bronze tasks complete)
        └── commercelake_etl_pipeline
                ├── Silver (9 streaming tables)
                └── Gold (4 materialized views)
```

---

## 🗂️ File Structure

```
CommerceLake/
│
├── configs/                          # Domain configuration files
│   ├── customers.conf
│   ├── orders.conf
│   ├── orderlist.conf
│   ├── payments.conf
│   ├── orderreview.conf
│   ├── products.conf
│   ├── seller.conf
│   ├── geolocation.conf
│   └── productcategorytranslation.conf
│
├── notebooks/
│   └── transformations/
│       ├── bronze_ingestion_framework.py    # Parameterized bronze ingestion
│       ├── silver_ingestion_framework.py    # DLT Silver pipeline (Python)
│       └── gold_aggregation_framework.sql   # DLT Gold pipeline (SQL)
│
└── docs
       └──ReadMe.md
```

---

## 🔧 Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| **Platform** | Databricks (Serverless) | Unified analytics platform |
| **Storage Format** | Delta Lake | ACID transactions, time travel |
| **Catalog** | Unity Catalog | Governance, lineage, security |
| **Ingestion** | Auto Loader (cloudFiles) | Incremental file ingestion |
| **Pipeline** | Lakeflow Spark Declarative Pipelines | ETL pipeline framework |
| **Orchestration** | Databricks Workflows | Job scheduling & dependencies |
| **Language (Bronze)** | PySpark (Python) | Parameterized ingestion |
| **Language (Silver)** | Python + DLT decorators | Streaming transformations |
| **Language (Gold)** | SQL + DLT | Business aggregations |
| **CDC Pattern** | APPLY CHANGES INTO (SCD Type 1) | Deduplication & upserts |
| **Compute** | Serverless | Auto-scaling, zero config |

---

## 🥉 Bronze Layer

### Overview
The Bronze layer ingests raw CSV files **as-is** from Databricks Volumes using **Auto Loader** with a parameterized notebook pattern. Each domain has its own configuration file, and a single reusable notebook handles all 9 domains.

### Design Pattern: Parameterized Ingestion
```
domain parameter (e.g. "customers")
        ↓
Load customers.conf
        ↓
Auto Loader reads /commercelake_raw/customers/
        ↓
Append to main.dev_bronze.bronze_customers
        ↓
Log run stats to bronze_ingestion_audit
```

### Bronze Tables

| Table | Source File | Rows | Primary Key |
|---|---|---|---|
| bronze_customers | customers.csv | 99,441 | customer_id |
| bronze_orders | orders.csv | 99,441 | order_id |
| bronze_orderlist | olist_order_items.csv | 112,650 | order_id, order_item_id |
| bronze_payments | payments.csv | 103,886 | order_id, payment_sequential |
| bronze_orderreview | orderreview.csv | 104,162 | review_id |
| bronze_products | products.csv | 32,951 | product_id |
| bronze_seller | seller.csv | 3,095 | seller_id |
| bronze_geolocation | geolocation.csv | 1,000,163 | geolocation_zip_code_prefix |
| bronze_productcategorytranslation | productcategory.csv | 71 | product_category_name |

### Audit Columns Added
Every Bronze record gets 3 additional audit columns:

| Column | Type | Description |
|---|---|---|
| `ingestion_date` | DATE | Date the file was ingested |
| `file_modify_time` | TIMESTAMP | Last modified time of source file |
| `file_path` | STRING | Full path of the source file |

### Audit Log Table
```sql
main.dev_bronze.bronze_ingestion_audit
```
Tracks every pipeline run with domain, rows written, timestamp and status.

---

### Error Handling
The Bronze ingestion notebook implements production-grade error handling
to ensure pipeline reliability and audit visibility.

| Scenario | Handling |
|---|---|
| Config file not found | Raises exception with domain name |
| Missing required config key | Raises exception with key name |
| Ingestion failure | Caught, logged to audit as FAILED, re-raised |
| Audit always runs | `finally` block ensures audit log is always written |

#### Error Handling Flow
```
try:
    Auto Loader ingestion
    row_count = SUCCESS count
    run_status = 'SUCCESS'
except Exception:
    run_status = 'FAILED'
    raise Exception with domain + error details
finally:
    Always write to audit table (SUCCESS or FAILED)
```

#### Audit on Failure Example
```
domain    | target_table          | rows_written | run_status
orders    | dev_bronze.bronze_... | 0            | FAILED
```
```

## 🥈 Silver Layer

### Overview
The Silver layer cleanses, types, and deduplicates Bronze data using **Lakeflow Spark Declarative Pipelines** with Python. It uses a **Data-Driven Design** pattern — a single config list + loop creates all 9 silver tables dynamically.

### Design Pattern: Data-Driven DLT Pipeline
```python
silver_configs = [
    {"viewname": "...", "target": "...", "source": "...",
     "keys": [...], "sequence_by": "ingestion_date"},
    # ... 8 more entries
]

# One loop creates all 9 tables!
for config in silver_configs:
    dlt.create_streaming_table(config["target"])
    dlt.apply_changes(...)
```

### Transformations Applied

| Domain | Type Casts | Null Handling |
|---|---|---|
| products | dimensions→DOUBLE, counts→INT | Drop null product_id |
| orders | timestamps→TIMESTAMP | Drop null order_id, customer_id |
| orderlist | price→DOUBLE, date→TIMESTAMP | Drop null order_id, product_id |
| payments | value→DOUBLE, sequential→INT | Drop null order_id |
| orderreview | review_score→INT | Drop null review_id, order_id |
| customers | zip_code→INT | Drop null customer_id |
| sellers | zip_code→INT | Drop null seller_id |
| geolocation | lat/lng→DOUBLE, zip→INT | Drop null zip_code_prefix |
| productcategory | No casting needed | Drop all-null rows |

### CDC Pattern: APPLY CHANGES INTO
```
SCD Type 1 (Overwrite):
    New record arrives with same key
        → UPDATE existing row with latest values
        → No history kept
        → Always reflects current state
```

---

## 🥇 Gold Layer

### Overview
The Gold layer produces **business-ready aggregations** using **SQL Materialized Views** in the same DLT pipeline. Four tables answer key business questions for the Olist e-commerce platform.

### Gold Tables

#### 1. `gold_revenue_trends` 📈
**Business Question:** What is our monthly revenue performance across Brazilian states?

| Column | Description |
|---|---|
| `geolocation_state` | Brazilian state (SP, RJ, MG...) |
| `month_year` | Year-Month (DATE type) |
| `total_revenue` | Sum of price + freight per month/state |
| `total_orders` | Count of delivered orders |
| `average_order_value` | Average revenue per order |

#### 2. `gold_delivery_sla` 🚚
**Business Question:** Are we delivering on time by product category and state?

| Column | Description |
|---|---|
| `product_category_name_english` | Product category |
| `geolocation_state` | Customer state |
| `average_actual_delivery_days` | Avg actual days to deliver |
| `average_expected_delivery_days` | Avg promised days to deliver |
| `on_time_delivery_rate` | % of orders delivered on time |
| `total_late_deliveries` | Count of late deliveries |

#### 3. `gold_product_sales` 📦
**Business Question:** Which product categories drive most sales and when is peak?

| Column | Description |
|---|---|
| `product_category_name_english` | Product category |
| `year_month` | Year-Month (DATE type) |
| `total_orders` | Count of orders per month |
| `total_revenue` | Sum of revenue per month |
| `rolling_revenue_3m_window` | 3-month rolling avg revenue |

#### 4. `gold_review_analysis` ⭐
**Business Question:** Which product categories have best/worst customer satisfaction?

| Column | Description |
|---|---|
| `product_category_name_english` | Product category |
| `avg_review_score` | Average star rating (1-5) |
| `positive_reviews` | Count of 4-5 star reviews |
| `negative_reviews` | Count of 1-2 star reviews |
| `total_review_count` | Total reviews |
| `positive_percentage` | % positive reviews |
| `negative_percentage` | % negative reviews |

---

## 📊 Sample Results

### Revenue Trends (Top States)
```
State | Month      | Total Revenue  | Orders | Avg Order Value
SP    | 2018-05-01 | R$ 892,451.23 | 4,821  | R$ 185.12
RJ    | 2018-05-01 | R$ 234,123.45 | 1,203  | R$ 194.62
MG    | 2018-05-01 | R$ 189,234.56 | 998    | R$ 189.61
```

### Product Review Analysis (Sample)
```
Category                    | Avg Score | Positive% | Negative%
fashion_childrens_clothes   | 5⭐       | 87.5%     | 12.5%
security_and_services       | 3⭐       | 50.0%     | 50.0%
```

### Pipeline Performance
```
Total Job Duration    : ~3 minutes
Bronze (9 tasks)      : ~1m 30s (max 4 parallel)
Silver (9 tables)     : ~1m 00s
Gold (4 tables)       : ~0m 25s
Compute               : Serverless (zero config)
Photon Acceleration   : 87%
```

---

## 🔑 Key Design Decisions

| Decision | Rationale |
|---|---|
| **Parameterized Bronze** | Single notebook handles all 9 domains — DRY principle |
| **Config files per domain** | Externalized config — no code changes for new domains |
| **DLT for Silver/Gold** | Auto dependency resolution, built-in lineage, data quality |
| **SCD Type 1** | E-commerce data reflects current state — no history needed |
| **SQL for Gold** | Readable aggregations, easy for analysts to understand/modify |
| **Serverless compute** | Zero cluster management, auto-scaling, cost efficient |
| **Single DLT pipeline** | End-to-end lineage Bronze→Silver→Gold in Unity Catalog |
| **availableNow trigger** | Process all pending files then stop — efficient batch pattern |

---

## 🗃️ Unity Catalog Structure

```
main (catalog)
├── dev_bronze (schema)
│   ├── bronze_customers
│   ├── bronze_orders
│   ├── bronze_orderlist
│   ├── bronze_payments
│   ├── bronze_orderreview
│   ├── bronze_products
│   ├── bronze_seller
│   ├── bronze_geolocation
│   ├── bronze_productcategorytranslation
│   └── bronze_ingestion_audit
│
├── dev_silver (schema)
│   ├── silver_customers
│   ├── silver_orders
│   ├── silver_orderlist
│   ├── silver_payments
│   ├── silver_orderreview
│   ├── silver_products
│   ├── silver_seller
│   ├── silver_geolocation
│   └── silver_productcategorytranslation
│
├── dev_gold (schema)
│   ├── gold_revenue_trends
│   ├── gold_delivery_sla
│   ├── gold_product_sales
│   └── gold_review_analysis
│
└── dev_storage (schema)
    ├── commercelake_raw/     (source CSV volumes)
    └── commercelake_metadata/ (checkpoints & schemas)
```

---

*Built with ❤️ by Sandesh M S | March 2026*