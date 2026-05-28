# Databricks Lakeflow Ingestion Pipeline
### Incremental Data Ingestion from PostgreSQL to Databricks Bronze Layer

> **Part of:** [ShopEase E-Commerce Customer Churn & Retention Analysis](https://github.com/Dapinder-BI/ecommerce-churn-analysis) — a full end-to-end data engineering and analytics portfolio project.

---

## Problem Statement

> *"How do you reliably and efficiently move transactional data from a cloud PostgreSQL database into a Databricks Data Lakehouse — without writing complex ETL code, without reprocessing data you've already loaded, and in a way that scales as data grows?"*

In real-world data projects, the source system (OLTP database) is always live — data is being inserted and updated constantly. A naive approach (load everything every time) is slow, expensive, and doesn't scale. The solution is **incremental ingestion** — only processing what's new or changed since the last run.

This project demonstrates exactly that using **Databricks Lakeflow Ingestion Pipeline**.

---

## Architecture

```
┌─────────────────────────────┐
│   Neon PostgreSQL (OLTP)    │
│   5 tables · 60,259 rows    │
│   cloud-hosted, free tier   │
└──────────────┬──────────────┘
               │
               │  Lakeflow Ingestion Pipeline
               │  ├── Incremental sync (cursor: updated_at)
               │  ├── Primary key-based upserts
               │  ├── SCD Type 1 / SCD Type 2 per table
               │  └── Serverless compute · 53 seconds
               │
               ▼
┌─────────────────────────────────────────────────────┐
│         Databricks Unity Catalog                    │
│   Catalog: e_commerce_customer_churn_retention_analysis  │
│   Schema:  bronze                                   │
│                                                     │
│   ├── customers    (5,000 rows)  · Streaming Table  │
│   ├── orders      (50,000 rows)  · Streaming Table  │
│   ├── products       (200 rows)  · Streaming Table  │
│   ├── returns      (5,000 rows)  · Streaming Table  │
│   └── churn_flags  (4,259 rows)  · Streaming Table  │
└─────────────────────────────────────────────────────┘
```

---

## What is Lakeflow Ingestion Pipeline?

**Databricks Lakeflow Ingestion Pipeline** is a no-code ingestion feature in Databricks (Jobs & Pipelines → Ingestion Pipeline). It connects directly to source databases (PostgreSQL, MySQL, Salesforce, etc.) and ingests data into Delta Lake tables automatically.

### Why Lakeflow instead of manual JDBC notebooks?

| Feature | Lakeflow Pipeline | Manual JDBC Notebook |
|---------|------------------|---------------------|
| Code required | None — UI configured | Python notebook |
| Incremental sync | Automatic (cursor column) | Manual watermark logic |
| Scheduling | Built-in | Databricks Jobs |
| Monitoring | Pipeline run history UI | Print statements |
| Error handling | Automatic retries | Manual try/except |
| Scalability | Serverless, auto-scales | Fixed cluster size |

In a real project, you use the right tool for the job. Lakeflow handles ingestion so that engineering effort is focused on **transformation logic** (Silver/Gold layers) — not plumbing.

---

## What are Streaming Tables?

When Lakeflow creates Bronze tables, it creates them as **Streaming Tables** (powered by Delta Live Tables under the hood).

### Batch Table vs Streaming Table

```
Batch Table (traditional approach):
  Run 1 → Load ALL 50,000 orders ──────────────────► Bronze
  Run 2 → Load ALL 50,000 orders ──────────────────► Bronze (reprocessed everything!)
  Run 3 → Load ALL 50,000 orders ──────────────────► Bronze (wasted compute!)

Streaming Table (Lakeflow approach):
  Run 1 → Load ALL 50,000 orders ──────────────────► Bronze
  Run 2 → Load only 500 NEW orders ────────────────► Bronze (efficient!)
  Run 3 → Load only 200 NEW orders ────────────────► Bronze (only what changed!)
```

### How it works
- Lakeflow uses `updated_at` as the **cursor column** — the timestamp that tells it "only sync rows newer than my last run"
- A **checkpoint** is maintained automatically — the pipeline always knows exactly where it left off
- If the pipeline fails mid-run, it resumes from the checkpoint — no data loss, no duplicates

### Key insight
Streaming Tables describe **how data gets in** — not how you read it. When querying in Silver notebooks, they behave exactly like regular Delta tables:
```sql
SELECT * FROM e_commerce_customer_churn_retention_analysis.bronze.customers
```

---

## SCD Type 1 vs SCD Type 2 — History Tracking Decision

This was one of the most important configuration decisions in this pipeline. Each table was evaluated based on the **nature of its data**.

### What is SCD?

**Slowly Changing Dimension (SCD)** defines how you handle changes to data over time.

| Type | Behaviour | Example |
|------|-----------|---------|
| **SCD Type 1** | Overwrite — keep only current state | Customer changes email → old email gone |
| **SCD Type 2** | Track history — keep all versions | Customer changes email → both old and new emails stored with timestamps |

In Lakeflow, this is controlled by the **History Tracking** toggle per table.

---

### History Tracking OFF → SCD Type 1

**Tables configured this way:** `orders`, `returns`

**Why:**
These are **fact/event tables** — they record what happened. An order placed is a permanent event. A return processed is a permanent event. There is no meaningful "history" to track — the event itself is immutable.

Keeping SCD Type 1 here:
- Keeps Bronze tables clean and simple
- Avoids unnecessary `__START_AT` / `__END_AT` columns on fact data
- Reduces storage and complexity

```
orders table (SCD Type 1):
  order_id=1001, status=Completed, amount=250.00  ← only current state stored
```

---

### History Tracking ON → SCD Type 2

**Tables configured this way:** `customers`, `products`, `churn_flags`

**Why:**
These are **dimension/entity tables** — they describe things that change over time, and that history is analytically valuable.

| Table | What changes | Why history matters |
|-------|-------------|---------------------|
| `customers` | customer_segment (New → Premium), email, city | Track when a customer was upgraded — important for cohort analysis |
| `products` | unit_price, description | Track price changes — affects revenue analysis accuracy |
| `churn_flags` | churn_segment (Active → At Risk → Churned) | **Most critical** — shows the journey of a customer from active to churned over time |

Lakeflow adds these system columns automatically for SCD Type 2 tables:

| Column | Description |
|--------|-------------|
| `__START_AT` | When this version of the record became active |
| `__END_AT` | When this version was superseded (NULL = current) |
| `__CURRENT` | Boolean — True means this is the latest version |

**Example — customer changes segment:**
```
customer_id=101, segment=New,     __START_AT=2023-01-15, __END_AT=2023-08-01, __CURRENT=False
customer_id=101, segment=Regular, __START_AT=2023-08-01, __END_AT=2024-03-10, __CURRENT=False
customer_id=101, segment=Premium, __START_AT=2024-03-10, __END_AT=NULL,       __CURRENT=True
```

**In Silver notebooks**, we filter for current records:
```python
df = spark.read.format("delta") \
    .load("e_commerce_customer_churn_retention_analysis.bronze.customers") \
    .filter("__CURRENT = true")
```

---

## Pipeline Configuration

### Connection Details

| Setting | Value |
|---------|-------|
| Source | Neon PostgreSQL (cloud-hosted) |
| Host | Neon pooler endpoint |
| Database | neondb |
| SSL | Required |
| Compute | Databricks Serverless |
| Destination catalog | e_commerce_customer_churn_retention_analysis |
| Destination schema | bronze |

### Table Configuration

| Table | Primary Key | Cursor Column | History Tracking | SCD Type |
|-------|-------------|---------------|-----------------|----------|
| customers | customer_id | updated_at | ON | Type 2 |
| orders | order_id | updated_at | OFF | Type 1 |
| products | product_id | updated_at | ON | Type 2 |
| returns | return_id | updated_at | OFF | Type 1 |
| churn_flags | customer_id | updated_at | ON | Type 2 |

---

## Results

### Pipeline Run

| Metric | Value |
|--------|-------|
| Run date | May 28, 2026 |
| Status | Completed |
| Total duration | 53 seconds |
| Errors | 0 |
| Warnings | 0 |
| Compute | Serverless |

### Row Count Verification

```sql
SELECT 'customers'   AS tbl, COUNT(*) AS rows FROM bronze.customers   UNION ALL
SELECT 'orders'      AS tbl, COUNT(*) AS rows FROM bronze.orders      UNION ALL
SELECT 'products'    AS tbl, COUNT(*) AS rows FROM bronze.products    UNION ALL
SELECT 'returns'     AS tbl, COUNT(*) AS rows FROM bronze.returns     UNION ALL
SELECT 'churn_flags' AS tbl, COUNT(*) AS rows FROM bronze.churn_flags
```

| Table | Rows |
|-------|------|
| customers | 5,000 |
| orders | 50,000 |
| products | 200 |
| returns | 5,000 |
| churn_flags | 4,259 |
| **Total** | **64,459** |

### Screenshots

**Pipeline Flow — All 5 tables ingested successfully**

![Pipeline Flow](screenshots/pipeline_flow.png)

**Row Count Verification — SQL Editor**

![Row Count Verification](screenshots/row_count_verification.png)

**Catalog View — Bronze schema with all 5 tables**

![Catalog View](screenshots/catalog_view.png)

---

## Key Concepts Demonstrated

| Concept | Demonstrated By |
|---------|----------------|
| Incremental ingestion | Cursor column (`updated_at`) — only new/changed rows processed per run |
| SCD Type 1 | History Tracking OFF on `orders` and `returns` — fact tables, immutable events |
| SCD Type 2 | History Tracking ON on `customers`, `products`, `churn_flags` — dimension tables, changes tracked |
| Streaming Tables | All Bronze tables are Delta Live Tables streaming tables — checkpoint-based, fault tolerant |
| Unity Catalog | Tables registered in Unity Catalog — governed, discoverable, access-controlled |
| Serverless compute | No cluster management — Databricks auto-scales compute for the pipeline |

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Neon PostgreSQL | Source OLTP database (cloud-hosted, free tier) |
| Databricks Free Edition | Data Lakehouse platform |
| Lakeflow Ingestion Pipeline | No-code incremental ingestion |
| Delta Lake | Open table format for Bronze tables |
| Unity Catalog | Data governance and cataloging |
| Databricks Serverless | Compute for pipeline execution |

---

## Part of a Larger Project

This repo covers **Phase 3 (Bronze Layer)** of the full ShopEase project:

```
Phase 1  ✅  Documentation (6 industry-standard documents)
Phase 2  ✅  Synthetic data generation + Neon PostgreSQL upload
Phase 3  ✅  Bronze layer — Lakeflow Ingestion Pipeline  ← THIS REPO
Phase 4  🔲  Silver layer — PySpark data cleaning
Phase 5  🔲  EDA + SQL analysis (RFM, Cohort, Churn)
Phase 6  🔲  Gold layer — Dimensional model (Star Schema)
Phase 7  🔲  Serving layer — Analytical aggregations
Phase 8  🔲  Power BI — Semantic model + 4 dashboards
```

👉 **Main project:** [ecommerce-churn-analysis](https://github.com/Dapinder-BI/ecommerce-churn-analysis)

---

*Built as part of a portfolio project to demonstrate end-to-end data engineering and analytics skills.*
