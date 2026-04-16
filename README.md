# dbt + Snowflake ELT Pipeline

> **Production-grade ELT pipeline** built with dbt Core, modelling e-commerce
> order data across Raw → Staging → Marts layers. Runs locally in **60 seconds**
> via Docker + DuckDB — no Snowflake account required.

[![dbt CI Pipeline](https://github.com/YOUR_USERNAME/dbt-snowflake-elt-pipeline/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/dbt-snowflake-elt-pipeline/actions)
[![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt)](https://docs.getdbt.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-ready-29B5E8?logo=snowflake)](https://www.snowflake.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-local-FCD34D)](https://duckdb.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Table of Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [dbt Models](#dbt-models)
- [Tests](#tests)
- [Quick Start — Local (Docker)](#quick-start--local-docker)
- [Quick Start — Native Python](#quick-start--native-python)
- [Connecting to Snowflake](#connecting-to-snowflake)
- [dbt Docs](#dbt-docs)
- [CI/CD](#cicd)
- [Push to GitHub](#push-to-github)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ELT Pipeline Overview                            │
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │  SOURCE DATA  │    │    LOAD      │    │      TRANSFORM (dbt)     │  │
│  │              │    │              │    │                          │  │
│  │  CSV Seeds   │───▶│  DuckDB /    │───▶│  RAW → STAGING → MARTS  │  │
│  │  (local dev) │    │  Snowflake   │    │                          │  │
│  │              │    │  (prod)      │    │                          │  │
│  └──────────────┘    └──────────────┘    └──────────────────────────┘  │
│                                                    │                    │
│                                          ┌─────────▼──────────┐        │
│                                          │   BI / Analytics   │        │
│                                          │  (Metabase, Looker, │        │
│                                          │   Tableau, etc.)   │        │
│                                          └────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                        dbt Layer Detail                                 │
│                                                                         │
│   RAW LAYER (views)          STAGING LAYER (views)                      │
│   ─────────────────          ───────────────────────────────────────    │
│   raw_customers  ──────────▶ stg_customers                              │
│   raw_products   ──────────▶ stg_products                               │
│   raw_orders     ──────────▶ stg_orders                                 │
│   raw_order_items ─────────▶ stg_order_items                            │
│                                                                         │
│   MARTS LAYER (tables)                                                  │
│   ─────────────────────────────────────────────────────────────────     │
│   stg_customers ─────────────────────────────────────▶ dim_customers   │
│   stg_orders ──────────────────────────────────────┐                   │
│   stg_order_items ──────────────────────────────┐  ├─▶ fct_orders      │
│   stg_customers ────────────────────────────────┘  │                   │
│                                                     │                   │
│   stg_products + stg_order_items + stg_orders ──────┴─▶ mart_product_  │
│                                                            performance  │
│   fct_orders ──────────────────────────────────────────▶ mart_monthly_ │
│                                                            revenue      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Transformation | dbt Core 1.8 | SQL models, tests, docs |
| Warehouse (prod) | Snowflake | Scalable cloud DW |
| Warehouse (local) | DuckDB | Zero-config local dev |
| Containerisation | Docker + Compose | Reproducible runs |
| CI/CD | GitHub Actions | Automated test on every push |
| Linting | SQLFluff | SQL style enforcement |

---

## Project Structure

```
dbt-snowflake-elt-pipeline/
├── .github/
│   └── workflows/
│       └── dbt_ci.yml              ← GitHub Actions CI pipeline
├── docker/
│   └── Dockerfile                  ← dbt + DuckDB image
├── docker-compose.yml              ← run / docs / debug services
├── dbt_project/
│   ├── dbt_project.yml             ← project config, materialisation strategy
│   ├── packages.yml                ← dbt_utils, audit_helper
│   ├── profiles.yml                ← DuckDB (dev) + Snowflake (prod) targets
│   ├── seeds/
│   │   ├── raw_customers.csv       ← 20 customer records
│   │   ├── raw_products.csv        ← 15 product SKUs
│   │   ├── raw_orders.csv          ← 30 order headers
│   │   └── raw_order_items.csv     ← 57 line items
│   ├── models/
│   │   ├── raw/
│   │   │   └── sources.yml         ← source declarations + column tests
│   │   ├── staging/
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   └── schema.yml          ← staging docs + tests
│   │   └── marts/
│   │       ├── fct_orders.sql
│   │       ├── dim_customers.sql
│   │       ├── mart_product_performance.sql
│   │       ├── mart_monthly_revenue.sql
│   │       └── schema.yml          ← marts docs + tests
│   ├── tests/
│   │   ├── test_no_negative_revenue_on_completed_orders.sql
│   │   ├── test_line_total_integrity.sql
│   │   └── test_ytd_revenue_monotonically_increasing.sql
│   └── macros/
│       ├── cents_to_dollars.sql
│       └── safe_divide.sql
├── .gitignore
├── .env.example
├── .sqlfluff
└── README.md
```

---

## Data Model

The pipeline models a simplified e-commerce domain with four source entities:

```
raw_customers (1)──────────(many) raw_orders (1)──────────(many) raw_order_items
                                                                        │
raw_products (1) ──────────────────────────────────────────────────────┘
```

| Seed File | Rows | Description |
|---|---|---|
| `raw_customers.csv` | 20 | Customer master with PII fields |
| `raw_products.csv` | 15 | Product catalog with pricing + COGS |
| `raw_orders.csv` | 30 | Order headers with status + shipping |
| `raw_order_items.csv` | 57 | Line items linking orders to products |

---

## dbt Models

### Staging Layer — Views (1:1 with source tables)

| Model | Source | Key Transformations |
|---|---|---|
| `stg_customers` | `raw_customers` | Type casts, `full_name` derivation, boolean normalisation |
| `stg_products` | `raw_products` | `gross_margin_pct` derivation, `initcap` on categories |
| `stg_orders` | `raw_orders` | Boolean status flags, date-part extraction, `has_discount` flag |
| `stg_order_items` | `raw_order_items` | `calculated_line_total` DQ check, `line_total_matches` flag |

### Marts Layer — Tables (business-ready)

| Model | Grain | Description |
|---|---|---|
| `fct_orders` | 1 row / order | Central fact table joining orders + items + customers. Produces `gross_revenue`, `net_revenue`, `item_count`. |
| `dim_customers` | 1 row / customer | Customer dimension with lifetime metrics and RFM-lite `customer_segment` (VIP / High Value / Repeat Buyer / New Customer). |
| `mart_product_performance` | 1 row / product | Product sales analytics: units sold, gross profit, `realised_margin_pct`, `revenue_rank`. |
| `mart_monthly_revenue` | 1 row / month | Executive trend mart: MoM revenue change %, YTD cumulative revenue. |

---

## Tests

### Schema Tests (generic)

| Test | Count | Models Covered |
|---|---|---|
| `not_null` | 30+ | All key columns across all layers |
| `unique` | 12 | All primary keys + natural keys |
| `accepted_values` | 6 | `status`, `category`, `country`, `shipping_method`, `customer_segment` |
| `relationships` | 5 | All FK → PK relationships enforced across layers |

### Singular Tests (custom SQL)

| Test File | What It Catches |
|---|---|
| `test_no_negative_revenue_on_completed_orders` | Net revenue < 0 on completed orders — data pipeline bug |
| `test_line_total_integrity` | `line_total` mismatches `quantity × unit_price` in source — upstream ETL drift |
| `test_ytd_revenue_monotonically_increasing` | YTD revenue decreasing within a year — broken window frame or duplicate months |

Run all tests: `dbt test`
Run a single test: `dbt test --select test_line_total_integrity`

---

## Quick Start — Local (Docker)

**Requirements:** Docker Desktop

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/dbt-snowflake-elt-pipeline.git
cd dbt-snowflake-elt-pipeline

# 2. Run the full pipeline (seed → run → test)
docker-compose run --rm dbt-run

# 3. (Optional) View dbt docs at http://localhost:8080
docker-compose up dbt-docs
```

Expected output:
```
📦 Installing dbt packages...
🌱 Seeding raw data...       4 seeds loaded.
🏗️  Running models...        8 models completed.
🧪 Running tests...          41 tests passed.
✅ Pipeline complete!
```

---

## Quick Start — Native Python

```bash
# 1. Clone and create virtual environment
git clone https://github.com/YOUR_USERNAME/dbt-snowflake-elt-pipeline.git
cd dbt-snowflake-elt-pipeline

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 2. Install dbt with DuckDB adapter
pip install dbt-core==1.8.3 dbt-duckdb==1.8.1

# 3. Navigate to project and install packages
cd dbt_project
export DBT_PROFILES_DIR=.        # Windows: set DBT_PROFILES_DIR=.

dbt deps

# 4. Run the full pipeline
dbt seed
dbt run
dbt test

# 5. (Optional) Serve docs
dbt docs generate
dbt docs serve --port 8080
```

---

## Connecting to Snowflake

```bash
# 1. Install the Snowflake adapter
pip install dbt-snowflake==1.8.3

# 2. Copy and fill in credentials
cp .env.example .env
# Edit .env with your Snowflake account details

# 3. Export env vars (or use a secrets manager)
export $(cat .env | xargs)

# 4. Run against Snowflake
cd dbt_project
dbt seed   --target prod
dbt run    --target prod
dbt test   --target prod
```

**Minimum Snowflake privileges required:**

```sql
-- Run as ACCOUNTADMIN or SYSADMIN
GRANT USAGE   ON WAREHOUSE COMPUTE_WH  TO ROLE TRANSFORMER;
GRANT USAGE   ON DATABASE  ANALYTICS   TO ROLE TRANSFORMER;
GRANT CREATE SCHEMA ON DATABASE ANALYTICS TO ROLE TRANSFORMER;
```

---

## dbt Docs

dbt auto-generates a data catalog with lineage graphs, column descriptions, and test results.

```bash
# Docker
docker-compose up dbt-docs
# Open http://localhost:8080

# Native
cd dbt_project && dbt docs generate && dbt docs serve
```

The catalog covers all 8 models, 4 sources, 30+ column descriptions, and the full DAG lineage.

---

## CI/CD

Every push to `main` or `develop` triggers the GitHub Actions workflow (`.github/workflows/dbt_ci.yml`):

```
Push → dbt deps → dbt seed → dbt run → dbt test → dbt docs generate → SQLFluff lint
```

The generated `target/` artifact (including `catalog.json` and `manifest.json`) is uploaded as a workflow artifact for 7 days, enabling doc hosting or downstream pipeline triggers.

---

## Push to GitHub

```bash
# 1. Initialise git
cd dbt-snowflake-elt-pipeline
git init
git add .
git commit -m "feat: initial dbt + Snowflake ELT pipeline

- Raw → Staging → Marts model structure
- 4 staging models + 4 mart models (8 total)
- 41 tests: not_null, unique, accepted_values, relationships, 3 custom SQL
- DuckDB local dev + Snowflake prod targets
- Docker Compose for zero-config local runs
- GitHub Actions CI on push"

# 2. Create the GitHub repo (requires GitHub CLI)
gh repo create dbt-snowflake-elt-pipeline \
  --public \
  --description "Production-grade dbt ELT pipeline for e-commerce analytics. Raw → Staging → Marts on DuckDB (local) and Snowflake (prod)." \
  --push \
  --source .

# 3. (Without GitHub CLI) Add remote manually
git remote add origin https://github.com/YOUR_USERNAME/dbt-snowflake-elt-pipeline.git
git branch -M main
git push -u origin main
```

### Recommended GitHub Topics

Add these in **Settings → Topics** on your repo page:

```
dbt  dbt-core  snowflake  duckdb  data-engineering  elt  analytics-engineering
data-warehouse  sql  python  docker  github-actions  data-modeling  olap
ecommerce  dimensional-modeling  etl-pipeline  portfolio
```

---

## Key Design Decisions

**Why DuckDB for local dev?**
DuckDB is an embedded OLAP database that runs entirely in-process — no server, no credentials, no cost. The `dbt-duckdb` adapter is a drop-in replacement for `dbt-snowflake`, so every model, test, and macro works identically. Switching to Snowflake for production is a single flag: `--target prod`.

**Why views for staging and tables for marts?**
Staging models are cheap to recompute and benefit from always reflecting source data freshly. Mart models are queried by BI tools and aggregated frequently, so materialising them as tables eliminates repeated computation and improves query performance.

**Why custom singular tests?**
Generic schema tests (`not_null`, `unique`) catch structural problems but miss business-logic violations. The three custom tests enforce domain invariants that can't be expressed generically — negative revenue, DQ drift between source fields, and monotonicity of cumulative metrics.

---

## License

MIT © 2024. See [LICENSE](LICENSE) for details.
