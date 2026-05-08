# Fintech Behavioral Analytics Platform

A full-stack analytics engineering project that simulates the internal data platform of a fintech company. It covers the entire stack: live API ingestion, cloud data warehousing, a layered dbt transformation model, data quality testing, and daily pipeline orchestration via Apache Airflow.

The project was built to answer a question I kept coming back to while working through the modern data stack: what does it actually look like when all of these pieces connect? Not in a tutorial, where the data is already clean and the pipeline already works, but in practice, where things break in ways you did not anticipate and the debugging teaches you more than the building did.

---

## What This Project Models

The platform simulates the analytics infrastructure of a fintech company tracking customer financial health across two institutions. The business question driving the design was: can behavioral signals in transaction data predict which customers are most at risk before a balance stress event occurs?

The analytical output lives in `mart_risk_signals`. The model surfaces customers who combine two specific risk factors: high merchant concentration (top three merchants accounting for more than 60% of spend) and two or more consecutive months of negative net cash flow. The hypothesis is that these customers have both inflexible spending patterns and deteriorating financial buffers, making them the highest-priority intervention targets for a risk team.

The Plaid Sandbox data used in this project is synthetic and does not produce analytically meaningful findings. What the project demonstrates is that the pipeline is correctly designed: the models compute what they were built to compute, the data flows cleanly from source to mart, and the quality tests catch problems before they reach downstream consumers.

---

## Architecture

The pipeline moves data through five distinct layers, each with a single responsibility.

```
Plaid Sandbox API (2 institutions)
        |
Python Extraction Layer
(cursor-based sync, accounts, balances, retry logic, GCS write)
        |
Google Cloud Storage
(partitioned by institution + date, immutable raw JSON)
        |
BigQuery Raw Tables (Bronze)
(raw_transactions, raw_accounts, raw_balances)
        |
dbt Staging Models (Silver)
(stg_plaid__transactions, stg_plaid__accounts, stg_plaid__balances)
        |
dbt Intermediate Models
(int_transactions_enriched, int_account_monthly_cashflow, int_customer_risk_signals)
        |
dbt Mart Models (Gold)
(mart_customer_financial_health, mart_spending_behavior, mart_risk_signals, mart_operational_monitoring)
        |
Apache Airflow DAG
(daily orchestration with quality gates between layers)
```

Each layer reads only from the layer directly above it. Staging reads from raw. Intermediate reads from staging. Marts read from intermediate. This constraint sounds simple but it matters in practice: when something breaks, you know exactly which layer owns the problem.

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| API Source | Plaid Sandbox API | plaid-python 38.4.0 |
| Extraction | Python | 3.13 |
| Raw Storage | Google Cloud Storage | partitioned by institution + date |
| Data Warehouse | BigQuery | us-central1 |
| Transformations | dbt (BigQuery adapter) | 1.11.9 / adapter 1.11.1 |
| Orchestration | Apache Airflow (LocalExecutor) | 2.9.1 |
| Container Runtime | Docker Compose | v2.33.1 |
| Version Control | GitHub (conventional commits) | - |

---

## The Two-Institution Design

One of the first things I discovered when working with the Plaid Sandbox API is that it returns identical transaction data for every institution when you use the same test credentials. Both institutions had the same merchants, the same amounts, the same dates. There was no contrast, no story, and no risk signal to find.

The two-institution design was the answer to that. Institution A (StableBank) kept the real Plaid Sandbox data: regular salary credits, diversified merchant spend, healthy and stable balances. It serves as the control group. Institution B (VolatileBank) had its transaction data replaced with a synthetic seed designed to reflect a genuinely different customer profile: irregular income timing, high concentration across three dominant merchants, recurring months of negative net cash flow, and a stressed checking balance.

This was a deliberate design choice, documented as DEC-014. The alternative would have been to use two identical datasets and pretend the contrast existed, which would have made the project less honest and less useful as a learning exercise.

The synthetic seed script (`ingestion/seed_institution_b.py`) uses a fixed random seed for reproducibility. It can be re-run safely after any truncation of the raw tables.

---

## Project Structure

```
fintech-behavioral-analytics/
|
|-- ingestion/
|   |-- plaid_client.py          # Plaid API client factory with sandbox validation
|   |-- generate_tokens.py       # Access token generation for both institutions
|   |-- validate_accounts.py     # Account validation and auth confirmation
|   |-- cursor_store.py          # Persistent cursor storage for incremental sync
|   |-- extract_transactions.py  # Full extraction: transactions, accounts, balances
|   |-- load_to_bigquery.py      # GCS to BigQuery loader with idempotent partition writes
|   |-- seed_institution_b.py    # Synthetic data seed for institution_b
|
|-- dbt/fintech_analytics/
|   |-- models/
|   |   |-- staging/             # 3 models: clean, cast, rename from raw
|   |   |-- intermediate/        # 3 models: enrich, aggregate, risk signal derivation
|   |   |-- marts/               # 4 models: financial health, spending, risk, monitoring
|   |-- macros/
|   |   |-- generate_schema_name.sql  # Routes models to correct BigQuery datasets
|   |-- dbt_project.yml
|   |-- profiles.yml             # Local only, gitignored
|
|-- airflow/
|   |-- dags/
|   |   |-- fintech_pipeline_dag.py  # 7-task DAG with quality gates
|   |-- config/
|   |   |-- profiles.yml         # Container-specific dbt profile
|   |-- docker-compose.yaml      # LocalExecutor setup, 4 containers
|   |-- bootstrap.sh             # Post-startup container setup script
|   |-- .env                     # Secrets, gitignored
|
|-- .env                         # Local secrets, gitignored
|-- requirements.txt
```

---

## The dbt Transformation Layer

The transformation layer has ten models across three layers. Every model has a defined grain and a single responsibility.

### Staging

The staging layer is where raw data becomes trustworthy. Everything that could cause a silent downstream failure gets handled here: type casting, null handling, field renaming, filter logic. Intermediate and mart models read from staging and do not need to repeat any of this work.

`stg_plaid__transactions` was the most consequential model to get right. Plaid returns `pending` as NULL for some institutions rather than explicit `false`, which meant a naive `WHERE pending = false` filter silently dropped half the dataset. That bug is documented in detail in the data quality section below.

### Intermediate

The intermediate layer is where the business logic lives. This is where transactions get enriched with derived fields like `transaction_direction` and `merchant_category_normalised`, where monthly cashflow gets aggregated using a spine pattern to preserve zero-transaction months, and where customer risk signals get computed using window functions.

`int_customer_risk_signals` is the most technically complex model in the project. It uses a gaps-and-islands pattern to calculate the maximum consecutive negative cashflow streak per account, a merchant concentration ratio derived from the top three merchants by debit spend, and a combined risk flag that identifies accounts meeting both thresholds simultaneously.

### Marts

The mart layer is designed around four stakeholder perspectives inside the business.

| Model | Stakeholder | What It Answers |
|---|---|---|
| `mart_risk_signals` | Risk team | Which accounts show combined behavioral risk signals |
| `mart_customer_financial_health` | Finance team | Cashflow trends, savings rate, income stability |
| `mart_spending_behavior` | Product team | Merchant concentration, recurring vs discretionary spend, spend spikes |
| `mart_operational_monitoring` | Data team | Pipeline freshness, ingestion completeness, duplicate detection |

### Full model inventory

81 tests passing across all 10 models.

| Model | Layer | Grain | Key Output |
|---|---|---|---|
| `stg_plaid__transactions` | Staging | One row per transaction | Cleaned, cast, pending filtered, `is_debit` added |
| `stg_plaid__accounts` | Staging | One row per account per snapshot | Cleaned, `institution_id` from top-level column |
| `stg_plaid__balances` | Staging | One row per account per snapshot date | SAFE_CAST balances, `snapshot_date` alias |
| `int_transactions_enriched` | Intermediate | One row per transaction | `transaction_direction`, `merchant_category_normalised`, `is_recurring` |
| `int_account_monthly_cashflow` | Intermediate | One row per account per month | Spine pattern ensures zero-transaction months present |
| `int_customer_risk_signals` | Intermediate | One row per account | Cashflow streak, merchant concentration ratio, volatility, `combined_risk_flag` |
| `mart_risk_signals` | Mart | One row per account | Risk tier (HIGH/MEDIUM/LOW), balance snapshot joined |
| `mart_customer_financial_health` | Mart | One row per account per month | Rolling 3-month cashflow, savings rate proxy, cashflow trend |
| `mart_spending_behavior` | Mart | One row per account per month | Top 3 merchants, recurring vs discretionary split, spend spike flag |
| `mart_operational_monitoring` | Mart | One row per institution per ingestion date | Record counts, duplicate detection, freshness status |

---

## Airflow Pipeline

The full pipeline runs daily via an Airflow DAG with a quality gate between the staging and intermediate layers. If staging tests fail, the pipeline stops. Intermediate models do not run on bad data.

```
extract_plaid_data
        |
load_to_bigquery
        |
dbt_run_staging
        |
dbt_test_staging   <-- quality gate: pipeline halts here on test failure
        |
dbt_run_intermediate
        |
dbt_run_marts
        |
dbt_test_marts
```

All 7 tasks confirmed green in a full end-to-end manual run.

Airflow runs via Docker Compose using the LocalExecutor. The development machine has 4GB of total RAM. The CeleryExecutor stack (scheduler, worker, triggerer, webserver, postgres, redis) exceeded that limit and consistently killed tasks under memory pressure. Switching to LocalExecutor dropped the container count from 6 to 4 and eliminated the memory problem entirely. LocalExecutor runs tasks directly in the scheduler process and is a legitimate production pattern for single-machine deployments.

---

## Data Quality

Test coverage is 81 tests across all 10 models, covering uniqueness, not-null constraints, accepted values, and custom business logic checks.

Beyond the tests, three genuine data quality bugs were discovered and resolved during the build. None of them were obvious. All three caused silent data loss that would not have been caught without downstream investigation. They are worth documenting in full because they reflect the kind of problems that come up in real pipelines.

### Bug 1: The load that quietly erased half the data

The BigQuery loader was written with institutions as the outer loop and data types as the inner loop. For each institution, it loaded transactions, then accounts, then balances, using `WRITE_TRUNCATE` on the date partition.

The problem was that `WRITE_TRUNCATE` replaces the entire partition on every write. So when institution_b's transactions were loaded into the same partition that already held institution_a's transactions, institution_a's data was silently deleted. Then when institution_b's accounts were loaded, institution_a's accounts were deleted. And so on.

The result was that institution_a was entirely absent from every downstream model. The marts only showed institution_b. This was not immediately obvious because institution_b's data looked correct on its own. You would only notice the problem if you knew to look for the missing institution.

The fix was to swap the loop order: data_type as the outer loop, institutions as the inner loop. All institution rows for a given data type are now accumulated first, then written in a single `WRITE_TRUNCATE`. Each partition write is now complete and idempotent.

### Bug 2: The filter that silently dropped an institution

The staging filter for pending transactions was `WHERE pending = false`. This looked correct. Plaid marks pending transactions with a boolean flag and you do not want them in your analysis models.

The problem was that Plaid Sandbox returns `pending = NULL` for institution_a rather than explicit `false`. In SQL, `NULL = false` does not evaluate to true. It evaluates to NULL, which means the row fails the filter and is excluded. Every single institution_a transaction was silently filtered out of the staging model.

This bug was only discovered after fixing the load loop bug and noticing that institution_a still did not appear in the downstream models. The dataset had two layers of data loss stacked on top of each other. The fix was `WHERE COALESCE(pending, false) = false`, which treats NULL as non-pending and passes those rows through.

### Bug 3: When BigQuery resolves a name to the wrong thing

`int_customer_risk_signals` failed with an error that took several attempts to understand: "Ordering by expressions of type STRUCT is not allowed."

The model had a CTE named `merchant_spend` and, inside that CTE, a column also named `merchant_spend`. When a downstream CTE referenced `ORDER BY merchant_spend`, BigQuery resolved the name to the CTE itself rather than the column. A CTE is a table-like structure, which BigQuery represents internally as a STRUCT. Ordering by a STRUCT is not allowed.

The error message gave no indication that a name collision was the cause. It just said the expression type was wrong. Several attempts to cast the column to a different type did not help, because the ambiguity was at the name resolution level, not the type level. The fix was to rename the CTE to `merchant_spend_by_name` and the column to `spend_amount`. The model was also materialized as a TABLE rather than a view to ensure BigQuery resolves all types cleanly across chained view references. This pattern is now a documented convention in the project: CTE names and column names must never be identical within the same model.

---

## Key Design Decisions

**Cursor-based sync over date-based extraction.**
Plaid offers two endpoints for transaction data. `/transactions/get` is date-based and deprecated. `/transactions/sync` is cursor-based and the current recommendation. The cursor approach handles added, modified, and removed transactions cleanly without date-range overlap issues. More importantly, the cursor is saved only after a successful GCS write, not before. If the write fails, the cursor does not advance. The next run retries from the last known good position. This guarantee matters because losing transactions silently is exactly the kind of failure that is hard to detect later.

**A cross-join spine for monthly cashflow.**
The first version of `int_account_monthly_cashflow` aggregated transactions by account and month. The problem was that months with zero transactions simply did not appear in the output. An account that went quiet for two or three months would show a gap in the month sequence, and the consecutive negative cashflow streak calculation would treat that gap as a break in the streak rather than continued zero-activity months.

The fix was a spine: a cross join of all distinct accounts with a generated array of every month in the dataset's date range. Every account now has a row for every month, even when the transaction count is zero. This is the correct model of the underlying reality and it is what makes the streak calculation trustworthy.

**Staging as the single point of data quality control.**
Every decision about how to handle raw data lives in the staging layer: type casting, null handling, filter logic, field renaming. Intermediate and mart models inherit clean data and do not repeat any of this work. The practical benefit is that fixing a data quality issue in staging propagates correctly through the entire model graph. You change one model and every dependent model reflects the fix automatically on the next run.

**LocalExecutor over CeleryExecutor.**
This decision was forced by the constraints of the development environment, but it turned out to be the right call regardless. The CeleryExecutor requires a separate worker container and a Redis broker on top of the scheduler, webserver, and triggerer. On a 4GB machine, this pushed Docker into memory pressure and caused tasks to be killed before they could complete. LocalExecutor runs tasks in the scheduler process, cuts the container count in half, and is entirely appropriate for a pipeline that does not need distributed workers. For a single-machine deployment, it is the simpler and more maintainable choice.

---

## How to Run

### Prerequisites

- Python 3.13
- Google Cloud SDK (`gcloud` CLI)
- Docker Desktop (4GB RAM minimum, 6GB recommended)
- A Plaid developer account with Sandbox access
- A GCP project with BigQuery and Cloud Storage enabled

### 1. Clone the repo and set up the Python environment

```bash
git clone https://github.com/ashleyesau/fintech-behavioral-analytics.git
cd fintech-behavioral-analytics
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and fill in your Plaid credentials and GCP project details.

```bash
cp .env.example .env
```

### 3. Run the ingestion scripts

```bash
python ingestion/generate_tokens.py
python ingestion/extract_transactions.py
python ingestion/load_to_bigquery.py
python ingestion/seed_institution_b.py
```

### 4. Run dbt

```bash
dbt run --project-dir dbt/fintech_analytics --profiles-dir ~/.dbt
dbt test --project-dir dbt/fintech_analytics --profiles-dir ~/.dbt
```

### 5. Start Airflow

```bash
cd airflow
docker compose up -d
./bootstrap.sh
```

Open `http://localhost:8080` (login: `airflow` / `airflow`) and trigger the `fintech_pipeline` DAG manually.

`bootstrap.sh` installs the required Python packages into the scheduler container, copies the dbt profile, and creates the GCP key symlink. It needs to be re-run after every `docker compose up` because container filesystems do not persist between restarts.

---

## Known Limitations

**Synthetic Plaid Sandbox data.**
The Plaid Sandbox environment returns deterministic, not realistic, transaction histories. Institution_b's data was replaced entirely with a synthetic seed to create the behavioural contrast required by the project design. Neither institution reflects real customer behaviour.

**Institution_a Sandbox artefact.**
Institution_a shows unexpectedly high risk signals in `mart_risk_signals`: a merchant concentration ratio of 1.0 and a maximum negative cashflow streak of 24 months. This is a known limitation of Plaid Sandbox. Sparse merchant names cause extreme concentration ratios because a small number of descriptors dominate the dataset. Non-transaction accounts like savings accounts, IRAs, and mortgages always show negative net cashflow because debits are recorded but no corresponding credits appear. This is Plaid Sandbox behaviour, not a pipeline bug. The risk signal analysis is scoped to institution_b only.

**Local cursor storage.**
Plaid sync cursors are stored in `state/cursors.json` on the local filesystem. In a production pipeline this would be persisted in GCS or a database so that cursor state survives infrastructure restarts. For portfolio scope, local file storage is sufficient.

**No row-level security.**
The mart layer has no access controls. In a production environment, risk signals and customer financial data would require role-based access controls at the dataset or row level. This is out of scope for a portfolio project.

**No live dashboard.**
A dashboard layer was scoped but ultimately dropped. Looker Studio is GUI-only with no programmatic API for building reports. Streamlit would have required significant build time for marginal additional signal given the depth of the engineering layer already in place. The decision was to ship a complete, well-tested pipeline rather than a shallow pipeline with a polished front end.

---

## GCP Infrastructure

| Resource | Details |
|---|---|
| Project | `plaid-495309` |
| GCS Bucket | `plaid-495309-raw-data` (us-central1) |
| BigQuery Datasets | `raw`, `staging`, `intermediate`, `marts`, `operational` |
| Raw Table Partitioning | By `ingestion_date`, clustered by `account_id` + `institution_id` |
| Service Account | `plaid-pipeline-sa@plaid-495309.iam.gserviceaccount.com` |

---

## Repository

[github.com/ashleyesau/fintech-behavioral-analytics](https://github.com/ashleyesau/fintech-behavioral-analytics)
