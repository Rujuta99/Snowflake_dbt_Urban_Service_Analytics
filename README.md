# Chicago 311 Service Reliability Pipeline

An end-to-end data engineering pipeline that ingests Chicago 311 service requests,
cleans them at scale with PySpark, models them into a star schema in Snowflake with
dbt, validates data quality with Great Expectations, orchestrates everything with
Apache Airflow, and surfaces a neighborhood **service-equity** analysis in a dashboard.

**Core question:** do wealthier Chicago neighborhoods get their 311 requests resolved
faster than lower-income ones, and does weather widen the gap?

## Architecture

```
Data sources        Chicago 311 (Socrata API)
      |
Bronze layer        raw JSON landed untouched
      |             (PySpark: clean, dedupe, derive resolution_hours)
Silver layer        cleaned & conformed Parquet
      |             (load to Snowflake -> dbt models)
Gold layer          star schema: fact_service_requests + dim_date / _neighborhood / _service_type
      |             (Great Expectations validates before publish)
Dashboard           neighborhood equity analysis
```

Apache Airflow orchestrates every stage end to end.

## Tech stack

| Layer          | Tool                          |
|----------------|-------------------------------|
| Ingestion      | Python + Socrata SODA API     |
| Transformation | PySpark                       |
| Warehouse      | Snowflake                     |
| Modeling       | dbt                           |
| Data quality   | Great Expectations            |
| Orchestration  | Apache Airflow                |
| Dashboard      | Power BI / Streamlit          |

## Build status

- [x] Phase 1 -- Ingestion
- [x] **Phase 2 -- Silver layer (PySpark)** (this milestone)
- [ ] Phase 3 -- Load to Snowflake
- [ ] Phase 4 -- Gold layer (dbt)
- [ ] Phase 5 -- Data quality (Great Expectations)
- [ ] Phase 6 -- Orchestration (Airflow)
- [ ] Phase 7 -- Dashboard + analysis

## Setup

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. (Optional) copy the env template; not required to start
cp .env.example .env
```

## Phase 1 -- Ingestion

Pull 311 records into the bronze layer (`data/bronze/sr_311/`).

```bash
# Backfill a single month (run month by month for the full history)
python -m src.ingest.ingest_311 --start 2024-01-01 --end 2024-02-01

# Incremental: pull the last 3 days (what Airflow will run daily)
python -m src.ingest.ingest_311 --incremental 3
```

Each run writes one newline-delimited JSON file. Bronze keeps the raw API
response untouched -- all cleaning happens later in the silver layer.

### Run the tests

```bash
python -m pytest tests/ -v
```

The ingestion tests mock the API, so they run offline and verify pagination,
the stop condition, and file writing without downloading anything.

## Phase 2 -- Silver layer (PySpark)

Clean the raw bronze data into a conformed, deduplicated Parquet dataset.

**Prerequisite:** PySpark needs Java (JDK 11 or 17). Check with `java -version`.
On macOS, if it's missing: `brew install openjdk@17`.

```bash
python -m src.transform.clean_silver \
    --input  data/bronze/sr_311 \
    --output data/silver/sr_311
```

What the job does:
- parses ISO timestamps and casts numeric fields to real types
- deduplicates on `sr_number`, keeping the most recently modified version
- derives `resolution_hours`, nulling and flagging invalid (negative) values
- flags rows with missing or out-of-Chicago geo coordinates
- writes Parquet partitioned by `created_year` / `created_month`

The silver tests run the real transform against a sample file covering each
edge case (duplicate IDs, negative resolution time, blank fields, missing geo).
