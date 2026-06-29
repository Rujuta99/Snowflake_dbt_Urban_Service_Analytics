# Urban Service Analytics: Chicago 311

An end-to-end data engineering pipeline that ingests Chicago 311 service
requests, cleans them at scale with PySpark, validates them with Great
Expectations, loads them into Snowflake, and models them into a star schema
with dbt to analyze service equity across neighborhoods. Apache Airflow
orchestrates the pipeline locally.

Core question: do lower-income Chicago neighborhoods wait longer for their
311 service requests to be resolved than higher-income ones?

## Headline finding

Across 157K requests from January 2024, the gap depends on the service. For
weather and safety related requests, the lowest-income quartile of community
areas waited substantially longer than the highest-income quartile (median
resolution time):

| Service type       | Low-income median | High-income median | Gap   |
|--------------------|-------------------|--------------------|-------|
| Water on Street    | 100.9 hrs         | 55.2 hrs           | +83%  |
| Ice & Snow Removal | 3.8 hrs           | 2.2 hrs            | +73%  |
| Traffic Signal Out | 8.2 hrs           | 5.0 hrs            | +64%  |
| Pothole in Street  | 71.8 hrs          | 53.8 hrs           | +34%  |

A blended average across all service types showed no clean income gradient; the
disparity only appears when comparing like-for-like service types. Caveats:
single month of data (seasonal effects on winter services), and the income
indicators are from the 2008-2012 census release.

## Architecture (medallion)
git add -A
git status
Apache Airflow orchestrates these steps end to end:
ingest -> clean_silver -> validate -> load -> dbt build.

## Tech stack

| Layer          | Tool                          |
|----------------|-------------------------------|
| Ingestion      | Python + Socrata SODA API     |
| Transformation | PySpark                       |
| Data quality   | Great Expectations            |
| Warehouse      | Snowflake (key-pair auth)     |
| Modeling       | dbt                           |
| Orchestration  | Apache Airflow (Docker)       |

## Build status

- [x] Phase 1: Ingestion (Socrata API to bronze)
- [x] Phase 2: Silver layer (PySpark cleaning)
- [x] Phase 3: Load to Snowflake (key-pair auth)
- [x] Phase 4: Gold layer (dbt star schema, tests, equity analysis)
- [x] Phase 5: Data quality (Great Expectations gate)
- [x] Phase 6: Orchestration (Airflow, local via SSHOperator)
- [ ] Phase 7: Dashboard and write-up

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                 # then fill in Snowflake credentials
```

Snowflake uses key-pair authentication (it enforces MFA on password logins):
generate a key, register the public key on your user, and point
SNOWFLAKE_PRIVATE_KEY_PATH at the private key. See .env.example.

Great Expectations has dependencies that conflict with dbt, so it lives in its
own environment:

```bash
python -m venv .venv-gx
.venv-gx/bin/pip install great-expectations pandas pyarrow
```

## Running the pipeline (manually)

```bash
# 1. Ingest one month into the bronze layer
python -m src.ingest.ingest_311 --start 2024-01-01 --end 2024-02-01

# 2. Clean into the silver layer (needs Java 11/17 for PySpark)
python -m src.transform.clean_silver --input data/bronze/sr_311 --output data/silver/sr_311

# 3. Validate the silver layer (quality gate)
.venv-gx/bin/python -m src.quality.validate_silver --input data/silver/sr_311

# 4. Run sql/setup_snowflake.sql once in Snowsight, then load the silver data
python -m src.load.load_snowflake --input data/silver/sr_311

# 5. Build and test the dbt models
set -a; source .env; set +a
dbt build --project-dir dbt --profiles-dir dbt
```

## Running the pipeline (orchestrated)

Airflow runs the same steps in order. See airflow/README.md. The DAG triggers
the steps on the host over SSH inside the project's virtual environments. This
is a local-orchestration setup; in production these steps would run as
containerized tasks (ECS / Kubernetes) or on managed Airflow.

## Tests

```bash
python -m pytest tests/ -v
```

## Project layout
