# EU Job Market Data Pipeline

A medallion-architecture data engineering pipeline that ingests **EU data
engineering job postings** from the Adzuna API and refines them through
**bronze → silver → gold** Delta Lake tables, with data-quality gates, unit tests
and CI. Built with PySpark + Delta Lake, fully reproducible via Docker.

> **Why this project?** It demonstrates a complete, production-shaped batch
> workflow — ingestion, lakehouse modelling, data quality, testing and
> orchestration-ready entrypoints — on a dataset that is genuinely useful: it
> shows which tools and clouds are most in demand for junior roles across Europe.

## Stack

| Concern        | Tool |
|----------------|------|
| Processing     | PySpark 3.5 |
| Storage format | Delta Lake 3.2 (medallion: bronze/silver/gold) |
| Ingestion      | Adzuna REST API (`requests`) |
| Data quality   | Custom gate framework (error checks block the pipeline) |
| Testing        | pytest (pure + Spark tests) |
| Reproducibility| Docker + docker compose |
| CI             | GitHub Actions |

See [`docs/architecture.md`](docs/architecture.md) for the diagram and design
trade-offs.

## What the pipeline produces

Five gold tables, each answering one question:

- `skills_by_country` — top skills demanded per country
- `top_tools` — most requested tools in **junior** postings
- `cloud_demand` — Azure vs AWS vs GCP demand by country
- `jobs_by_day` — posting volume over time
- `top_companies` — most active hirers

## Quick start

### 1. Get free Adzuna credentials
Register at <https://developer.adzuna.com/> and copy your `app_id` / `app_key`.

```bash
cp .env.example .env       # then paste your credentials into .env
```

### 2. Build and start the container
```bash
make build
make up
```

Or run it directly with docker compose. This now starts the container, runs the
full pipeline, and keeps the service alive afterward for optional `exec` calls:
```bash
docker compose build
docker compose up -d
```

The Streamlit UI will be available at <http://localhost:8501>.

### 3. Run the pipeline
```bash
make all        # bronze ingest -> silver (quality-gated) -> gold
# or run stages individually:
make bronze
make silver
make gold
```

If the container is already up, you can also rerun the full project manually:
```bash
docker compose exec -T pipeline python pipelines/run_all.py
```

To start just the UI service:
```bash
docker compose up -d ui
```

### 4. Run the tests
```bash
make test
```

## Project layout

```
src/jobpipe/
  common/      Spark session factory + medallion paths
  ingest/      Adzuna API client + bronze writer
  transform/   silver (clean/dedupe/enrich) + gold (aggregates)
  skills/      keyword-taxonomy skill extractor
  quality/     data-quality gate framework
pipelines/     runnable entrypoints (run_bronze/silver/gold/all)
notebooks/     interactive viewer for browsing job postings and gold tables
tests/         pure + Spark unit tests
config/        environment-driven settings
docs/          architecture & design notes
```

## Data quality

Before silver is published, the batch is checked. **Error**-severity failures
(empty table, null/duplicate primary keys, mostly-unparseable dates) raise and
stop the run so bad data never reaches gold. **Warn**-severity issues (missing
company, low skill coverage) are logged but allowed through. See
`src/jobpipe/quality/checks.py`.

## Notes & honesty

- Ingestion uses the **Adzuna API** (free tier), not scraping — stable and within
  terms of service.
- Skill extraction is a transparent keyword taxonomy
  (`src/jobpipe/skills/extract.py`), easily extended.
- The transforms are plain PySpark + Delta and **port to Databricks** with only
  path and session changes — see the architecture doc.

## Roadmap

- [ ] Orchestrate with Databricks Workflows / Airflow on a schedule
- [ ] Add a Databricks SQL dashboard / Power BI report over the gold tables
- [ ] Track skill-demand trends over time (snapshot gold daily)
- [ ] Incremental silver via Delta `MERGE` instead of full rebuild
