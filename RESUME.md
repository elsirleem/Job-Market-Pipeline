# EU Job Market Data Pipeline

**Stack:** Python · PySpark · Delta Lake · Docker · GitHub Actions

A reproducible batch **medallion (bronze/silver/gold)** pipeline that ingests EU
data-engineering job postings from the Adzuna API and models them into clean,
analytics-ready Delta tables.

**Highlights**
- Ingests job postings via REST API into an append-only **bronze** layer, then
  cleans, deduplicates and skill-tags them in **silver** with PySpark/SQL.
- Builds **gold** analytics tables (skill demand by country, top tools for junior
  roles, Azure/AWS/GCP demand, top hiring companies).
- **Blocking data-quality gates** (null/duplicate-key, date-parse and coverage
  checks) stop bad data before it reaches gold.
- Fully **Dockerised** Spark + Delta runtime, **15-test** suite, GitHub Actions CI;
  structured to port to Databricks unchanged.

**Resume bullet**
> Built a PySpark + Delta Lake medallion pipeline ingesting EU data-engineering job
> postings via API into bronze/silver/gold tables, with skill extraction, blocking
> data-quality gates, automated tests and CI in a reproducible Docker environment.

🔗 GitHub: _add link_
