# Architecture

## Overview

A medallion-architecture batch pipeline that ingests EU data-engineering job
postings from the **Adzuna API**, refines them through **bronze → silver → gold**
Delta Lake tables, enforces **data-quality gates**, and exposes analytics-ready
tables for a dashboard.

```
                 ┌──────────────┐
   Adzuna API ──▶│   INGEST     │  HTTP client (pure Python, mockable)
                 └──────┬───────┘
                        ▼
                 ┌──────────────┐
                 │   BRONZE     │  append-only raw rows + ingestion metadata
                 │ (Delta)      │  full audit trail, never deduped
                 └──────┬───────┘
                        ▼
                 ┌──────────────┐   ┌────────────────────┐
                 │   SILVER     │──▶│  DATA QUALITY GATE  │  error checks block,
                 │ (Delta)      │   │  (block on error)   │  warn checks log
                 │ dedupe,      │   └────────────────────┘
                 │ normalize,   │
                 │ skills,      │
                 │ seniority    │
                 └──────┬───────┘
                        ▼
                 ┌──────────────┐
                 │    GOLD      │  skills_by_country · top_tools · cloud_demand
                 │ (Delta x5)   │  jobs_by_day · top_companies
                 └──────┬───────┘
                        ▼
              Power BI / Databricks SQL / FastAPI
```

## Layer responsibilities

| Layer  | Write mode | Owns |
|--------|-----------|------|
| Bronze | append | Raw source fidelity + `ingested_at`/`country_code` lineage. Never modified. |
| Silver | overwrite (full rebuild from bronze) | One clean, deduplicated row per posting; normalization; skill & seniority enrichment. |
| Gold   | overwrite | Small aggregate tables, one per business question. |

## Key design decisions (and trade-offs)

- **Batch, not streaming.** Job postings change on the order of hours/days, so a
  scheduled batch run is the right tool. Streaming would add Kafka/Flink
  complexity with no freshness benefit here. (Streaming is demonstrated in a
  separate portfolio project where it *is* warranted.)
- **Bronze is append-only; silver dedupes.** This keeps a full audit trail of what
  the API returned each run and makes silver a deterministic, idempotent rebuild.
- **Skill extraction is a curated taxonomy, not ML.** Transparent, deterministic,
  fast, and easy to justify in an interview. A model would add opacity for little
  gain at this scale.
- **Quality gates block on error severity.** A bad batch (empty table, duplicate
  keys, mostly-unparseable dates) raises and never reaches gold; soft issues
  (missing company, low skill coverage) warn but pass.
- **Spark runs in Docker.** Pinned Python 3.11 + Java 17 + Spark 3.5 / Delta 3.2
  makes the project reproducible on any machine and portable to Databricks.

## Portability to Databricks

The transforms are plain PySpark + Delta, so moving to Databricks means: point the
paths at a Unity Catalog volume / DBFS, drop the local `SparkSession` builder (the
cluster provides one), and wrap `run_bronze/silver/gold` as tasks in a Databricks
Workflow. No transform logic changes.
