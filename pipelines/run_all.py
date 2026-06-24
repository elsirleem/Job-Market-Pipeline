"""End-to-end run: bronze ingest -> silver (gated) -> gold, on one Spark session.

This is the entrypoint an orchestrator (Airflow / Databricks Workflows) would call.
Each stage is also runnable on its own via run_bronze/run_silver/run_gold.
"""
from __future__ import annotations

import logging

from config.settings import settings
from jobpipe.common.spark import get_spark
from jobpipe.ingest.adzuna import fetch_jobs, write_bronze
from jobpipe.quality.checks import assert_quality, run_checks
from jobpipe.transform.gold import build_gold
from jobpipe.transform.silver import build_silver, write_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline")


def main() -> None:
    spark = get_spark("eu-job-market-pipeline")

    # --- Bronze ---
    records = fetch_jobs(settings)
    log.info("Bronze: appending %d fetched records", len(records))
    write_bronze(spark, records)

    # --- Silver (with quality gate) ---
    silver = build_silver(spark)
    results = run_checks(silver)
    for r in results:
        marker = "OK " if r.passed else ("FAIL" if r.severity == "error" else "WARN")
        log.info("[%s] %-22s %s", marker, r.name, r.detail)
    assert_quality(results)
    written = write_silver(silver)
    log.info("Silver: wrote %d clean rows", written)

    # --- Gold ---
    counts = build_gold(spark)
    for table, rows in counts.items():
        log.info("Gold: %-20s %d rows", table, rows)

    spark.stop()
    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
