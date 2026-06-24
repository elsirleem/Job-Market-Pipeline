"""Ingest jobs from Adzuna into the bronze Delta table."""
from __future__ import annotations

import logging

from config.settings import settings
from jobpipe.common.spark import get_spark
from jobpipe.ingest.adzuna import fetch_jobs, write_bronze

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze")


def main() -> None:
    log.info("Fetching jobs for countries=%s query=%r", settings.countries, settings.query)
    records = fetch_jobs(settings)
    log.info("Fetched %d records from Adzuna", len(records))

    spark = get_spark("bronze-ingest")
    written = write_bronze(spark, records)
    log.info("Appended %d records to bronze", written)
    spark.stop()


if __name__ == "__main__":
    main()
