"""Build the silver table and enforce data-quality gates before publishing it."""
from __future__ import annotations

import logging

from jobpipe.common.spark import get_spark
from jobpipe.quality.checks import assert_quality, run_checks
from jobpipe.transform.silver import build_silver, write_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("silver")


def main() -> None:
    spark = get_spark("silver-transform")

    silver = build_silver(spark)

    # Gate on quality BEFORE writing, so a bad batch never reaches gold.
    results = run_checks(silver)
    for r in results:
        marker = "OK " if r.passed else ("FAIL" if r.severity == "error" else "WARN")
        log.info("[%s] %-22s %s", marker, r.name, r.detail)
    assert_quality(results)

    written = write_silver(silver)
    log.info("Wrote %d rows to silver", written)
    spark.stop()


if __name__ == "__main__":
    main()
