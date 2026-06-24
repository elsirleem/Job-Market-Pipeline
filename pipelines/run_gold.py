"""Build the gold analytics tables from silver."""
from __future__ import annotations

import logging

from jobpipe.common.spark import get_spark
from jobpipe.transform.gold import build_gold

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold")


def main() -> None:
    spark = get_spark("gold-build")
    counts = build_gold(spark)
    for table, rows in counts.items():
        log.info("gold.%-20s %d rows", table, rows)
    spark.stop()


if __name__ == "__main__":
    main()
