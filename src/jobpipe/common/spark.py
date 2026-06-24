"""Spark session factory wired for Delta Lake.

Using delta-spark's `configure_spark_with_delta_pip` keeps the Delta jars in sync
with the Python package version, so there is no manual --packages juggling.
"""
from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "eu-job-market-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        # Local single-node; deterministic small-shuffle partitions for a laptop.
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
