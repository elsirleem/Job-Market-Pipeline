"""Silver -> gold: small, purpose-built analytics tables.

Each table answers one question from the project brief and is written to its own
Delta path so a BI tool / Databricks SQL dashboard can read them independently.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobpipe.common import paths


def _write(df: DataFrame, path: str) -> int:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return df.count()


def build_gold(spark: SparkSession) -> dict[str, int]:
    silver = spark.read.format("delta").load(paths.SILVER).cache()
    exploded = silver.withColumn("skill", F.explode_outer("skills"))
    counts: dict[str, int] = {}

    # 1. Top skills by country.
    skills_by_country = (
        exploded.where(F.col("skill").isNotNull())
        .groupBy("country_code", "skill")
        .agg(F.count("*").alias("mentions"))
        .orderBy("country_code", F.desc("mentions"))
    )
    counts["skills_by_country"] = _write(skills_by_country, paths.GOLD_SKILLS_BY_COUNTRY)

    # 2. Top tools for junior roles specifically.
    top_tools = (
        exploded.where((F.col("skill").isNotNull()) & (F.col("seniority") == "junior"))
        .groupBy("skill")
        .agg(F.count("*").alias("junior_mentions"))
        .orderBy(F.desc("junior_mentions"))
    )
    counts["top_tools"] = _write(top_tools, paths.GOLD_TOP_TOOLS)

    # 3. Cloud demand: Azure vs AWS vs GCP by country.
    cloud_demand = (
        silver.withColumn("cloud", F.explode_outer("clouds"))
        .where(F.col("cloud").isNotNull())
        .groupBy("country_code", "cloud")
        .agg(F.count("*").alias("mentions"))
        .orderBy("country_code", F.desc("mentions"))
    )
    counts["cloud_demand"] = _write(cloud_demand, paths.GOLD_CLOUD_DEMAND)

    # 4. Posting volume by day.
    jobs_by_day = (
        silver.where(F.col("posted_date").isNotNull())
        .groupBy("posted_date")
        .agg(F.count("*").alias("jobs_posted"))
        .orderBy("posted_date")
    )
    counts["jobs_by_day"] = _write(jobs_by_day, paths.GOLD_JOBS_BY_DAY)

    # 5. Most active hiring companies.
    top_companies = (
        silver.where(F.col("company").isNotNull())
        .groupBy("company")
        .agg(F.count("*").alias("openings"))
        .orderBy(F.desc("openings"))
    )
    counts["top_companies"] = _write(top_companies, paths.GOLD_TOP_COMPANIES)

    silver.unpersist()
    return counts
