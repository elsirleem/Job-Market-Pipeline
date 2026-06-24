"""Bronze -> silver: one clean, deduplicated row per job posting.

Silver is a full rebuild from the append-only bronze history, so it is naturally
idempotent: re-running produces the same table regardless of how many times
bronze was appended to.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from jobpipe.common.paths import BRONZE, SILVER
from jobpipe.skills.extract import CLOUD_SKILLS, extract_skills

# Registered once; calls the pure Python extractor row-wise.
_skills_udf = F.udf(extract_skills, ArrayType(StringType()))


def _classify_seniority(title_col):
    t = F.lower(title_col)
    return (
        F.when(t.rlike(r"\b(junior|graduate|entry|trainee|intern)\b"), "junior")
        .when(t.rlike(r"\b(senior|lead|principal|staff|head)\b"), "senior")
        .when(t.rlike(r"\b(medior|mid[- ]?level)\b"), "mid")
        .otherwise("unspecified")
    )


def build_silver(spark: SparkSession) -> DataFrame:
    """Read bronze and produce the silver DataFrame (I/O wrapper)."""
    bronze = spark.read.format("delta").load(BRONZE)
    return transform_silver(bronze)


def transform_silver(bronze: DataFrame) -> DataFrame:
    """Pure bronze->silver transform. Takes a DataFrame so it is unit-testable."""
    # Keep the most recently ingested copy of each posting (by source_id).
    newest = Window.partitionBy("source_id").orderBy(F.col("ingested_at").desc())
    deduped = (
        bronze.where(F.col("source_id").isNotNull())
        .withColumn("_rn", F.row_number().over(newest))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    skill_text = F.concat_ws(" ", F.col("title"), F.col("description"))
    silver = (
        deduped.withColumn("title", F.trim(F.col("title")))
        .withColumn("company", F.trim(F.col("company")))
        .withColumn("country_code", F.lower(F.trim(F.col("country_code"))))
        .withColumn("posted_date", F.to_date(F.col("created")))
        .withColumn("seniority", _classify_seniority(F.col("title")))
        .withColumn("skills", _skills_udf(skill_text))
        .withColumn(
            "clouds",
            F.array_intersect(F.col("skills"), F.array(*[F.lit(c) for c in sorted(CLOUD_SKILLS)])),
        )
        .withColumn("skill_count", F.size(F.col("skills")))
    )

    return silver.select(
        "source_id", "title", "company", "country_code", "city", "region",
        "location_display", "category", "contract_time", "seniority",
        "posted_date", "skills", "clouds", "skill_count",
        "salary_min", "salary_max", "redirect_url", "ingested_at",
    )


def write_silver(df: DataFrame) -> int:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER)
    return df.count()
