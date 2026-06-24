"""Spark tests for the data-quality gates."""
from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql.types import (
    ArrayType,
    DateType,
    StringType,
    StructField,
    StructType,
)

from jobpipe.quality.checks import (
    DataQualityError,
    assert_quality,
    run_checks,
)

# Explicit schema: rows can be all-null in a column (e.g. dates), which Spark
# cannot type by inference.
SCHEMA = StructType([
    StructField("source_id", StringType()),
    StructField("company", StringType()),
    StructField("posted_date", DateType()),
    StructField("skills", ArrayType(StringType())),
])


def _df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_clean_silver_passes(spark):
    rows = [
        ("1", "ACME", dt.date(2026, 6, 1), ["Python", "SQL"]),
        ("2", "Globex", dt.date(2026, 6, 2), ["Spark"]),
    ]
    results = run_checks(_df(spark, rows))
    assert all(r.passed for r in results)
    assert_quality(results)  # should not raise


def test_duplicate_ids_fail(spark):
    rows = [
        ("1", "ACME", dt.date(2026, 6, 1), ["Python"]),
        ("1", "ACME", dt.date(2026, 6, 1), ["Python"]),
    ]
    results = run_checks(_df(spark, rows))
    assert any(r.name == "source_id_unique" and not r.passed for r in results)
    with pytest.raises(DataQualityError):
        assert_quality(results)


def test_too_many_null_dates_fail(spark):
    rows = [("1", "ACME", None, ["Python"]), ("2", "Globex", None, ["SQL"])]
    results = run_checks(_df(spark, rows))
    assert any(r.name == "posted_date_parseable" and not r.passed for r in results)
    with pytest.raises(DataQualityError):
        assert_quality(results)


def test_missing_company_is_warning_not_error(spark):
    rows = [("1", None, dt.date(2026, 6, 1), ["Python"])]
    results = run_checks(_df(spark, rows))
    company_check = next(r for r in results if r.name == "company_present")
    assert not company_check.passed
    assert company_check.severity == "warn"
    assert_quality(results)  # warnings must not raise
