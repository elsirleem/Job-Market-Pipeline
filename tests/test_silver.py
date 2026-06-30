"""Spark tests for the bronze->silver transform."""
from __future__ import annotations

from jobpipe.transform.silver import transform_silver


def _bronze_row(**overrides):
    base = dict(
        source_name="adzuna", source_id="1", title="Data Engineer", company=" ACME ", country_code="DE",
        city="Berlin", region="Berlin", location_display="Berlin, Germany",
        category="IT Jobs", contract_time="full_time", salary_min=40000.0,
        salary_max=50000.0, created="2026-06-01T09:00:00Z",
        description="Python and Spark on Azure.", redirect_url="https://x/1",
        ingested_at="2026-06-01T00:00:00Z",
    )
    base.update(overrides)
    return base


def test_dedup_keeps_latest_ingested(spark):
    rows = [
        _bronze_row(source_id="1", title="OLD", ingested_at="2026-06-01T00:00:00Z"),
        _bronze_row(source_id="1", title="NEW", ingested_at="2026-06-02T00:00:00Z"),
    ]
    out = transform_silver(spark.createDataFrame(rows)).collect()
    assert len(out) == 1
    assert out[0]["title"] == "NEW"


def test_normalization_and_enrichment(spark):
    rows = [_bronze_row(title="Junior Data Engineer", company=" ACME ", country_code="DE")]
    row = transform_silver(spark.createDataFrame(rows)).collect()[0]
    assert row["company"] == "ACME"          # trimmed
    assert row["country_code"] == "de"        # lowercased
    assert row["seniority"] == "junior"       # classified from title
    assert str(row["posted_date"]) == "2026-06-01"  # parsed to date
    assert "Python" in row["skills"]
    assert "Azure" in row["clouds"]


def test_rows_with_null_source_id_dropped(spark):
    rows = [_bronze_row(source_id=None), _bronze_row(source_id="2")]
    out = transform_silver(spark.createDataFrame(rows)).collect()
    assert [r["source_id"] for r in out] == ["2"]


def test_dedup_is_source_aware(spark):
    rows = [
        _bronze_row(source_name="adzuna", source_id="1", title="A", ingested_at="2026-06-01T00:00:00Z"),
        _bronze_row(source_name="adzuna", source_id="1", title="B", ingested_at="2026-06-02T00:00:00Z"),
        _bronze_row(source_name="indeed", source_id="1", title="C", ingested_at="2026-06-03T00:00:00Z"),
    ]
    out = transform_silver(spark.createDataFrame(rows)).collect()
    assert {(r["source_name"], r["title"]) for r in out} == {("adzuna", "B"), ("indeed", "C")}
