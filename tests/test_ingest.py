"""Unit tests for the Adzuna payload flattener — no network, no Spark."""
from __future__ import annotations

from jobpipe.ingest.adzuna import _flatten

SAMPLE = {
    "id": 12345,
    "title": "Junior Data Engineer",
    "company": {"display_name": "ACME Data"},
    "location": {"display_name": "Berlin, Germany", "area": ["Germany", "Berlin", "Berlin"]},
    "category": {"label": "IT Jobs"},
    "contract_time": "full_time",
    "salary_min": 45000,
    "salary_max": 55000,
    "created": "2026-06-01T09:00:00Z",
    "description": "Work with Python and Spark.",
    "redirect_url": "https://example.com/job/12345",
}


def test_flatten_extracts_nested_fields():
    row = _flatten(SAMPLE, country="de", ingested_at="2026-06-24T00:00:00Z")
    assert row["source_name"] == "adzuna"
    assert row["source_id"] == "12345"
    assert row["company"] == "ACME Data"
    assert row["country_code"] == "de"
    assert row["region"] == "Berlin"
    assert row["city"] == "Berlin"
    assert row["category"] == "IT Jobs"
    assert row["ingested_at"] == "2026-06-24T00:00:00Z"


def test_flatten_tolerates_missing_fields():
    row = _flatten({"title": "Data Engineer"}, country="nl", ingested_at="t")
    assert row["source_name"] == "adzuna"
    assert row["source_id"] is None
    assert row["company"] is None
    assert row["city"] is None
    assert row["country_code"] == "nl"


def test_flatten_normalizes_salary_types():
    row = _flatten(
        {"id": 1, "salary_min": 45000, "salary_max": 55000.5},
        country="gb",
        ingested_at="t",
    )
    assert row["salary_min"] == 45000.0
    assert row["salary_max"] == 55000.5
