"""Adzuna ingestion -> bronze Delta table.

Design notes
------------
* The HTTP layer (`fetch_jobs`) is pure Python and has no Spark dependency, so it
  is fast to unit-test and mock.
* Bronze is *append-only* and stores the payload close to its raw shape plus
  ingestion metadata. We never dedupe here — silver owns that. This keeps a full
  audit trail of what the source returned on each run.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import requests
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from config.settings import Settings
from jobpipe.common.paths import BRONZE

API_TEMPLATE = "https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
RESULTS_PER_PAGE = 50
REQUEST_TIMEOUT = 30

BRONZE_SCHEMA = StructType(
    [
        StructField("source_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("company", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("location_display", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("category", StringType(), True),
        StructField("contract_time", StringType(), True),
        StructField("salary_min", DoubleType(), True),
        StructField("salary_max", DoubleType(), True),
        StructField("created", StringType(), True),
        StructField("description", StringType(), True),
        StructField("redirect_url", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)


def fetch_jobs(cfg: Settings) -> list[dict]:
    """Pull raw job dicts from Adzuna across all configured countries/pages.

    Returns a flat list of records, each tagged with the country it came from
    and the ingestion timestamp. Network/transport concerns live here only.
    """
    cfg.require_adzuna_creds()
    ingested_at = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []

    for country in cfg.countries:
        for page in range(1, cfg.max_pages + 1):
            url = API_TEMPLATE.format(country=country, page=page)
            params = {
                "app_id": cfg.adzuna_app_id,
                "app_key": cfg.adzuna_app_key,
                "results_per_page": RESULTS_PER_PAGE,
                "what": cfg.query,
                "content-type": "application/json",
            }
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            page_results = resp.json().get("results", [])
            if not page_results:
                break  # no more pages for this country

            for r in page_results:
                records.append(_flatten(r, country, ingested_at))

            time.sleep(0.5)  # be polite to the free-tier rate limit

    return records


def _flatten(r: dict, country: str, ingested_at: str) -> dict:
    """Pull the nested Adzuna fields we care about into a flat row.

    Anything missing becomes None so the bronze schema stays stable — dirtiness
    is expected and gets handled downstream, not hidden here.
    """
    company = (r.get("company") or {}).get("display_name")
    location = r.get("location") or {}
    area = location.get("area") or []
    category = (r.get("category") or {}).get("label")

    def _as_float(value):
        return float(value) if value is not None else None

    return {
        "source_id": str(r.get("id")) if r.get("id") is not None else None,
        "title": r.get("title"),
        "company": company,
        "country_code": country,
        "location_display": location.get("display_name"),
        "region": area[1] if len(area) > 1 else None,
        "city": area[-1] if area else None,
        "category": category,
        "contract_time": r.get("contract_time"),
        "salary_min": _as_float(r.get("salary_min")),
        "salary_max": _as_float(r.get("salary_max")),
        "created": r.get("created"),
        "description": r.get("description"),
        "redirect_url": r.get("redirect_url"),
        "ingested_at": ingested_at,
    }


def write_bronze(spark, records: list[dict]) -> int:
    """Append fetched records to the bronze Delta table. Returns rows written."""
    if not records:
        return 0
    df = spark.createDataFrame(records, schema=BRONZE_SCHEMA)
    df.write.format("delta").mode("append").save(BRONZE)
    return df.count()
