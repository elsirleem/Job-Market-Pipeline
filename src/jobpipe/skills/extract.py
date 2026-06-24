"""Keyword-based skill extraction.

A curated taxonomy maps a canonical skill name to the surface forms that appear
in job text. We compile word-boundary regexes once at import time. This is
deliberately a dictionary lookup rather than an NLP model: it is transparent,
fast, deterministic and trivial to explain in an interview.
"""
from __future__ import annotations

import re

# canonical name -> list of aliases (matched case-insensitively, word-bounded)
SKILL_TAXONOMY: dict[str, list[str]] = {
    "Python": ["python"],
    "SQL": ["sql"],
    "PySpark": ["pyspark"],
    "Spark": ["spark", "apache spark"],
    "Databricks": ["databricks"],
    "Delta Lake": ["delta lake", "delta tables?"],
    "Microsoft Fabric": ["microsoft fabric", "ms fabric"],
    "Snowflake": ["snowflake"],
    "dbt": ["dbt"],
    "Airflow": ["airflow"],
    "Kafka": ["kafka"],
    "Flink": ["flink"],
    "Azure": ["azure", "adf", "azure data factory", "synapse"],
    "AWS": ["aws", "redshift", "glue", "s3"],
    "GCP": ["gcp", "google cloud", "bigquery"],
    "Docker": ["docker"],
    "Kubernetes": ["kubernetes", "k8s"],
    "Terraform": ["terraform"],
    "PostgreSQL": ["postgresql", "postgres"],
    "MySQL": ["mysql"],
    "MongoDB": ["mongodb", "mongo"],
    "Hadoop": ["hadoop"],
    "Hive": ["hive"],
    "Scala": ["scala"],
    "Java": ["java"],
    "Power BI": ["power bi", "powerbi"],
    "Tableau": ["tableau"],
    "CI/CD": ["ci/cd", "ci cd", "github actions", "gitlab ci"],
}

# Cloud platforms get singled out for the Azure-vs-AWS-vs-GCP gold table.
CLOUD_SKILLS = {"Azure", "AWS", "GCP"}


def _compile(aliases: list[str]) -> re.Pattern:
    # Escape only literal aliases; a couple use a regex quantifier ("tables?").
    parts = []
    for a in aliases:
        parts.append(a if a.endswith("?") or " " in a and "?" in a else re.escape(a))
    return re.compile(r"\b(?:" + "|".join(parts) + r")\b", re.IGNORECASE)


_COMPILED: dict[str, re.Pattern] = {name: _compile(al) for name, al in SKILL_TAXONOMY.items()}


def extract_skills(text: str | None) -> list[str]:
    """Return the sorted list of canonical skills mentioned in `text`."""
    if not text:
        return []
    found = [name for name, pattern in _COMPILED.items() if pattern.search(text)]
    return sorted(found)
