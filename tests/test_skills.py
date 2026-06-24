"""Pure unit tests for skill extraction — no Spark, fast."""
from __future__ import annotations

from jobpipe.skills.extract import extract_skills


def test_extracts_common_tools():
    text = "We need Python, SQL and PySpark on Azure Databricks with Airflow."
    skills = extract_skills(text)
    assert {"Python", "SQL", "PySpark", "Azure", "Databricks", "Airflow"} <= set(skills)


def test_is_case_insensitive():
    assert "Python" in extract_skills("strong PYTHON skills")


def test_word_boundaries_avoid_false_positives():
    # "java" must not match inside "javascript".
    assert "Java" not in extract_skills("frontend javascript developer")


def test_aliases_map_to_canonical_name():
    assert "GCP" in extract_skills("experience with bigquery")
    assert "AWS" in extract_skills("we run everything on redshift")


def test_handles_empty_and_none():
    assert extract_skills("") == []
    assert extract_skills(None) == []


def test_result_is_sorted_and_unique():
    skills = extract_skills("python Python SQL sql")
    assert skills == sorted(skills)
    assert len(skills) == len(set(skills))
