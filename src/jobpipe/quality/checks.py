"""Data-quality gates run against the silver table.

`error`-severity failures raise and stop the pipeline (a bad load should never
reach gold); `warn`-severity failures are logged but allowed through. This is the
behaviour interviewers look for: quality checks that *block*, not just report.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Allow up to this fraction of postings to have no parseable date / no skills
# before we treat it as a real problem rather than ordinary source noise.
MAX_NULL_DATE_RATIO = 0.20
MAX_EMPTY_SKILLS_RATIO = 0.50


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: str  # "error" | "warn"
    detail: str


class DataQualityError(RuntimeError):
    """Raised when one or more error-severity checks fail."""


def run_checks(df: DataFrame) -> list[CheckResult]:
    df = df.cache()
    total = df.count()
    results: list[CheckResult] = []

    if total == 0:
        results.append(CheckResult("non_empty", False, "error", "silver table is empty"))
        df.unpersist()
        return results
    results.append(CheckResult("non_empty", True, "error", f"{total} rows"))

    # No null primary keys.
    null_ids = df.where(F.col("source_id").isNull()).count()
    results.append(
        CheckResult("source_id_not_null", null_ids == 0, "error", f"{null_ids} null ids")
    )

    # Primary key is unique (silver must be deduped).
    distinct_ids = df.select("source_id").distinct().count()
    dupes = total - distinct_ids
    results.append(
        CheckResult("source_id_unique", dupes == 0, "error", f"{dupes} duplicate ids")
    )

    # Company name present.
    null_company = df.where(F.col("company").isNull() | (F.trim(F.col("company")) == "")).count()
    results.append(
        CheckResult("company_present", null_company == 0, "warn", f"{null_company} missing companies")
    )

    # Parseable posting date.
    null_dates = df.where(F.col("posted_date").isNull()).count()
    ratio = null_dates / total
    results.append(
        CheckResult(
            "posted_date_parseable",
            ratio <= MAX_NULL_DATE_RATIO,
            "error",
            f"{null_dates}/{total} ({ratio:.1%}) unparseable dates",
        )
    )

    # Skills actually extracted for a reasonable share of rows.
    empty_skills = df.where(F.size(F.col("skills")) == 0).count()
    sratio = empty_skills / total
    results.append(
        CheckResult(
            "skills_extracted",
            sratio <= MAX_EMPTY_SKILLS_RATIO,
            "warn",
            f"{empty_skills}/{total} ({sratio:.1%}) rows with no skills",
        )
    )

    df.unpersist()
    return results


def assert_quality(results: list[CheckResult]) -> None:
    """Raise if any error-severity check failed; callers log the full list first."""
    failed = [r for r in results if not r.passed and r.severity == "error"]
    if failed:
        lines = "; ".join(f"{r.name}: {r.detail}" for r in failed)
        raise DataQualityError(f"{len(failed)} error-severity check(s) failed -> {lines}")
