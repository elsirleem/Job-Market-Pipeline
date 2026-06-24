"""Medallion layout helpers — one place that knows where each layer lives."""
from __future__ import annotations

from config.settings import settings

BRONZE = f"{settings.lake_root}/bronze/jobs"
SILVER = f"{settings.lake_root}/silver/jobs"

# Gold is a small star of aggregate tables, each independently queryable.
GOLD_SKILLS_BY_COUNTRY = f"{settings.lake_root}/gold/skills_by_country"
GOLD_TOP_TOOLS = f"{settings.lake_root}/gold/top_tools"
GOLD_CLOUD_DEMAND = f"{settings.lake_root}/gold/cloud_demand"
GOLD_JOBS_BY_DAY = f"{settings.lake_root}/gold/jobs_by_day"
GOLD_TOP_COMPANIES = f"{settings.lake_root}/gold/top_companies"
