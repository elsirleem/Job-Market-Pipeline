from __future__ import annotations

from pathlib import Path
import datetime as dt

import pandas as pd
import streamlit as st
from pyspark.sql import functions as F

from jobpipe.common import paths
from jobpipe.common.spark import get_spark


st.set_page_config(page_title="Salim's Job Search Agent", layout="wide")


@st.cache_resource
def spark_session():
    return get_spark("streamlit-ui")


def read_delta_table(path: str):
    delta_path = Path(path)
    if not delta_path.exists():
        return None
    return spark_session().read.format("delta").load(str(delta_path))


def to_frame(df, limit: int | None = None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    if limit is not None:
        df = df.limit(limit)
    return pd.DataFrame([row.asDict(recursive=True) for row in df.collect()])


def metric_value(df, column: str | None = None) -> int:
    if df is None:
        return 0
    if column is None:
        return df.count()
    return df.select(column).distinct().count()


st.title("Salim's Job Search Agent")
st.caption("Browse the saved job postings and gold aggregates from the Delta Lake tables.")

silver = read_delta_table(paths.SILVER)
if silver is None:
    st.warning("No silver table found yet. Run the pipeline first with `docker compose up -d --build`.")
    st.stop()

countries = [row[0] for row in silver.select("country_code").where("country_code is not null").distinct().orderBy("country_code").collect()]
seniority_levels = [row[0] for row in silver.select("seniority").where("seniority is not null").distinct().orderBy("seniority").collect()]
cities = [row[0] for row in silver.select("city").where("city is not null and trim(city) <> ''").distinct().orderBy("city").collect()]
date_bounds = silver.where(F.col("posted_date").isNotNull()).agg(
    F.min("posted_date").alias("min_date"),
    F.max("posted_date").alias("max_date"),
).collect()[0]

min_posted_date = date_bounds["min_date"]
max_posted_date = date_bounds["max_date"]
if min_posted_date is None or max_posted_date is None:
    min_posted_date = dt.date.today().replace(day=1)
    max_posted_date = dt.date.today()

default_start = max(min_posted_date, dt.date(max_posted_date.year, max_posted_date.month, 1))
default_range = (default_start, max_posted_date)

st.sidebar.header("Filters")
role_keyword = st.sidebar.text_input("Role keyword", value="", help="Example: data engineer, ml engineer, analytics engineer")
selected_countries = st.sidebar.multiselect("Country", countries, default=[])
selected_cities = st.sidebar.multiselect("City", cities, default=[])
location_keyword = st.sidebar.text_input("Location keyword", value="", help="Matches city, region, and location display")
seniority = st.sidebar.selectbox("Seniority", ["All levels", *seniority_levels])
search = st.sidebar.text_input("Other keyword", value="", help="Search across title, company, and skills")
posted_range = st.sidebar.date_input(
    "Posted date range",
    value=default_range,
    min_value=min_posted_date,
    max_value=max_posted_date,
)
row_limit = st.sidebar.slider("Rows to show", min_value=10, max_value=200, value=50, step=10)

filtered = silver
if selected_countries:
    filtered = filtered.where(F.col("country_code").isin(selected_countries))
if selected_cities:
    filtered = filtered.where(F.col("city").isin(selected_cities))
if role_keyword.strip():
    role_term = role_keyword.strip().lower()
    filtered = filtered.where(
        F.lower(F.coalesce(F.col("title"), F.lit(""))).contains(role_term)
    )
if location_keyword.strip():
    loc_term = location_keyword.strip().lower()
    filtered = filtered.where(
        F.lower(F.coalesce(F.col("city"), F.lit(""))).contains(loc_term)
        | F.lower(F.coalesce(F.col("region"), F.lit(""))).contains(loc_term)
        | F.lower(F.coalesce(F.col("location_display"), F.lit(""))).contains(loc_term)
        | F.lower(F.coalesce(F.col("country_code"), F.lit(""))).contains(loc_term)
    )
if seniority != "All levels":
    filtered = filtered.where(filtered.seniority == seniority)
if search.strip():
    term = search.strip().lower()
    filtered = filtered.where(
        F.col("title").isNotNull()
        & (
            F.lower(F.coalesce(F.col("title"), F.lit(""))).contains(term)
            | F.lower(F.coalesce(F.col("company"), F.lit(""))).contains(term)
            | F.lower(F.coalesce(F.col("skills").cast("string"), F.lit(""))).contains(term)
        )
    )
if isinstance(posted_range, tuple) and len(posted_range) == 2:
    start_date, end_date = posted_range
else:
    start_date = end_date = posted_range
filtered = filtered.where(
    F.col("posted_date").between(F.lit(start_date), F.lit(end_date))
)

metrics = st.columns(4)
metrics[0].metric("Jobs", f"{metric_value(filtered)}")
metrics[1].metric("Companies", f"{metric_value(filtered, 'company')}")
metrics[2].metric("Countries", f"{metric_value(filtered, 'country_code')}")
metrics[3].metric("Seniorities", f"{metric_value(filtered, 'seniority')}")

st.subheader("Job postings")
job_columns = [
    "title",
    "company",
    "country_code",
    "city",
    "seniority",
    "posted_date",
    "skills",
    "salary_min",
    "salary_max",
    "redirect_url",
]
jobs_frame = to_frame(filtered.select(*job_columns).orderBy(F.col("posted_date").desc_nulls_last(), "country_code", "company").limit(row_limit))
st.dataframe(
    jobs_frame,
    use_container_width=True,
    hide_index=True,
    column_config={
        "redirect_url": st.column_config.LinkColumn("Job link"),
    },
)

st.subheader("Gold summaries")
gold_tables = {
    "skills_by_country": paths.GOLD_SKILLS_BY_COUNTRY,
    "top_tools": paths.GOLD_TOP_TOOLS,
    "cloud_demand": paths.GOLD_CLOUD_DEMAND,
    "jobs_by_day": paths.GOLD_JOBS_BY_DAY,
    "top_companies": paths.GOLD_TOP_COMPANIES,
}

for name, path in gold_tables.items():
    table_df = read_delta_table(path)
    with st.expander(name, expanded=False):
        if table_df is None:
            st.info("This table is not available yet.")
        else:
            st.write(f"{metric_value(table_df)} rows")
            st.dataframe(to_frame(table_df.limit(20)), use_container_width=True, hide_index=True)