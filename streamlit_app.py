from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
from pyspark.sql import functions as F

from jobpipe.common import paths
from jobpipe.common.spark import get_spark


st.set_page_config(page_title="EU Job Market Pipeline", layout="wide")


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


st.title("EU Job Market Pipeline")
st.caption("Browse the saved job postings and gold aggregates from the Delta Lake tables.")

silver = read_delta_table(paths.SILVER)
if silver is None:
    st.warning("No silver table found yet. Run the pipeline first with `docker compose up -d --build`.")
    st.stop()

countries = [row[0] for row in silver.select("country_code").where("country_code is not null").distinct().orderBy("country_code").collect()]
seniority_levels = [row[0] for row in silver.select("seniority").where("seniority is not null").distinct().orderBy("seniority").collect()]

st.sidebar.header("Filters")
country = st.sidebar.selectbox("Country", ["All countries", *countries])
seniority = st.sidebar.selectbox("Seniority", ["All levels", *seniority_levels])
search = st.sidebar.text_input("Search title/company", value="")
row_limit = st.sidebar.slider("Rows to show", min_value=10, max_value=200, value=50, step=10)

filtered = silver
if country != "All countries":
    filtered = filtered.where(filtered.country_code == country)
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

metrics = st.columns(4)
metrics[0].metric("Jobs", f"{metric_value(filtered)}")
metrics[1].metric("Companies", f"{metric_value(filtered, 'company')}")
metrics[2].metric("Countries", f"{metric_value(filtered, 'country_code')}")
metrics[3].metric("Seniorities", f"{metric_value(filtered, 'seniority')}")

st.subheader("Job postings")
job_columns = ["title", "company", "country_code", "city", "seniority", "skills", "salary_min", "salary_max"]
st.dataframe(to_frame(filtered.select(*job_columns).orderBy("country_code", "company").limit(row_limit)), use_container_width=True, hide_index=True)

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