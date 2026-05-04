from __future__ import annotations

import datetime as dt
import os

import pandas as pd
import streamlit as st
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px

st.set_page_config(
    page_title="Healthcare Ops Command Center",
    page_icon="🩺",
    layout="wide",
)

st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;600&display=swap');

:root {
  --bg: #f6f3ea;
  --panel: #ffffff;
  --ink: #0f1b2d;
  --muted: #5a6a7a;
  --accent: #1b7f6b;
  --accent-2: #b36b00;
  --accent-3: #2d4f8b;
}

html, body, [class*="css"] {
  font-family: 'Space Grotesk', sans-serif;
  color: var(--ink);
}

.stApp {
  background: radial-gradient(circle at 15% 15%, #f2efe6 0, #f6f3ea 45%, #f9f6ee 100%);
}

.block-container {
  padding-top: 2rem;
}

h1, h2, h3 {
  letter-spacing: 0.3px;
}

.metric-card {
  background: var(--panel);
  border: 1px solid #e7e1d6;
  border-radius: 18px;
  padding: 16px 18px;
  box-shadow: 0 8px 30px rgba(15, 27, 45, 0.08);
}

.badge {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  background: #e8f2ef;
  color: var(--accent);
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1px;
}

.section-title {
  margin-top: 1.5rem;
  margin-bottom: 0.75rem;
}

.stMetric > div {
  background: transparent;
}

code, pre {
  font-family: 'IBM Plex Mono', monospace;
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("Healthcare Ops Command Center")

CREDENTIALS_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", ".google", "credentials", "google_credentials.json")
)
project_id = "de-zoomcamp-493207"
dataset = "gold"

if not os.path.exists(CREDENTIALS_PATH):
    st.error(
        "Missing credentials file. Expected at .google/credentials/google_credentials.json.",
    )
    st.stop()

@st.cache_resource(show_spinner=False)
def get_credentials() -> service_account.Credentials:
    return service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)


@st.cache_resource(show_spinner=False)
def get_client(project: str) -> bigquery.Client:
    return bigquery.Client(project=project, credentials=get_credentials())


def run_query(project: str, query: str, params: list[bigquery.ScalarQueryParameter]) -> pd.DataFrame:
    client = get_client(project)
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).result().to_dataframe()


def safe_query(
    project: str,
    query: str,
    params: list[bigquery.ScalarQueryParameter],
    label: str,
) -> pd.DataFrame:
    try:
        return run_query(project, query, params)
    except GoogleAPIError:
        st.warning(f"{label} is temporarily unavailable.")
        return pd.DataFrame()

start_date, end_date = '2020-02-01', '2020-04-01'
vitals_start, vitals_end = '2026-01-01', '2026-06-01'

admissions_params = [
    bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
    bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
]
vitals_params = [
    bigquery.ScalarQueryParameter("start_date", "DATE", vitals_start),
    bigquery.ScalarQueryParameter("end_date", "DATE", vitals_end),
]

try:
    overview_query = f"""
    select
      count(*) as admissions,
      count(distinct patient_key) as patients,
      avg(length_of_stay_days) as avg_los,
      avg(billing_amount) as avg_billing
    from `{project_id}.{dataset}.fact_admissions`
    where admission_date between @start_date and @end_date
    """
    overview = run_query(project_id, overview_query, admissions_params)

    admissions_query = f"""
    select
      admission_date,
      count(*) as admissions
    from `{project_id}.{dataset}.fact_admissions`
    where admission_date between @start_date and @end_date
    group by 1
    order by 1
    """
    admissions_ts = run_query(project_id, admissions_query, admissions_params)

    insurance_query = f"""
    select
      coalesce(insurance_provider, 'unknown') as insurance_provider,
      count(*) as admissions
    from `{project_id}.{dataset}.fact_admissions`
    where admission_date between @start_date and @end_date
    group by 1
    order by admissions desc
    """
    insurance_mix = run_query(project_id, insurance_query, admissions_params)

except GoogleAPIError as exc:
    st.error(
        "BigQuery query failed. Verify credentials, dataset, and table names before refreshing.",
    )
    st.exception(exc)
    st.stop()

vitals_query = f"""
select
    coalesce(cast(alert_flag as string), 'unknown') as alert_flag,
    count(*) as event_count
from `{project_id}.{dataset}.fact_vital_signs`
where event_date between @start_date and @end_date
group by 1
order by event_count desc
"""
vitals_alerts = safe_query(project_id, vitals_query, vitals_params, "Vitals data")

kpi = overview.iloc[0].fillna(0)

kpi_cols = st.columns(4)
with kpi_cols[0]:
    st.metric("Admissions", int(kpi["admissions"]))
with kpi_cols[1]:
    st.metric("Unique patients", int(kpi["patients"]))
with kpi_cols[2]:
    st.metric("Avg LOS (days)", round(float(kpi["avg_los"] or 0), 2))
with kpi_cols[3]:
    st.metric("Avg billing", f"${float(kpi['avg_billing'] or 0):,.2f}")

st.markdown('<span class="badge">Admissions pulse</span>', unsafe_allow_html=True)
st.subheader("Admissions over time")
if admissions_ts.empty:
    st.warning("No admissions found for the selected window.")
else:
    admissions_ts = admissions_ts.set_index("admission_date")
    st.line_chart(admissions_ts)

col_left, col_right = st.columns([1.1, 0.9])

with col_left:
    st.markdown('<span class="badge">Payer view</span>', unsafe_allow_html=True)
    st.subheader("Insurance distribution")
    if insurance_mix.empty:
        st.warning("No insurance data found.")
    else:
        insurance_mix = insurance_mix.set_index("insurance_provider")
        st.bar_chart(insurance_mix)

with col_right:
    st.markdown('<span class="badge">Vitals</span>', unsafe_allow_html=True)
    st.subheader("Vitals risk signal")
    if vitals_alerts.empty:
        st.warning("No vitals data available.")
    else:
        vitals_alerts = vitals_alerts.copy()
        vitals_alerts["alert_flag"] = vitals_alerts["alert_flag"].fillna("unknown")
        fig = px.pie(
            vitals_alerts,
            values="event_count",
            names="alert_flag",
            hole=0.35,
            color_discrete_sequence=["#1b7f6b", "#b36b00", "#2d4f8b", "#94a3b8"],
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), showlegend=True)
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
