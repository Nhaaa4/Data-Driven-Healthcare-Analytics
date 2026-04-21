/* @bruin

name: gold.fct_healthcare_vitals
type: bq.sql
connection: healthcare_bq
depends:
  - silver.silver_vital_sign
  - gold.dim_patient

@bruin */

create or replace table `healthcare_gold.fct_vital_sign`
partition by event_date
cluster by patient_key, device_key as
select
  abs(farm_fingerprint(v.vital_event_id)) as vital_sign_fact_key,
  v.vital_event_id,
  abs(farm_fingerprint(cast(v.subject_id as string))) as patient_key,
  abs(farm_fingerprint(cast(v.device_id as string))) as device_key,
  cast(format_date('%Y%m%d', v.event_date) as int64) as event_date_key,
  v.event_date,
  v.event_time,
  v.heart_rate,
  v.oxygen_level,
  v.systolic_bp,
  v.diastolic_bp,
  v.body_temperature,
  v.activity_level,
  v.alert_flag,
  v.is_tachycardic,
  v.is_hypoxic,
  v.is_hypertensive,
  v.risk_score
from `healthcare_silver.silver_vital_sign` as v;
