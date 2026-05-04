/* @bruin

name: gold.fact_vital_signs
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - patient_key

depends:
  - silver.vitals_sign

columns:
  - name: vital_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: patient_key
    type: string
    checks:
      - name: not_null
  - name: event_date
    type: date
    checks:
      - name: not_null
  - name: oxygen_level
    type: integer
    checks:
      - name: min
        value: 50
      - name: max
        value: 100
  - name: risk_score
    type: integer
    checks:
      - name: min
        value: 0
      - name: max
        value: 4
@bruin */


select
  vital_key,
  cast(patient_key as string) as patient_key,
  cast(device_id as string) as device_id,
  timestamp(event_time) as event_time,
  date(timestamp(event_time)) as event_date,
  safe_cast(heart_rate as int64) as heart_rate,
  safe_cast(oxygen_level as int64) as oxygen_level,
  safe_cast(systolic_bp as int64) as systolic_bp,
  safe_cast(diastolic_bp as int64) as diastolic_bp,
  round(safe_cast(body_temperature as float64), 1) as body_temperature,
  lower(coalesce(nullif(trim(activity_level), ''), 'unknown')) as activity_level,
  coalesce(safe_cast(alert_flag as bool), false) as alert_flag,
  safe_cast(heart_rate as int64) > 100 as is_tachycardic,
  safe_cast(oxygen_level as int64) < 92 as is_hypoxic,
  safe_cast(systolic_bp as int64) >= 140 as is_hypertensive,
  (
    if(safe_cast(heart_rate as int64) > 100, 1, 0) +
    if(safe_cast(oxygen_level as int64) < 92, 1, 0) +
    if(safe_cast(systolic_bp as int64) >= 140, 1, 0) +
    if(coalesce(safe_cast(alert_flag as bool), false), 1, 0)
  ) as risk_score
from `silver.vitals_sign`