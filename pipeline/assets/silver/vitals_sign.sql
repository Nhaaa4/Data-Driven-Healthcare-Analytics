/* @bruin

name: silver.vitals_sign
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table
  partition_by: event_date
  cluster_by:
    - patient_key

depends:
  - bronze.raw_vitals_sign

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

@bruin */

select
  generate_uuid() as vital_key,
  subject_id as patient_key,
  cast(device_id as int64) as device_id,
  timestamp(event_time) as event_time,
  date(timestamp(event_time)) as event_date,
  cast(heart_rate as int64) as heart_rate,
  cast(oxygen_level as int64) as oxygen_level,
  cast(systolic_bp as int64) as systolic_bp,
  cast(diastolic_bp as int64) as diastolic_bp,
  round(cast(body_temperature as float64), 1) as body_temperature,
  lower(coalesce(nullif(trim(activity_level), ''), 'unknown')) as activity_level,
  coalesce(cast(alert_flag as bool), false) as alert_flag,
  cast(heart_rate as int64) > 100 as is_tachycardic,
  cast(oxygen_level as int64) < 92 as is_hypoxic,
  cast(systolic_bp as int64) >= 140 as is_hypertensive,
  (
    if(cast(heart_rate as int64) > 100, 1, 0) +
    if(cast(oxygen_level as int64) < 92, 1, 0) +
    if(cast(systolic_bp as int64) >= 140, 1, 0) +
    if(coalesce(cast(alert_flag as bool), false), 1, 0)
  ) as risk_score
from `de-zoomcamp-493207.bronze.raw_vitals_sign`
