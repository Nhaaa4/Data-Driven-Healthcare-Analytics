/* @bruin

name: silver.healthcare_vitals
type: bq.sql
connection: healthcare_bq
depends:
  - bronze.healthcare_vitals

materialization:
  type: table

columns:
  - name: vital_event_id
    type: string
    checks:
      - name: not_null
      - name: unique

  - name: subject_id
    type: integer
    checks:
      - name: not_null

  - name: event_date
    type: date
    checks:
      - name: not_null
      
  - name: risk_score
    type: float
    checks:
      - name: min
        value: 0

@bruin */

select
  GENERATE_UUID() AS vital_event_id,
  subject_id,
  device_id,
  event_time,
  date(timestamp(event_time)) AS event_date,
  heart_rate,
  oxygen_level,
  systolic_bp,
  diastolic_bp,
  round(body_temperature, 1) as body_temperature,
  lower(coalesce(nullif(trim(activity_level), ''), 'resting')) as activity_level,
  coalesce(alert_flag, false) as alert_flag,
  heart_rate > 100 as is_tachycardic,
  oxygen_level < 92 as is_hypoxic,
  systolic_bp >= 140 as is_hypertensive,
  (
    if(heart_rate > 100, 1.0, 0.0) +
    if(oxygen_level < 92, 1.0, 0.0) +
    if(systolic_bp >= 140, 1.0, 0.0) +
    if(coalesce(alert_flag, false), 1.0, 0.0)
  ) as risk_score
from `bronze.healthcare_vitals`
where subject_id is not null
  and device_id is not null
  and event_time is not null
  and heart_rate between 20 and 250
  and oxygen_level between 50 and 100
  and systolic_bp between 60 and 250
  and diastolic_bp between 30 and 150
  and body_temperature between 30 and 45;
