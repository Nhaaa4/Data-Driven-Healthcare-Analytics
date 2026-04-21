/* @bruin

name: gold.fct_admission
type: bq.sql
connection: healthcare_bq
depends:
  - silver.admissions
  - gold.dim_patient
  - gold.dim_diagnosis
  - gold.dim_date

materialization:
  type: table

@bruin */

select
  abs(farm_fingerprint(cast(a.hadm_id as string))) as admission_fact_key,
  a.hadm_id,
  abs(farm_fingerprint(cast(a.subject_id as string))) as patient_key,
  cast(format_date('%Y%m%d', a.admit_date) as int64) as admit_date_key,
  cast(format_date('%Y%m%d', a.discharge_date) as int64) as discharge_date_key,
  a.admit_date,
  a.discharge_date,
  abs(farm_fingerprint(a.primary_diagnosis_code)) as primary_diagnosis_key,
  a.stay_length_hours,
  a.age_at_admission_years,
  a.expired_in_hospital
from `silver.admissions` as a;
