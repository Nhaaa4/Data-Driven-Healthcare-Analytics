/* @bruin

name: silver.admissions
type: bq.sql
connection: healthcare_bq
depends:
  - silver.patients
  - bronze.mimic_admissions
  - bronze.mimic_diagnoses

materialization:
  type: table

columns:
  - name: hadm_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: subject_id
    type: integer
    checks:
      - name: not_null
  - name: admit_date
    type: date
    checks:
      - name: not_null

@bruin */

select
  a.hadm_id,
  a.subject_id,
  a.admittime,
  a.dischtime,
  a.deathtime,
  cast(a.admittime as date) as admit_date,
  cast(a.dischtime as date) as discharge_date,
  coalesce(nullif(trim(a.admission_type), ''), 'UNKNOWN') as admission_type,
  coalesce(nullif(trim(a.admission_location), ''), 'UNKNOWN') as admission_location,
  coalesce(nullif(trim(a.discharge_location), ''), 'UNKNOWN') as discharge_location,
  coalesce(nullif(trim(a.insurance), ''), 'UNKNOWN') as insurance,
  coalesce(nullif(trim(a.language), ''), 'UNKNOWN') as language,
  coalesce(nullif(trim(a.religion), ''), 'UNKNOWN') as religion,
  coalesce(nullif(trim(a.marital_status), ''), 'UNKNOWN') as marital_status,
  coalesce(nullif(trim(a.ethnicity), ''), 'UNKNOWN') as ethnicity,
  coalesce(nullif(trim(a.diagnosis), ''), 'UNKNOWN') as admission_diagnosis,
  pdiag.diagnosis_key as primary_diagnosis_key,
  cast(a.hospital_expire_flag as bool) as expired_in_hospital,
  cast(a.has_chartevents_data as bool) as has_chartevents_data,
  safe_divide(timestamp_diff(coalesce(a.dischtime, current_timestamp()), a.admittime, minute), 60.0) as stay_length_hours,
  date_diff(cast(a.admittime as date), p.date_of_birth, year) as age_at_admission_years,
  coalesce(p.gender, 'UNKNOWN') as gender,
  coalesce(p.is_deceased, false) as patient_is_deceased
from `bronze.mimic_admissions` as a
left join `silver.patients` as p
  on a.subject_id = p.subject_id
left join `silver.diagnoses` as pdiag
  on a.hadm_id = pdiag.hadm_id
where cast(a.admittime as date) between date('{{ start_date }}') and date('{{ end_date }}')
  and a.hadm_id is not null
  and a.subject_id is not null
  and a.admittime is not null
  and (a.dischtime is null or a.dischtime >= a.admittime)
  and date_diff(cast(a.admittime as date), p.date_of_birth, year) between 0 and 120;
