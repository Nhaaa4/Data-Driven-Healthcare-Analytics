/* @bruin

name: gold.dim_patient
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table
  cluster_by:
    - patient_key

depends:
  - silver.patients

columns:
  - name: patient_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: patient_name
    type: string
    checks:
      - name: not_null
  - name: age
    type: integer
    checks:
      - name: min
        value: 0
  - name: gender
    type: string
    checks:
      - name: not_null

custom_checks:
  - name: table has rows
    value: 1
    query: select count(*) > 0 from `gold.dim_patient`

@bruin */

with cleaned as (
  select
    patient_key,
    lower(nullif(trim(patient_name), '')) as patient_name,
    safe_cast(age as int64) as age,
    lower(nullif(trim(gender), '')) as gender,
    lower(nullif(trim(blood_type), '')) as blood_type,
    safe_cast(birth_date as date) as birth_date
  from `silver.patients`
)
select
  patient_key,
  patient_name,
  age,
  gender,
  blood_type,
  birth_date,
  'silver.patients' as source_system
from cleaned
where patient_name is not null
  and age is not null
  and gender is not null;
