/* @bruin

name: silver.patients
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table

depends:
  - bronze.raw_healthcare_admissions

columns:
  - name: patient_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: birth_date
    type: date
  - name: gender
    type: string

@bruin */

with source_patients as (
  select
    nullif(trim(name), '') as patient_name,
    safe_cast(age as int64) as age,
    lower(nullif(trim(gender), '')) as gender,
    coalesce(any_value(nullif(trim(blood_type), '')), 'unknown') as blood_type
  from `bronze.raw_healthcare_admissions`
  where nullif(trim(name), '') is not null
    and safe_cast(age as int64) is not null
    and lower(nullif(trim(gender), '')) is not null
  group by 1, 2, 3
)
select
  generate_uuid() as patient_key,
  patient_name,
  age,
  date_sub(current_date(), interval age year) as birth_date,
  gender,
  blood_type
from source_patients;
