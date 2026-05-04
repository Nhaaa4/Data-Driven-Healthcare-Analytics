/* @bruin

name: silver.healthcare_admissions
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table

depends:
  - bronze.raw_healthcare_admissions
  - silver.patients

columns:
  - name: admission_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: billing_amount
    type: float
    checks:
      - name: min
        value: 0

@bruin */

select
  generate_uuid() as admission_key,
  p.patient_key as patient_key,
  coalesce(nullif(trim(a.medical_condition), ''), 'unknown') as medical_condition,
  coalesce(safe_cast(a.date_of_admission as date), date '1900-01-01') as admission_date,
  coalesce(safe_cast(a.discharge_date as date), date '1900-01-01') as discharge_date,
  coalesce(nullif(trim(a.doctor), ''), 'unknown') as doctor_name,
  coalesce(nullif(trim(a.hospital), ''), 'unknown') as hospital_name,
  coalesce(nullif(trim(a.insurance_provider), ''), 'unknown') as insurance_provider,
  safe_cast(a.billing_amount as float64) as billing_amount,
  coalesce(cast(a.room_number as string), 'unknown') as room_number,
  coalesce(lower(nullif(trim(a.admission_type), '')), 'unknown') as admission_type,
  coalesce(nullif(trim(a.medication), ''), 'unknown') as medication_name,
  coalesce(lower(nullif(trim(a.test_results), '')), 'unknown') as test_results
from `bronze.raw_healthcare_admissions` as a
inner join `silver.patients` as p
  on lower(nullif(trim(a.name), '')) = lower(p.patient_name)
  and safe_cast(a.age as int64) = p.age
  and lower(nullif(trim(a.gender), '')) = p.gender
where safe_cast(a.billing_amount as float64) >= 0
  and nullif(trim(a.name), '') is not null
  and safe_cast(a.age as int64) is not null
  and lower(nullif(trim(a.gender), '')) is not null;
