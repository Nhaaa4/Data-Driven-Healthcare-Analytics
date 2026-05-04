/* @bruin

name: gold.fact_admissions
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table
  partition_by: admission_date
  cluster_by:
    - patient_key
    - hospital_key

depends:
  - silver.healthcare_admissions
  - gold.dim_hospital
  - gold.dim_insurance

columns:
  - name: admission_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: patient_key
    type: string
    checks:
      - name: not_null
  - name: hospital_key
    type: string
    checks:
      - name: not_null
  - name: insurance_key
    type: string
    checks:
      - name: not_null
  - name: admission_date
    type: date
    checks:
      - name: not_null
  - name: length_of_stay_days
    type: integer
    checks:
      - name: min
        value: 0
  - name: billing_amount
    type: float
    checks:
      - name: min
        value: 0

custom_checks:
  - name: table has rows
    value: 1
    query: select count(*) > 0 from `gold.fact_admissions`

@bruin */

with admissions as (
  select
    a.admission_key,
    a.patient_key,
    a.medical_condition,
    a.admission_date,
    a.discharge_date,
    a.doctor_name,
    a.hospital_name,
    a.insurance_provider,
    a.billing_amount,
    a.room_number,
    a.admission_type,
    a.medication_name,
    a.test_results
  from `silver.healthcare_admissions` as a
),
cleaned as (
  select
    nullif(trim(patient_key), '') as patient_key,
    nullif(trim(hospital_name), '') as hospital_name,
    lower(nullif(trim(hospital_name), '')) as hospital_name_norm,
    lower(nullif(trim(insurance_provider), '')) as insurance_provider_norm,
    safe_cast(admission_date as date) as admission_date,
    safe_cast(discharge_date as date) as discharge_date,
    lower(nullif(trim(medical_condition), '')) as medical_condition,
    nullif(trim(doctor_name), '') as doctor_name,
    nullif(trim(room_number), '') as room_number,
    lower(nullif(trim(admission_type), '')) as admission_type,
    nullif(trim(medication_name), '') as medication_name,
    lower(nullif(trim(test_results), '')) as test_results,
    safe_cast(billing_amount as float64) as billing_amount
  from admissions
)
select
  generate_uuid() as admission_key,
  c.patient_key,
  coalesce(
    dh.hospital_key,
    to_hex(md5(concat('name|', coalesce(c.hospital_name_norm, ''))))
  ) as hospital_key,
  coalesce(
    di.insurance_key,
    to_hex(md5(coalesce(c.insurance_provider_norm, 'unknown')))
  ) as insurance_key,
  c.admission_date,
  c.discharge_date,
  case
    when c.admission_date is not null
      and c.discharge_date is not null
      and c.discharge_date >= c.admission_date
      then date_diff(c.discharge_date, c.admission_date, day)
    else null
  end as length_of_stay_days,
  c.medical_condition,
  c.doctor_name,
  c.hospital_name,
  c.insurance_provider_norm as insurance_provider,
  c.billing_amount,
  c.room_number,
  c.admission_type,
  c.medication_name,
  c.test_results,
  1 as admission_count
from cleaned as c
left join `gold.dim_hospital` as dh
  on lower(nullif(trim(dh.hospital_name), '')) = c.hospital_name_norm
left join `gold.dim_insurance` as di
  on lower(nullif(trim(di.insurance_provider), '')) = c.insurance_provider_norm
where c.admission_date is not null
  and c.patient_key is not null;
