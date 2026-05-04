/* @bruin

name: gold.fact_hospital_capacity
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table
  partition_by: collection_week
  cluster_by:
    - hospital_key

depends:
  - silver.hospital_capacity
  - gold.dim_hospital

columns:
  - name: capacity_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: hospital_key
    type: string
    checks:
      - name: not_null
  - name: collection_week
    type: date
    checks:
      - name: not_null
  - name: total_beds_7_day_avg
    type: float
    checks:
      - name: min
        value: 0
  - name: bed_utilization_rate
    type: float
  - name: icu_utilization_rate
    type: float

custom_checks:
  - name: table has rows
    value: 1
    query: select count(*) > 0 from `gold.fact_hospital_capacity`

@bruin */

with cleaned as (
  select
    capacity_key,
    cast(hospital_pk as string) as hospital_pk,
    safe_cast(collection_week as date) as collection_week,
    nullif(trim(hospital_name), '') as hospital_name,
    upper(nullif(trim(state), '')) as state,
    lower(nullif(trim(city), '')) as city,
    nullif(trim(zip_code), '') as zip_code,
    nullif(trim(fips_code), '') as fips_code,
    safe_cast(total_beds_7_day_avg as float64) as total_beds_7_day_avg,
    safe_cast(inpatient_beds_used_7_day_avg as float64) as inpatient_beds_used_7_day_avg,
    safe_cast(total_icu_beds_7_day_avg as float64) as total_icu_beds_7_day_avg,
    safe_cast(icu_beds_used_7_day_avg as float64) as icu_beds_used_7_day_avg,
    safe_cast(adult_covid_patients_7_day_avg as float64) as adult_covid_patients_7_day_avg,
    safe_cast(influenza_patients_7_day_avg as float64) as influenza_patients_7_day_avg
  from `silver.hospital_capacity`
)
select
  c.capacity_key,
  dh.hospital_key,
  c.hospital_pk,
  c.collection_week,
  c.hospital_name,
  c.state,
  c.city,
  c.zip_code,
  c.fips_code,
  if(c.total_beds_7_day_avg >= 0, c.total_beds_7_day_avg, null) as total_beds_7_day_avg,
  if(c.inpatient_beds_used_7_day_avg >= 0, c.inpatient_beds_used_7_day_avg, null) as inpatient_beds_used_7_day_avg,
  if(c.total_icu_beds_7_day_avg >= 0, c.total_icu_beds_7_day_avg, null) as total_icu_beds_7_day_avg,
  if(c.icu_beds_used_7_day_avg >= 0, c.icu_beds_used_7_day_avg, null) as icu_beds_used_7_day_avg,
  safe_divide(
    if(c.inpatient_beds_used_7_day_avg >= 0, c.inpatient_beds_used_7_day_avg, null),
    if(c.total_beds_7_day_avg > 0, c.total_beds_7_day_avg, null)
  ) as bed_utilization_rate,
  safe_divide(
    if(c.icu_beds_used_7_day_avg >= 0, c.icu_beds_used_7_day_avg, null),
    if(c.total_icu_beds_7_day_avg > 0, c.total_icu_beds_7_day_avg, null)
  ) as icu_utilization_rate,
  if(c.adult_covid_patients_7_day_avg >= 0, c.adult_covid_patients_7_day_avg, null) as adult_covid_patients_7_day_avg,
  if(c.influenza_patients_7_day_avg >= 0, c.influenza_patients_7_day_avg, null) as influenza_patients_7_day_avg
from cleaned as c
inner join `gold.dim_hospital` as dh
  on c.hospital_pk is not null
 and dh.hospital_pk = c.hospital_pk
where c.collection_week is not null;
