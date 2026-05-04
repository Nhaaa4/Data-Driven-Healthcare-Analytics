/* @bruin

name: silver.hospital_capacity
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table

depends:
  - bronze.raw_hospital_capacity

columns:
  - name: capacity_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: hospital_pk
    type: string
    checks:
      - name: not_null
  - name: collection_week
    type: date

@bruin */

select
  generate_uuid() as capacity_key,
  cast(_hospital_pk as string) as hospital_pk,
  safe_cast(collection_week as date) as collection_week,
  upper(nullif(trim(state), '')) as state,
  nullif(trim(hospital_name), '') as hospital_name,
  nullif(trim(address), '') as hospital_address,
  nullif(trim(city), '') as city,
  cast(zip as string) as zip_code,
  nullif(trim(hospital_subtype), '') as hospital_subtype,
  cast(fips_code as string) as fips_code,
  nullif(trim(is_metro_micro), '') as metro_micro_area,
  safe_cast(total_beds_7_day_avg as float64) as total_beds_7_day_avg,
  safe_cast(inpatient_beds_used_7_day_avg as float64) as inpatient_beds_used_7_day_avg,
  safe_cast(total_icu_beds_7_day_avg as float64) as total_icu_beds_7_day_avg,
  safe_cast(icu_beds_used_7_day_avg as float64) as icu_beds_used_7_day_avg,
  safe_cast(total_adult_patients_hospitalized_confirmed_covid_7_day_avg as float64) as adult_covid_patients_7_day_avg,
  safe_cast(total_patients_hospitalized_confirmed_influenza_7_day_avg as float64) as influenza_patients_7_day_avg
from `bronze.raw_hospital_capacity`
where _hospital_pk is not null;
