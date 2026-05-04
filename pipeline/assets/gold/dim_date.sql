/* @bruin

name: gold.dim_date
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table

depends:
  - silver.healthcare_admissions
  - silver.hospital_capacity
  - silver.vitals_sign

columns:
  - name: date_key
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: date_day
    type: date
    checks:
      - name: not_null
      - name: unique

custom_checks:
  - name: table has rows
    value: 1
    query: select count(*) > 0 from `gold.dim_date`

@bruin */

with source_dates as (
  select admission_date as date_day from `silver.healthcare_admissions`
  union distinct select discharge_date from `silver.healthcare_admissions`
  union distinct select collection_week from `silver.hospital_capacity`
  union distinct select event_date from `silver.vitals_sign`
),
bounds as (
  select
    coalesce(min(date_day), date '2020-01-01') as min_date,
    coalesce(max(date_day), current_date()) as max_date
  from source_dates
  where date_day is not null
)
select
  cast(format_date('%Y%m%d', date_day) as int64) as date_key,
  date_day,
  extract(year from date_day) as year,
  extract(quarter from date_day) as quarter,
  extract(month from date_day) as month,
  format_date('%B', date_day) as month_name,
  date_trunc(date_day, month) as month_start_date,
  date_trunc(date_day, week(monday)) as week_start_date,
  extract(dayofweek from date_day) as day_of_week,
  format_date('%A', date_day) as day_name,
  date_day in unnest([
    date(extract(year from date_day), 1, 1),
    date(extract(year from date_day), 7, 4),
    date(extract(year from date_day), 12, 25)
  ]) as is_us_fixed_holiday
from bounds,
unnest(generate_date_array(min_date, max_date)) as date_day;
