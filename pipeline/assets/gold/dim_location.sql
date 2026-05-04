/* @bruin

name: gold.dim_location
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table
  cluster_by:
    - location_key

depends:
  - silver.hospital_capacity

columns:
  - name: location_key
    type: string
    checks:
      - name: not_null
      - name: unique

custom_checks:
  - name: table has rows
    value: 1
    query: select count(*) > 0 from `gold.dim_location`

@bruin */

with locations as (
  select
    upper(nullif(trim(state), '')) as state,
    lower(nullif(trim(city), '')) as city,
    nullif(trim(zip_code), '') as zip_code,
    nullif(trim(fips_code), '') as fips_code,
    any_value(metro_micro_area) as metro_micro_area
  from `silver.hospital_capacity`
  where state is not null
     or city is not null
     or zip_code is not null
  group by 1, 2, 3, 4
)
select
  to_hex(md5(concat(
    coalesce(upper(state), ''),
    '|',
    coalesce(lower(city), ''),
    '|',
    coalesce(zip_code, ''),
    '|',
    coalesce(fips_code, '')
  ))) as location_key,
  state,
  city,
  zip_code,
  fips_code,
  metro_micro_area
from locations;
