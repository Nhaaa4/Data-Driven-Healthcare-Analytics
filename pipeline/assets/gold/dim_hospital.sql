/* @bruin

name: gold.dim_hospital
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table
  cluster_by:
    - hospital_key

depends:
  - silver.hospital_capacity
  - silver.hospital_placekey
  - silver.healthcare_admissions

columns:
  - name: hospital_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: hospital_name
    type: string
    checks:
      - name: not_null

custom_checks:
  - name: table has rows
    value: 1
    query: select count(*) > 0 from `gold.dim_hospital`

@bruin */

with capacity_hospitals as (
  select
    cast(hc.hospital_pk as string) as hospital_pk,
    nullif(trim(hc.hospital_name), '') as hospital_name,
    nullif(trim(hc.hospital_subtype), '') as hospital_subtype,
    nullif(trim(hp.placekey), '') as placekey,
    nullif(trim(hc.hospital_address), '') as hospital_address,
    nullif(trim(hc.city), '') as city,
    upper(nullif(trim(hc.state), '')) as state,
    nullif(trim(hc.zip_code), '') as zip_code,
    nullif(trim(hc.fips_code), '') as fips_code,
    nullif(trim(hc.metro_micro_area), '') as metro_micro_area,
    hc.collection_week
  from `silver.hospital_capacity` as hc
  left join `silver.hospital_placekey` as hp
    on hc.hospital_pk = hp.hospital_pk
  where nullif(trim(hc.hospital_name), '') is not null
     or hc.hospital_pk is not null
),
capacity_ranked as (
  select
    *,
    row_number() over (
      partition by hospital_pk
      order by collection_week desc nulls last, hospital_name
    ) as hospital_rank
  from capacity_hospitals
  where hospital_pk is not null
),
capacity_latest as (
  select * except(hospital_rank)
  from capacity_ranked
  where hospital_rank = 1
),
admission_hospitals as (
  select
    cast(null as string) as hospital_pk,
    nullif(trim(hospital_name), '') as hospital_name,
    cast(null as string) as hospital_subtype,
    cast(null as string) as placekey,
    cast(null as string) as hospital_address,
    cast(null as string) as city,
    cast(null as string) as state,
    cast(null as string) as zip_code,
    cast(null as string) as fips_code,
    cast(null as string) as metro_micro_area,
    cast(null as date) as collection_week
  from `silver.healthcare_admissions`
  where nullif(trim(hospital_name), '') is not null
  group by hospital_name
),
combined as (
  select * from capacity_latest
  union all
  select * from admission_hospitals
),
ranked as (
  select
    *,
    lower(trim(hospital_name)) as hospital_name_key,
    row_number() over (
      partition by lower(trim(hospital_name))
      order by hospital_pk is not null desc, collection_week desc nulls last
    ) as name_rank
  from combined
  where hospital_name is not null
)
select
  case
    when hospital_pk is not null then to_hex(md5(concat('pk|', hospital_pk)))
    else to_hex(md5(concat('name|', hospital_name_key)))
  end as hospital_key,
  hospital_pk,
  hospital_name,
  hospital_subtype,
  placekey,
  hospital_address,
  city,
  state,
  zip_code,
  fips_code,
  metro_micro_area,
  hospital_name_key,
  case
    when hospital_pk is not null then 'hospital_capacity'
    else 'healthcare_admissions'
  end as source_system
from ranked
where name_rank = 1;
