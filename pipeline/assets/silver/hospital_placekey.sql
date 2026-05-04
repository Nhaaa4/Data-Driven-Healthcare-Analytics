/* @bruin

name: silver.hospital_placekey
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table

depends:
  - bronze.raw_hospital_placekey

columns:
  - name: hospital_pk
    type: string
    checks:
      - name: not_null
      - name: unique

@bruin */

select
  cast(hospital_pk as string) as hospital_pk,
  nullif(trim(hospital_name), '') as hospital_name,
  nullif(trim(placekey), '') as placekey
from `bronze.raw_hospital_placekey`
where hospital_pk is not null;
