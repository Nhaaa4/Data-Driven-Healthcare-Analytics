/* @bruin

name: gold.dim_insurance
type: bq.sql
connection: healthcare_gcp

materialization:
  type: table
  cluster_by:
    - insurance_key

depends:
  - silver.healthcare_admissions

columns:
  - name: insurance_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: insurance_provider
    type: string
    checks:
      - name: not_null

custom_checks:
  - name: table has rows
    value: 1
    query: select count(*) > 0 from `gold.dim_insurance`

@bruin */

with providers as (
  select
    lower(nullif(trim(insurance_provider), '')) as insurance_provider
  from `silver.healthcare_admissions`
)
select
  to_hex(md5(coalesce(insurance_provider, 'unknown'))) as insurance_key,
  coalesce(insurance_provider, 'unknown') as insurance_provider
from providers
where insurance_provider is not null
group by 1, 2;
