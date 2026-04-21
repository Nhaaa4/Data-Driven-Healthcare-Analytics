/* @bruin

name: silver.labevents
type: bq.sql
connection: healthcare_bq
depends:
  - bronze.mimic_labevents

materialization:
  type: table

columns:
  - name: lab_result_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: subject_id
    type: integer
    checks:
      - name: not_null
  - name: chart_date
    type: date
    checks:
      - name: not_null

@bruin */

select
  cast(row_id as string) as lab_result_key,
  row_id,
  subject_id,
  hadm_id,
  itemid,
  charttime,
  cast(charttime as date) as chart_date,
  nullif(trim(value), '') as value_text,
  valuenum,
  nullif(trim(valueuom), '') as value_uom,
  upper(coalesce(nullif(trim(flag), ''), 'NORMAL')) as flag,
  upper(coalesce(nullif(trim(flag), ''), 'NORMAL')) != 'NORMAL' as is_abnormal,
  valuenum is not null as is_numeric_result
from `bronze.mimic_labevents`
where subject_id is not null
  and itemid is not null
  and charttime is not null
  and (valuenum is null or abs(valuenum) <= 1000000);
