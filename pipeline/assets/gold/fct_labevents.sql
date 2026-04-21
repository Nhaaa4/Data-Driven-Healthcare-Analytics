/* @bruin

name: gold.fct_labevents
type: bq.sql
connection: healthcare_bq
depends:
  - silver.labevents
  - gold.dim_patient

@bruin */

create or replace table `gold.fct_labevents`
partition by chart_date
cluster by patient_key, lab_item_key as
select
  abs(farm_fingerprint(l.lab_result_key)) as lab_result_fact_key,
  l.row_id,
  abs(farm_fingerprint(cast(l.subject_id as string))) as patient_key,
  abs(farm_fingerprint(cast(l.itemid as string))) as lab_item_key,
  cast(format_date('%Y%m%d', l.chart_date) as int64) as chart_date_key,
  l.chart_date,
  l.hadm_id,
  l.valuenum,
  l.value_text,
  l.value_uom,
  l.flag,
  l.is_abnormal,
  l.is_numeric_result
from `silver.labevents` as l;
