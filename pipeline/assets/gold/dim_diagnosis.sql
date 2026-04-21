/* @bruin

name: gold.dim_diagnosis
type: bq.sql
connection: healthcare_bq
depends:
  - silver.diagnoses

materialization:
  type: table

@bruin */

select
  abs(farm_fingerprint(diagnosis_key)) as diagnosis_key,
  icd9_code as diagnosis_code,
  diagnosis_code_family,
  is_numeric_code
from `silver.diagnoses`;
