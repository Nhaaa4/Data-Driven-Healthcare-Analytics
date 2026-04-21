/* @bruin

name: silver.diagnoses
type: bq.sql
connection: healthcare_bq
depends:
  - bronze.mimic_diagnoses

materialization:
  type: table

columns:
  - name: diagnosis_key
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: icd9_code
    type: string
    checks:
      - name: not_null

@bruin */

select
  GENERATE_UUID() AS diagnosis_key,
  icd9_code,
  regexp_extract(icd9_code, r'^[A-Z]?[0-9]+') as diagnosis_code_family,
  regexp_contains(icd9_code, r'^[0-9]+(\.[0-9]+)?$') as is_numeric_code,
  hadm_id,
  subject_id,
  seq_num
from `bronze.mimic_diagnoses`
where icd9_code is not null
  and trim(icd9_code) != '';
