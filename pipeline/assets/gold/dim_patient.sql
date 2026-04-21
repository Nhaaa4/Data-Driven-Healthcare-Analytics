/* @bruin

name: gold.dim_patient
type: bq.sql
connection: healthcare_bq
depends:
  - silver.patients
materialization:
  type: table
@bruin */

select
  abs(farm_fingerprint(cast(subject_id as string))) as patient_key,
  subject_id,
  gender,
  date_of_birth,
  date_of_death,
  is_deceased
from `silver.patients`;
