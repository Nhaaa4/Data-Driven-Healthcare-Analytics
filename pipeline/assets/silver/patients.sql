/* @bruin

name: silver.patients
type: bq.sql
connection: healthcare_bq
depends:
  - bronze.mimic_patients

materialization:
  type: table

columns:
  - name: subject_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: date_of_birth
    type: date
    checks:
      - name: not_null
  - name: gender
    type: string
  - name: is_deceased
    type: boolean

@bruin */

with ranked_patients as (
  select
    subject_id,
    upper(trim(gender)) as gender,
    cast(dob as date) as date_of_birth,
    cast(dod as date) as date_of_death,
    cast(expire_flag as bool) as is_deceased,
    row_number() over (
      partition by subject_id
      order by coalesce(dod, dob) desc, row_id desc
    ) as rn
  from `bronze.mimic_patients`
  where subject_id is not null
    and dob is not null
)
select
  subject_id,
  case
    when gender in ('M', 'F') then gender
    else 'UNKNOWN'
  end as gender,
  date_of_birth,
  date_of_death,
  is_deceased
from ranked_patients
where rn = 1
  and date_of_birth <= current_date()
  and (date_of_death is null or date_of_death >= date_of_birth);
