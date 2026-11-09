-- Real financial-device events (borrowing, lending, savings). Balance snapshots are removed in staging.
select
    row_id,
    hh_ids,
    region,
    trx_date,
    int_date,
    trx_type_code,
    trx_type_desc,
    trx_family_code,
    trx_direction,
    value_kes,
    case when trx_direction = 'Inflow' then value_kes else -value_kes end as signed_kes,
    account_ids,
    interview_designation
from {{ ref('stg_kfd') }}
where in_scope and reject_reason is null and trx_class_code = 'findev'
