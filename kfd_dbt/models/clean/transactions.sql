-- Grain: one row per reported income/opex transaction. Duplicates are kept and flagged (most are real repeat purchases).
{{ config(materialized='table', post_hook="alter table {{ this }} add primary key (trx_key)") }}

with base as (
    select
        *,
        count(*)     over w as dup_n,
        row_number() over (w order by row_id) as occ
    from {{ ref('stg_kfd') }}
    where in_scope and reject_reason is null and trx_class_code in ('income', 'opex')
    window w as (partition by hh_ids, trx_date, trx_class_code, trx_type_desc, trx_direction, value_kes, account_ids)
)

select
    -- natural key + occurrence number, so it is unique by construction
    md5(concat_ws('|', hh_ids, trx_date::text, trx_class_code, coalesce(trx_type_desc, ''), trx_direction,
                  value_kes::text, coalesce(account_ids, ''), occ::text)) as trx_key,
    row_id,
    hh_ids,
    region,
    trx_date,
    int_date,
    trx_class_code,
    trx_type_code,
    trx_type_desc,
    trx_family_code,
    trx_direction,
    value_kes,
    case when trx_direction = 'Inflow' then value_kes else -value_kes end as signed_kes,
    account_ids,
    interview_designation,
    dup_n > 1 as is_dup_key
from base
