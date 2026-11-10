-- Typed, trimmed version of raw.kfd_raw. One row per raw row, nothing dropped here:
-- rows that cannot be used get a reject_reason, and in_scope says whether they belong in the clean tables.
{{ config(materialized='table') }}

with src as (
    select
        row_id,
        nullif(trim(hh_ids), '')                       as hh_ids,
        nullif(trim(trx_date), '')                     as trx_date_raw,
        nullif(trim(int_date), '')                     as int_date_raw,
        lower(nullif(trim(trx_class_code), ''))        as trx_class_code,
        initcap(lower(nullif(trim(trx_direction), ''))) as trx_direction,
        nullif(trim(trx_value_kes), '')                as trx_value_raw,
        nullif(trim(trx_mode_code), '')                as trx_mode_code,
        nullif(trim(trx_type_code), '')                as trx_type_code,
        nullif(trim(trx_type_desc), '')                as trx_type_desc,
        nullif(trim(trx_family_code), '')              as trx_family_code,
        nullif(trim(account_ids), '')                  as account_ids,
        nullif(trim(interview_designation), '')        as interview_designation
    from {{ source('raw', 'kfd_raw') }}
),

typed as (
    select
        *,
        -- the file mixes 29jul2013 and 29-Jul-13
        {{ parse_kfd_date('trx_date_raw') }}   as trx_date,
        {{ parse_kfd_date('int_date_raw') }}   as int_date,
        case when trx_value_raw ~ '^-?[0-9]+(\.[0-9]+)?$' then trx_value_raw::numeric end as value_kes,
        -- region comes from the 3-letter household prefix
        case left(hh_ids, 3)
            when 'KEL' then 'Eldoret'
            when 'KMO' then 'Mombasa'
            when 'KVI' then 'Vihiga'
            when 'KMA' then 'Makueni'
            when 'KNB' then 'Nairobi'
            else 'UNKNOWN'
        end as region
    from src
),

scoped as (
    select
        *,
        -- income/opex = cash flows; findev = real device events (balance rows, mode 18/19, and directionless rows are out);
        -- cash/assets are snapshots and out of scope.
        -- KZI households (3 households, 3,131 raw rows) are not one of the five study regions, so they are dropped.
        (   coalesce(left(hh_ids, 3), '') <> 'KZI'
        and (   trx_class_code in ('income', 'opex')
             or (trx_class_code = 'findev'
                 and coalesce(trx_mode_code, '') not in ('18', '19')
                 and trx_direction is not null))
        ) as in_scope
    from typed
)

select
    *,
    case
        when hh_ids is null                                 then 'missing_household'
        when trx_date is null                               then 'bad_date'
        when value_kes is null or value_kes <= 0            then 'bad_value'
        when trx_direction not in ('Inflow', 'Outflow')
             or trx_direction is null                       then 'bad_direction'
        when region = 'UNKNOWN'                             then 'unknown_region'
    end as reject_reason
from scoped
