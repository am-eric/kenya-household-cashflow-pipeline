-- In-scope rows that failed a cleaning rule. Saved with the reason, never silently lost.
select
    row_id,
    hh_ids,
    trx_class_code,
    trx_date_raw,
    trx_value_raw,
    region,
    reject_reason
from {{ ref('stg_kfd') }}
where in_scope and reject_reason is not null
