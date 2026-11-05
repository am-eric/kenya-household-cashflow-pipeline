-- One row per household with its region.
select
    hh_ids,
    min(region)    as region,
    min(trx_date)  as first_trx_date,
    max(trx_date)  as last_trx_date
from {{ source('clean', 'transactions') }}
group by hh_ids
