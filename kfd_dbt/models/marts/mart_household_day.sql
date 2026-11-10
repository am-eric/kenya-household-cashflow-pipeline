-- Grain: one row per household per calendar day between its first and last transaction, including days with no data.
with bounds as (
    select hh_ids, min(trx_date) as first_day, max(trx_date) as last_day
    from {{ ref('transactions') }}
    group by hh_ids
),

spine as (
    select b.hh_ids, gs::date as day
    from bounds b
    cross join lateral generate_series(b.first_day, b.last_day, interval '1 day') as gs
),

agg as (
    select
        hh_ids,
        trx_date                                                                              as day,
        sum(signed_kes)                                                                       as net_kes,
        sum(value_kes) filter (where trx_class_code = 'income' and trx_direction = 'Inflow')  as income_in,
        sum(value_kes) filter (where trx_class_code = 'income' and trx_direction = 'Outflow') as business_costs,
        sum(value_kes) filter (where trx_class_code = 'opex')                                 as opex_out,
        count(*)                                                                              as n_rows
    from {{ ref('transactions') }}
    group by 1, 2
)

select
    s.hh_ids,
    s.day,
    coalesce(a.n_rows, 0)         as n_rows,
    a.net_kes,
    coalesce(a.income_in, 0)      as income_in,
    coalesce(a.business_costs, 0) as business_costs,
    coalesce(a.opex_out, 0)       as opex_out,
    case when a.n_rows is null then 'no_data' else 'ok' end as status
from spine s
left join agg a using (hh_ids, day)
