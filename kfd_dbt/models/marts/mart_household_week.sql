-- Grain: one row per household per week (Monday start), including weeks with no data.
with bounds as (
    select hh_ids,
           date_trunc('week', min(trx_date))::date as first_week,
           date_trunc('week', max(trx_date))::date as last_week
    from {{ source('clean', 'transactions') }}
    group by hh_ids
),

spine as (
    select b.hh_ids, gs::date as week_start
    from bounds b
    cross join lateral generate_series(b.first_week, b.last_week, interval '1 week') as gs
),

agg as (
    select
        hh_ids,
        date_trunc('week', trx_date)::date as week_start,
        sum(signed_kes)                                                          as net_kes,
        sum(value_kes) filter (where trx_class_code = 'income' and trx_direction = 'Inflow')  as income_in,
        sum(value_kes) filter (where trx_class_code = 'income' and trx_direction = 'Outflow') as business_costs,
        sum(value_kes) filter (where trx_class_code = 'opex')                    as opex_out,
        count(*)                                                                 as n_rows
    from {{ source('clean', 'transactions') }}
    group by 1, 2
)

select
    s.hh_ids,
    s.week_start,
    coalesce(a.n_rows, 0)         as n_rows,
    a.net_kes,
    coalesce(a.income_in, 0)      as income_in,
    coalesce(a.business_costs, 0) as business_costs,
    coalesce(a.opex_out, 0)       as opex_out,
    case
        when coalesce(a.n_rows, 0) = 0                          then 'no_data'
        when a.n_rows < {{ var('thin_week_min_rows') }}         then 'thin'
        else 'ok'
    end as status
from spine s
left join agg a using (hh_ids, week_start)