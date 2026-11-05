-- Baseline = each household's own mean and std dev of net over its 'ok' weeks (MVP: full history).
with base as (
    select hh_ids,
           avg(net_kes)         as baseline_mean,
           stddev_samp(net_kes) as baseline_sd,
           count(*)             as n_ok_weeks
    from {{ ref('mart_household_week') }}
    where status = 'ok'
    group by hh_ids
)

select
    w.hh_ids,
    d.region,
    w.week_start,
    w.status,
    w.n_rows,
    w.net_kes,
    w.income_in,
    w.business_costs,
    w.opex_out,
    b.baseline_mean,
    b.baseline_sd,
    b.n_ok_weeks,
    case when b.baseline_sd > 0 then (w.net_kes - b.baseline_mean) / b.baseline_sd end as z_score,
    (   w.status = 'ok'
    and b.n_ok_weeks >= {{ var('min_ok_weeks') }}
    and b.baseline_sd > 0
    and w.net_kes < b.baseline_mean - {{ var('flag_k') }} * b.baseline_sd
    ) as is_flagged
from {{ ref('mart_household_week') }} w
left join base b using (hh_ids)
left join {{ ref('dim_household') }} d using (hh_ids)