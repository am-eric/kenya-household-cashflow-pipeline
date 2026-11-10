-- Grain: one row per flagged household-week.
-- Question: after a week of unusually low net cash flow, does the household borrow, buy on credit or draw down savings?
-- Coping = findev Inflow events (cash received) of the types listed in seeds/coping_types.csv, in the
-- coping_window_days after the flagged week. Compared with that household's own typical amount per window.
with flagged as (
    select hh_ids, region, week_start, net_kes, z_score,
           week_start + 7                                              as window_start,
           week_start + 7 + {{ var('coping_window_days') }}            as window_end
    from {{ ref('mart_household_week_flags') }}
    where is_flagged
),

coping_events as (
    select e.hh_ids, e.trx_date, e.value_kes, c.coping_kind
    from {{ ref('findev_events') }} e
    join {{ ref('coping_types') }} c on c.trx_type_code = e.trx_type_code
    where e.trx_direction = 'Inflow'
),

after_flag as (
    select
        f.hh_ids, f.week_start,
        count(*)             filter (where e.coping_kind = 'borrowing')         as n_borrowing,
        coalesce(sum(e.value_kes) filter (where e.coping_kind = 'borrowing'), 0)        as borrowing_kes,
        count(*)             filter (where e.coping_kind = 'credit_goods')      as n_credit_goods,
        coalesce(sum(e.value_kes) filter (where e.coping_kind = 'credit_goods'), 0)     as credit_goods_kes,
        count(*)             filter (where e.coping_kind = 'savings_drawdown')  as n_savings_drawdown,
        coalesce(sum(e.value_kes) filter (where e.coping_kind = 'savings_drawdown'), 0) as savings_drawdown_kes
    from flagged f
    left join coping_events e
           on e.hh_ids = f.hh_ids and e.trx_date >= f.window_start and e.trx_date < f.window_end
    group by 1, 2
),

household_baseline as (
    -- typical coping per window = all of the household's coping KES / number of windows it was observed for
    select d.hh_ids,
           coalesce(sum(e.value_kes), 0)
               / greatest((d.last_trx_date - d.first_trx_date + 1)::numeric / {{ var('coping_window_days') }}, 1)
               as baseline_coping_kes_per_window,
           d.last_trx_date
    from {{ ref('dim_household') }} d
    left join coping_events e using (hh_ids)
    group by d.hh_ids, d.first_trx_date, d.last_trx_date
)

select
    f.hh_ids,
    f.region,
    f.week_start,
    f.net_kes,
    f.z_score,
    f.window_start,
    f.window_end,
    a.n_borrowing,
    a.borrowing_kes,
    a.n_credit_goods,
    a.credit_goods_kes,
    a.n_savings_drawdown,
    a.savings_drawdown_kes,
    a.borrowing_kes + a.credit_goods_kes + a.savings_drawdown_kes                       as coping_kes_total,
    (a.n_borrowing + a.n_credit_goods + a.n_savings_drawdown) > 0                       as any_coping,
    round(b.baseline_coping_kes_per_window)                                             as baseline_coping_kes_per_window,
    case when b.baseline_coping_kes_per_window > 0
         then round((a.borrowing_kes + a.credit_goods_kes + a.savings_drawdown_kes) / b.baseline_coping_kes_per_window, 2)
    end                                                                                 as coping_vs_baseline,
    -- false when the window runs past the household's last transaction, so "no coping" may just mean "not observed"
    f.window_end <= b.last_trx_date + 1                                                 as window_observed
from flagged f
join after_flag a using (hh_ids, week_start)
join household_baseline b using (hh_ids)
