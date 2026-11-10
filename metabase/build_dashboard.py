"""Create (or update) the KFD dashboard in Metabase through its API.

Run from the repo root: python metabase/build_dashboard.py
Needs METABASE_API_KEY in .env (an Administrators-group API key) and Metabase at http://localhost:3000.
Safe to re-run: collection, cards and dashboard are found by name and updated in place.
"""
import json
import os
import sys
import urllib.error
import urllib.request
import uuid

from dotenv import dotenv_values

env = {**dotenv_values('.env'), **os.environ}
BASE = env.get('METABASE_URL', 'http://localhost:3000') + '/api'
KEY = env.get('METABASE_API_KEY') or sys.exit('METABASE_API_KEY missing from .env')
DB_NAME = 'kfd'
COLLECTION = 'KFD Cash-Flow Shocks'
DASHBOARD = 'KFD Household Cash-Flow Shocks'


def api(method, path, body=None):
    req = urllib.request.Request(
        BASE + path, method=method, headers={'x-api-key': KEY, 'Content-Type': 'application/json'},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        return json.load(urllib.request.urlopen(req, timeout=60))
    except urllib.error.HTTPError as e:
        sys.exit(f'{method} {path} -> HTTP {e.code}: {e.read()[:500].decode(errors="replace")}')


REGION_TAG = {'region': {'id': str(uuid.uuid5(uuid.NAMESPACE_DNS, 'kfd-region')), 'name': 'region',
                         'display-name': 'Region', 'type': 'text'}}
# Each card: native SQL against the clean/marts schemas. {{region}} is wired to the dashboard's Region filter.
CARDS = [
    dict(name='Households in the study', display='scalar', region=False,
         sql='select count(distinct hh_ids) as households from clean.transactions'),
    dict(name='Transactions cleaned', display='scalar', region=False,
         sql='select count(*) as transactions from clean.transactions'),
    dict(name='Flagged household-days', display='scalar', region=True,
         sql='select count(*) as flagged_days from marts.mart_household_day_flags\n'
             'where is_flagged [[and region = {{region}}]]'),
    dict(name='Average daily net cash flow per household (KES)', display='line', region=True,
         sql='select day, round(avg(net_kes)) as avg_net_kes\n'
             'from marts.mart_household_day_flags\n'
             'where net_kes is not null [[and region = {{region}}]]\n'
             'group by 1 order by 1',
         viz={'graph.dimensions': ['day'], 'graph.metrics': ['avg_net_kes']}),
    dict(name='Flagged household-days per month, by region', display='bar', region=True,
         sql="select date_trunc('month', day)::date as month, region, count(*) as flagged_days\n"
             'from marts.mart_household_day_flags\n'
             'where is_flagged [[and region = {{region}}]]\n'
             'group by 1, 2 order by 1, 2',
         viz={'graph.dimensions': ['month', 'region'], 'graph.metrics': ['flagged_days'],
              'stackable.stack_type': 'stacked'}),
    dict(name='Inflows vs outflows by region (KES)', display='bar', region=False,
         sql="select region,\n"
             "       sum(value_kes) filter (where trx_direction = 'Inflow')  as inflow_kes,\n"
             "       sum(value_kes) filter (where trx_direction = 'Outflow') as outflow_kes\n"
             'from clean.transactions group by 1 order by 1',
         viz={'graph.dimensions': ['region'], 'graph.metrics': ['inflow_kes', 'outflow_kes']}),
    dict(name='Household spending by category (KES, top 10)', display='row', region=True,
         sql="select trim(regexp_replace(trx_type_desc, '\\s*\\(.*$', '')) as category,\n"
             '       sum(value_kes) as spent_kes\n'
             'from clean.transactions\n'
             "where trx_class_code = 'opex' [[and region = {{region}}]]\n"
             'group by 1 order by 2 desc limit 10',
         viz={'graph.dimensions': ['category'], 'graph.metrics': ['spent_kes']}),
    dict(name='Biggest drops: 20 most unusual days', display='table', region=True,
         sql='select hh_ids as household, region, day, net_kes,\n'
             '       round(baseline_mean) as usual_daily_net_kes, round(z_score::numeric, 2) as z_score, n_rows\n'
             'from marts.mart_household_day_flags\n'
             'where is_flagged [[and region = {{region}}]]\n'
             'order by z_score asc limit 20'),
    dict(name='Flagged weeks followed by coping (%)', display='scalar', region=True,
         sql='select round(100.0 * avg(any_coping::int), 1) as pct_followed_by_coping\n'
             'from marts.mart_coping_after_flag\n'
             'where window_observed [[and region = {{region}}]]'),
    dict(name='Median coping after a flagged week, as a multiple of the household usual', display='scalar', region=True,
         sql='select round((percentile_cont(0.5) within group (order by coping_vs_baseline))::numeric, 2) as median_vs_usual\n'
             'from marts.mart_coping_after_flag\n'
             'where window_observed and coping_vs_baseline is not null [[and region = {{region}}]]'),
    dict(name='Flagged weeks with a fully observed coping window', display='scalar', region=True,
         sql='select count(*) as flagged_weeks\n'
             'from marts.mart_coping_after_flag\n'
             'where window_observed [[and region = {{region}}]]'),
    dict(name='Average coping in the 28 days after a flagged week, by region (KES)', display='bar', region=False,
         sql='select region,\n'
             '       round(avg(borrowing_kes))        as borrowing_kes,\n'
             '       round(avg(credit_goods_kes))     as credit_goods_kes,\n'
             '       round(avg(savings_drawdown_kes)) as savings_drawdown_kes\n'
             'from marts.mart_coping_after_flag\n'
             'where window_observed\n'
             'group by 1 order by 1',
         viz={'graph.dimensions': ['region'],
              'graph.metrics': ['borrowing_kes', 'credit_goods_kes', 'savings_drawdown_kes'],
              'stackable.stack_type': 'stacked'}),
    dict(name='Flagged weeks with the most coping afterwards', display='table', region=True,
         sql='select hh_ids as household, region, week_start, net_kes,\n'
             '       borrowing_kes, credit_goods_kes, savings_drawdown_kes, coping_kes_total,\n'
             '       baseline_coping_kes_per_window as usual_coping_kes, coping_vs_baseline\n'
             'from marts.mart_coping_after_flag\n'
             'where window_observed [[and region = {{region}}]]\n'
             'order by coping_kes_total desc limit 20'),
]
# (card name, col, row, width, height) on Metabase's 24-column grid
LAYOUT = [
    ('Households in the study', 0, 0, 6, 3),
    ('Transactions cleaned', 6, 0, 6, 3),
    ('Flagged household-days', 12, 0, 12, 3),
    ('Average daily net cash flow per household (KES)', 0, 3, 24, 7),
    ('Flagged household-days per month, by region', 0, 10, 14, 7),
    ('Inflows vs outflows by region (KES)', 14, 10, 10, 7),
    ('Household spending by category (KES, top 10)', 0, 17, 24, 8),
    ('Biggest drops: 20 most unusual days', 0, 25, 24, 8),
    ('Flagged weeks followed by coping (%)', 0, 33, 8, 3),
    ('Median coping after a flagged week, as a multiple of the household usual', 8, 33, 8, 3),
    ('Flagged weeks with a fully observed coping window', 16, 33, 8, 3),
    ('Average coping in the 28 days after a flagged week, by region (KES)', 0, 36, 24, 7),
    ('Flagged weeks with the most coping afterwards', 0, 43, 24, 8),
]

db_id = next(d['id'] for d in api('GET', '/database')['data'] if d['name'] == DB_NAME)

collection = next((c for c in api('GET', '/collection') if c['name'] == COLLECTION), None) \
    or api('POST', '/collection', {'name': COLLECTION})
existing = {i['name']: i['id'] for i in api('GET', f"/collection/{collection['id']}/items")['data']
            if i['model'] == 'card'}

card_ids = {}
for c in CARDS:
    body = {
        'name': c['name'], 'display': c['display'], 'collection_id': collection['id'],
        'visualization_settings': c.get('viz', {}),
        'dataset_query': {'type': 'native', 'database': db_id,
                          'native': {'query': c['sql'], 'template-tags': REGION_TAG if c['region'] else {}}},
    }
    if c['name'] in existing:
        card_ids[c['name']] = api('PUT', f"/card/{existing[c['name']]}", body)['id']
    else:
        card_ids[c['name']] = api('POST', '/card', body)['id']
    print('card', card_ids[c['name']], c['name'])

# cards from an earlier version of this script (e.g. the weekly ones) are archived, not left behind
for name, card_id in existing.items():
    if name not in card_ids:
        api('PUT', f'/card/{card_id}', {'archived': True})
        print('archived', card_id, name)

by_name = {c['name']: c for c in CARDS}
dashcards = []
for i, (name, col, row, w, h) in enumerate(LAYOUT):
    dashcards.append({
        'id': -(i + 1), 'card_id': card_ids[name], 'col': col, 'row': row, 'size_x': w, 'size_y': h,
        'visualization_settings': {},
        'parameter_mappings': [{'parameter_id': 'region', 'card_id': card_ids[name],
                                'target': ['variable', ['template-tag', 'region']]}] if by_name[name]['region'] else [],
    })
parameters = [{'id': 'region', 'name': 'Region', 'slug': 'region', 'type': 'string/=', 'sectionId': 'string'}]

dash = next((d for d in api('GET', f"/collection/{collection['id']}/items")['data'] if d['model'] == 'dashboard'
             and d['name'] == DASHBOARD), None)
if dash is None:
    dash = api('POST', '/dashboard', {
        'name': DASHBOARD, 'collection_id': collection['id'],
        'description': 'Daily household net cash flow, regional inflows and outflows, and days flagged as unusually low.'})
api('PUT', f"/dashboard/{dash['id']}", {'dashcards': dashcards, 'parameters': parameters})
print(f"dashboard: {BASE.removesuffix('/api')}/dashboard/{dash['id']}")
