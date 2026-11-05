import hashlib
import pandas as pd

REGION_MAP = {'KEL': 'Eldoret', 'KMO': 'Mombasa', 'KVI': 'Vihiga', 'KMA': 'Makueni', 'KNB': 'Nairobi'}

# ---------- 1. LOOK AT THE DATA ----------
df = pd.read_csv('data/raw/kfd.csv', dtype=str)   # everything as text

print(df.head())
print(df.shape)
print(df.columns)
print(df.info())

print("------------------------------------------")
key_cols = ['hh_ids', 'trx_date', 'trx_class_code', 'trx_direction', 'trx_value_kes']
print(df[key_cols].isna().sum())

print(df['trx_class_code'].value_counts(dropna=False))
print(df.groupby(['trx_class_code', 'trx_direction'], dropna=False).size())
print(df['interview_designation'].value_counts(dropna=False))
print(df['hh_ids'].nunique())

# ---------- 2. CLEAN ----------
df_clean = df.copy()
df_clean['row_id'] = range(1, len(df_clean) + 1)      # line number in the raw file

# text columns: trim spaces, standard case
for col in ['hh_ids', 'trx_class_code', 'trx_direction', 'trx_mode_code']:
    df_clean[col] = df_clean[col].str.strip()
df_clean['trx_class_code'] = df_clean['trx_class_code'].str.lower()
df_clean['trx_direction'] = df_clean['trx_direction'].str.capitalize()

# dates: the file mixes 29jul2013 and 29-Jul-13
def parse_date(s):
    d = pd.to_datetime(s, format='%d%b%Y', errors='coerce')
    d = d.fillna(pd.to_datetime(s, format='%d-%b-%y', errors='coerce'))
    return d

df_clean['trx_date'] = parse_date(df_clean['trx_date'])
df_clean['int_date'] = parse_date(df_clean['int_date'])
print("bad trx_date:", df_clean['trx_date'].isna().sum())

# amount
df_clean['value_kes'] = pd.to_numeric(df_clean['trx_value_kes'], errors='coerce')
print(df_clean[df_clean['value_kes'] <= 0].shape)

# region from household id prefix
df_clean['region'] = df_clean['hh_ids'].str[:3].map(REGION_MAP).fillna('UNKNOWN')
print(df_clean['region'].value_counts())

# ---------- 3. SPLIT: flows vs financial-device events ----------
flows = df_clean[df_clean['trx_class_code'].isin(['income', 'opex'])].copy()
findev = df_clean[df_clean['trx_class_code'] == 'findev'].copy()
# cash and assets rows are snapshots / out of scope, so they are left out

# findev: drop starting/closing balance rows (mode code 18/19) and rows with no direction
findev = findev[~findev['trx_mode_code'].isin(['18', '19']) & findev['trx_direction'].notna()]

# ---------- 4. REJECT BAD ROWS (saved, not silently lost) ----------
def bad_rows(d):
    return (d['hh_ids'].isna() | d['trx_date'].isna() | d['value_kes'].isna()
            | (d['value_kes'] <= 0) | ~d['trx_direction'].isin(['Inflow', 'Outflow']))

rejected = flows[bad_rows(flows)]
flows = flows[~bad_rows(flows)].copy()
findev = findev[~bad_rows(findev)].copy()
print("rejected:", len(rejected), "| flows:", len(flows), "| findev:", len(findev))

# ---------- 5. DERIVED COLUMNS ----------
flows['signed_kes'] = flows['value_kes'].where(flows['trx_direction'] == 'Inflow', -flows['value_kes'])
findev['signed_kes'] = findev['value_kes'].where(findev['trx_direction'] == 'Inflow', -findev['value_kes'])

# duplicates: KEEP and FLAG (most are real repeat purchases, e.g. KES 9 water twice a day)
natural_key = ['hh_ids', 'trx_date', 'trx_class_code', 'trx_type_desc',
               'trx_direction', 'value_kes', 'account_ids']
flows['dup_n'] = flows.groupby(natural_key, dropna=False)['row_id'].transform('size')
flows['is_dup_key'] = flows['dup_n'] > 1
print("rows sharing a natural key:", flows['is_dup_key'].sum())

# unique key = natural key + occurrence number
flows['occ'] = flows.groupby(natural_key, dropna=False).cumcount() + 1
key_text = (flows[natural_key].astype(object).fillna('').astype(str).agg('|'.join, axis=1)
            + '|' + flows['occ'].astype(str))
flows['trx_key'] = key_text.map(lambda s: hashlib.md5(s.encode()).hexdigest())
print("trx_key unique:", flows['trx_key'].is_unique)

# ---------- 6. SAVE ----------
flow_cols = ['trx_key', 'row_id', 'hh_ids', 'region', 'trx_date', 'int_date', 'trx_class_code',
             'trx_type_code', 'trx_type_desc', 'trx_family_code', 'trx_direction',
             'value_kes', 'signed_kes', 'account_ids', 'interview_designation', 'is_dup_key']
findev_cols = ['row_id', 'hh_ids', 'region', 'trx_date', 'int_date', 'trx_type_code',
               'trx_type_desc', 'trx_family_code', 'trx_direction', 'value_kes', 'signed_kes',
               'account_ids', 'interview_designation']

flows[flow_cols].to_csv('data/kfd_transactions_cleaned.csv', index=False)
findev[findev_cols].to_csv('data/kfd_findev_cleaned.csv', index=False)
rejected[['row_id', 'hh_ids', 'trx_class_code', 'trx_date', 'trx_value_kes']].to_csv('data/kfd_rejected.csv', index=False)
print("saved")