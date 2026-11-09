"""Land the raw KFD csv in Postgres as-is (all columns text) -> raw.kfd_raw.

Run from the repo root: python load_raw.py [path/to/kfd.csv]
Full refresh: the table is dropped and rebuilt inside one transaction, so a failed run leaves the old table untouched.
"""
import csv
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

load_dotenv()   # reads .env, so the password is never in the code

RAW_PATH = Path(sys.argv[1] if len(sys.argv) > 1 else 'data/raw/kfd.csv')

# columns the dbt staging model reads; the load fails early if the file no longer has them
REQUIRED_COLS = {
    'hh_ids', 'trx_date', 'int_date', 'trx_class_code', 'trx_direction', 'trx_value_kes',
    'trx_mode_code', 'trx_type_code', 'trx_type_desc', 'trx_family_code', 'account_ids',
    'interview_designation',
}

if not RAW_PATH.exists() or RAW_PATH.stat().st_size == 0:
    sys.exit(f'raw file missing or empty: {RAW_PATH}')

with open(RAW_PATH, newline='') as f:
    reader = csv.reader(f)
    header = next(reader)
    file_rows = sum(1 for _ in reader)

missing = REQUIRED_COLS - set(header)
if missing:
    sys.exit(f'raw file is missing expected columns: {sorted(missing)}')
if file_rows == 0:
    sys.exit('raw file has a header but no rows')
print(f'{RAW_PATH}: {file_rows} rows, {len(header)} columns')

conn = psycopg2.connect(
    user=os.getenv('PGUSER', 'eric'),
    password=os.environ['PGPASSWORD'],
    host=os.getenv('PGHOST', 'localhost'),
    port=int(os.getenv('PGPORT', '5432')),
    dbname=os.getenv('PGDATABASE', 'kfd'),
)
try:
    with conn, conn.cursor() as cur:   # one transaction: commit on success, roll back on error
        cur.execute('CREATE SCHEMA IF NOT EXISTS raw')
        cur.execute('DROP TABLE IF EXISTS raw.kfd_raw CASCADE')
        cols = sql.SQL(', ').join(sql.SQL('{} text').format(sql.Identifier(c)) for c in header)
        # row_id = line number in the raw file (COPY inserts in file order)
        cur.execute(sql.SQL('CREATE TABLE raw.kfd_raw (row_id bigint GENERATED ALWAYS AS IDENTITY, {})').format(cols))
        copy_sql = sql.SQL("COPY raw.kfd_raw ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
            sql.SQL(', ').join(sql.Identifier(c) for c in header))
        with open(RAW_PATH, newline='') as f:
            cur.copy_expert(copy_sql.as_string(conn), f)
        cur.execute('SELECT count(*) FROM raw.kfd_raw')
        db_rows = cur.fetchone()[0]
        assert db_rows == file_rows, f'row count mismatch: file {file_rows} vs db {db_rows}'
        print(f'raw.kfd_raw loaded: file rows {file_rows} | db rows {db_rows}')
finally:
    conn.close()
