import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()   # reads .env, so the password is never in the code

engine = create_engine(URL.create(
    'postgresql+psycopg2',
    username=os.getenv('PGUSER', 'eric'),
    password=os.environ['PGPASSWORD'],
    host=os.getenv('PGHOST', 'localhost'),
    port=int(os.getenv('PGPORT', '5432')),
    database=os.getenv('PGDATABASE', 'kfd'),
))

with engine.begin() as conn:
    conn.execute(text('CREATE SCHEMA IF NOT EXISTS clean'))

tx = pd.read_csv('data/kfd_transactions_cleaned.csv', dtype={'account_ids': str, 'trx_type_code': str},
                 parse_dates=['trx_date', 'int_date'])
findev = pd.read_csv('data/kfd_findev_cleaned.csv', dtype={'account_ids': str, 'trx_type_code': str},
                     parse_dates=['trx_date', 'int_date'])

tx.to_sql('transactions', engine, schema='clean', if_exists='replace', index=False,
          chunksize=10000, method='multi')
findev.to_sql('findev_events', engine, schema='clean', if_exists='replace', index=False,
              chunksize=10000, method='multi')

# primary key = uniqueness test: this fails loudly if trx_key is not unique
with engine.begin() as conn:
    conn.execute(text('ALTER TABLE clean.transactions ADD PRIMARY KEY (trx_key)'))

# verify the load
with engine.connect() as conn:
    for table, df in [('transactions', tx), ('findev_events', findev)]:
        n = conn.execute(text(f'SELECT count(*) FROM clean.{table}')).scalar()
        print(table, 'file rows:', len(df), '| db rows:', n)
        assert n == len(df), f'row count mismatch in {table}'

print("Data loaded into PostgreSQL database successfully.")