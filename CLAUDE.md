# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

ELT pipeline for the Kenya Financial Diaries (KFD) household transaction data: raw CSV -> Postgres `raw.kfd_raw` (as-is, all text) -> dbt (`staging` -> `clean` -> `marts`) -> Metabase. Airflow runs it (`airflow/`). The dbt marts flag weeks where a household's net cashflow is unusually low, and look for coping activity afterwards. `analysis.ipynb` holds exploratory analysis.

Layout: `load_raw.py` and `data/` at the repo root; the dbt project is in `kfd_dbt/`; Airflow (Dockerfile, compose, DAG, container dbt profile) is in `airflow/`. Run `load_raw.py` from the repo root and dbt from `kfd_dbt/`.

## Pipeline & commands

Activate the venv first (`source venv/bin/activate`; Python version in `.python-version`).

1. `python load_raw.py` - reads `data/raw/kfd.csv`, checks the file and required columns, and bulk-loads it with COPY into `raw.kfd_raw` (all columns text, plus `row_id` = line number). Full refresh in one transaction; asserts the row count matches. Needs `PGPASSWORD` in `.env` (optional: `PGUSER`, `PGHOST`, `PGPORT`, `PGDATABASE`; defaults eric/localhost/5432/kfd).
2. From `kfd_dbt/`: `dbt run`, `dbt test`; single model/test: `dbt run -s <model>` / `dbt test -s <model>`. `profiles.yml` is in the project dir, so dbt picks it up when run from `kfd_dbt/` (otherwise pass `--profiles-dir`).
3. Airflow: `cd airflow && docker compose up --build`, UI at http://localhost:8080. DAG `kfd_pipeline` = `load_raw` >> `dbt_run` >> `dbt_test`, weekly plus on demand. The container reaches Postgres on the host via `host.docker.internal` and uses `airflow/profiles.yml` (password from env).

There is no linter or Python test suite; data quality is enforced via dbt tests (`models/sources.yml`, `models/clean/schema.yml`, `models/marts/schema.yml`) and the primary key on `clean.transactions.trx_key`.

## Cleaning rules worth knowing (kfd_dbt/models/staging/stg_kfd.sql)

- Dates come in two formats (`29jul2013` and `29-Jul-13`); both are parsed by the `parse_kfd_date` macro.
- Only `income` and `opex` classes become `clean.transactions`; `findev` goes to `clean.findev_events` with balance rows (mode code 18/19) and directionless rows dropped; `cash`/`assets` snapshots are out of scope.
- In-scope rows with missing household/date/value, value <= 0, direction not Inflow/Outflow, or an unmapped region go to `clean.rejected` with a `reject_reason`, not silently lost.
- Region is derived from the `hh_ids` 3-letter prefix (KEL, KMO, KVI, KMA, KNB). The `KZI` prefix (3 households, about 2.5k rows) is unmapped, so those rows are rejected as `unknown_region` until the dataset docs say what they are.
- Duplicates are kept and flagged (`is_dup_key`), since most are genuine repeat purchases. `trx_key` = md5 of the natural key plus an occurrence counter, so it is unique by construction.
- `signed_kes` is positive for Inflow, negative for Outflow.

## dbt

- Project `kfd_dbt`, profile `kfd_dbt`, Postgres target `dev`. Schemas: `staging`, `clean`, `marts` (set per folder in `dbt_project.yml`; `macros/generate_schema_name.sql` uses them exactly).
- Source `raw.kfd_raw` (`models/sources.yml`) is what `load_raw.py` creates.
- Business-logic thresholds are dbt vars in `dbt_project.yml`: `thin_week_min_rows`, `flag_k`, `min_ok_weeks`, `coping_window_days`. Use `var(...)` in models rather than hard-coding.
- `marts/mart_coping_after_flag.sql` is currently a copy of `mart_household_week_flags.sql`; the coping logic is not written yet.
