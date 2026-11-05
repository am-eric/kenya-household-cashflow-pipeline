# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Pipeline for the Kenya Financial Diaries (KFD) household transaction data: raw CSV → pandas clean → Postgres → dbt models (flagging weeks where a household's net cashflow is unusually low, and looking for coping activity afterwards). `analysis.ipynb` holds exploratory analysis.

Layout: Python scripts and `data/` live at the repo root; the dbt project is in `kfd_dbt/`. Run scripts from the repo root (they use relative `data/...` paths) and dbt from `kfd_dbt/`.

## Pipeline & commands

Activate the venv first (`source venv/bin/activate`; Python version in `.python-version`).

1. `python clean.py` — reads `data/raw/kfd.csv` (all columns as text), writes `data/kfd_transactions_cleaned.csv`, `data/kfd_findev_cleaned.csv`, `data/kfd_rejected.csv`.
2. `python load.py` — loads the cleaned CSVs into Postgres schema `clean` (`clean.transactions`, `clean.findev_events`), replacing existing tables, and asserts row counts match. Needs `PGPASSWORD` in `.env` (optional: `PGUSER`, `PGHOST`, `PGPORT`, `PGDATABASE`; defaults eric/localhost/5432/kfd).
3. From `kfd_dbt/`: `dbt run`, `dbt test`; single model/test: `dbt run -s <model>` / `dbt test -s <model_or_source:clean.transactions>`. `profiles.yml` is in the project dir, so dbt picks it up when run from `kfd_dbt/` (otherwise pass `--profiles-dir`).

There is no linter or Python test suite; data quality is enforced via dbt tests in `kfd_dbt/models/sources.yml` and the primary key on `clean.transactions.trx_key` (load fails if not unique).

## Cleaning rules worth knowing (clean.py)

- Dates come in two formats (`29jul2013` and `29-Jul-13`); both are parsed.
- Only `income` and `opex` classes become `transactions`; `findev` goes to its own table with balance rows (mode code 18/19) and directionless rows dropped; `cash`/`assets` snapshots are out of scope.
- Rows with missing household/date/value, value <= 0, or direction not Inflow/Outflow are written to the rejected file, not silently lost.
- Region is derived from the `hh_ids` 3-letter prefix (KEL, KMO, KVI, KMA, KNB).
- Duplicates are kept and flagged (`is_dup_key`), since most are genuine repeat purchases. `trx_key` = md5 of the natural key plus an occurrence counter, so it is unique by construction.
- `signed_kes` is positive for Inflow, negative for Outflow.

## dbt

- Project `kfd_dbt`, profile `kfd_dbt`, Postgres target `dev`, schema `clean`. `macros/generate_schema_name.sql` overrides schema naming.
- Source `clean` (`models/sources.yml`) points at the tables `load.py` creates. Note its description refers to `etl/clean.py`/`etl/load.py`, but the scripts are at the repo root.
- Business-logic thresholds are dbt vars in `dbt_project.yml`: `thin_week_min_rows`, `flag_k`, `min_ok_weeks`, `coping_window_days`. Use `var(...)` in models rather than hard-coding.
- `models/example/` is still the dbt starter scaffold; real models have not been added yet.
