# BigQuery Data Quality Improvements

## What was implemented

Data quality layer was added as reproducible scripts:

1. `scripts/build_bq_data_quality_layer.py`
- Builds typed curated table: `valtiontalous_curated_dq`
- Adds row-level quality flags and `quality_issue_count`
- Adds stable `row_fingerprint` for duplicate detection
- Builds dimensions:
  - `dim_hallinnonala`
  - `dim_momentti`
  - `dim_alamomentti`
  - `dim_topic_alias`
- Builds semantic view: `valtiontalous_semantic_v1`

2. `scripts/run_bq_data_quality_checks.py`
- Runs automated checks against curated table
- Outputs report as both JSON and Markdown under `docs/reports/`
- Produces an overall status `PASS/WARN/FAIL`

## Curated table design (`valtiontalous_curated_dq`)

- Type casting:
  - `vuosi` / `kk` are typed to `INT64`
  - Key budget amount fields are typed to `NUMERIC`
  - Original string raw fields are preserved for parse diagnostics (e.g. `nettokertyma_raw`)
- Normalization:
  - String fields are trimmed and empty strings converted to `NULL`
  - `TakpMrL_*` is mapped to `alamomentti_*`
- Data quality metadata:
  - `is_valid_year`
  - `is_valid_month`
  - `has_hallinnonala`
  - `has_momentti`
  - `has_valid_nettokertyma`
  - `quality_issue_count`
  - `row_fingerprint`
- Physical optimization:
  - Partitioned by `period_date`
  - Clustered by `hallinnonala`, `momentti_tunnusp`, `alamomentti_tunnus`

## Checks included

- Invalid year/month values
- Parse failures for `nettokertyma`
- Missing hallinnonala
- Missing momentti identity
- Duplicate rows by fingerprint
- Missing months before latest year
- Freshness (days since latest period)

## How to run

```bash
cd /Users/harrijuntunen/budjettihaukka

# Build curated+dims+semantic view
.venv/bin/python scripts/build_bq_data_quality_layer.py

# Run checks and print report
.venv/bin/python scripts/run_bq_data_quality_checks.py
```

If you only want SQL artifacts without BigQuery write permissions:

```bash
.venv/bin/python scripts/build_bq_data_quality_layer.py --render-sql-dir data/sql/dq_layer
```

## IAM requirements

To create curated tables/views and dimensions, caller needs at least:

- `bigquery.tables.create`
- `bigquery.tables.updateData`
- `bigquery.tables.update`

on target dataset (for example `valtion-budjetti-data.valtiodata_ingest_tmp_20260308`).

## Recommended next integration step

After validating report results, switch app data table from raw to semantic/curated path by environment:

```bash
export BUDJETTIHAUKKA_TABLE="valtiontalous_semantic_v1"
```

Before switching, SQL generation templates/contracts should be updated to reference curated column names consistently.
