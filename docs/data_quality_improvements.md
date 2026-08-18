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
  - `dim_maararahalaji`
  - `dim_talousarviotili`
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
  - `TakpMrL_*` is mapped to `maararahalaji_*`
  - `TakpT_*` is mapped to `talousarviotili_*`
  - `alamomentti_*` is published only after a derived suffix has an exact year-specific match in the official chart registry
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
  - Clustered by `hallinnonala`, `momentti_tunnusp`, `talousarviotili_tunnusp`

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

## App integration (done)

The app now reads the promoted semantic alias by default
(`BUDJETTIHAUKKA_TABLE=valtiontalous_semantic_current`); the raw ingest table
is exposed to pipeline scripts as `BUDJETTIHAUKKA_RAW_TABLE` (default
`budjettidata`).

The semantic layer is versioned: `build_bq_data_quality_layer.py
--semantic-version N` builds `valtiontalous_semantic_v{N}` (plus
`valtiontalous_yearly_agg_v1`) and repoints the alias. Older versions stay
queryable; `--promote-only` repoints the alias without rebuilding, which is
the rollback path.

Column-name compatibility between generated SQL (contracts, fallbacks,
ontology rule expressions) and the semantic view is enforced offline by
`scripts/test_semantic_view_column_compat.py`; run it whenever either side
changes.

## Schema drift detection

The accepted Valtiokonttori source schema lives in
`data/schema_snapshots/valtiokonttori_source_columns.json` (original CSV
header -> normalized name). Two guards compare against it:

- **Ingest** (`ingest_valtiokonttori_to_bigquery.py`): a changed source
  header aborts the run (exit 3) with an added/removed/possible-rename alert
  before anything is loaded. Re-run with `--accept-schema-drift` to accept
  the new schema and update the snapshot.
- **DQ checks** (`run_bq_data_quality_checks.py`): a `schema_drift` check
  compares the live raw table (`--drift-table`, default
  `BUDJETTIHAUKKA_RAW_TABLE`) against the snapshot and FAILs the report on
  mismatch.

Offline tests: `scripts/test_schema_drift_detection.py`.
