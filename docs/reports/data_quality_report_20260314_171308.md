# BigQuery Data Quality Report

- Generated (UTC): `2026-03-14T17:13:21.970219+00:00`
- Table: `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_semantic_v1`
- Mode: `semantic`
- Rows: `7418485`
- Distinct rows (method dependent): `7294003`
- Rows with quality issues: `17990` (0.24%)
- Period: `1998-01-01` -> `2025-11-01`
- Freshness: `133` days (`FAIL`)

| Check | Status | Failed | Ratio | Description |
|---|---|---:|---:|---|
| `invalid_year_or_month` | `PASS` | 0 | 0.00% | Vuosi/kuukausi tulee olla validi analyysia varten semantic-view'ssa. |
| `invalid_nettokertyma_parse` | `PASS` | 0 | 0.00% | Semantic-view sisältää rivejä, joilla nettokertymä ei ole validi upstream-parsinnan mukaan. |
| `missing_hallinnonala` | `PASS` | 14177 | 0.19% | Hallinnonala puuttuu semantic-view'sta. |
| `missing_momentti` | `PASS` | 3819 | 0.05% | Momentti tunnus ja nimi puuttuvat molemmat semantic-view'sta. |
| `duplicate_row_fingerprint` | `FAIL` | 124482 | 1.68% | Täsmälleen samat semantic-rivit duplikaatteina. |
| `missing_months_before_latest_year` | `WARN` | 2 | 0.00% | Puuttuvia kuukausia ennen viimeisintä vuotta semantic-view'ssa. |

## Overall: `FAIL`
