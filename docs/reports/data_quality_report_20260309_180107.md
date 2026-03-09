# BigQuery Data Quality Report

- Generated (UTC): `2026-03-09T18:01:23.776101+00:00`
- Table: `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_curated_dq_v`
- Mode: `curated`
- Rows: `7418485`
- Distinct rows (method dependent): `7294003`
- Rows with quality issues: `17990` (0.24%)
- Period: `1998-01-01` -> `2025-11-01`
- Freshness: `128` days (`FAIL`)

| Check | Status | Failed | Ratio | Description |
|---|---|---:|---:|---|
| `invalid_year_or_month` | `PASS` | 0 | 0.00% | Vuosi/kuukausi tulee olla validi analyysia varten. |
| `invalid_nettokertyma_parse` | `PASS` | 0 | 0.00% | Nettokertymän parse-virhe (raw arvo on olemassa, cast epäonnistuu). |
| `missing_hallinnonala` | `PASS` | 14177 | 0.19% | Hallinnonala puuttuu. |
| `missing_momentti` | `PASS` | 3819 | 0.05% | Momentti tunnus tai nimi puuttuu molemmat. |
| `duplicate_row_fingerprint` | `FAIL` | 124482 | 1.68% | Täsmälleen samat rivit duplikaatteina. |
| `missing_months_before_latest_year` | `WARN` | 2 | 0.00% | Puuttuvia kuukausia ennen viimeisintä vuotta. |

## Overall: `FAIL`
