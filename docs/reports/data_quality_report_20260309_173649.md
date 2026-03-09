# BigQuery Data Quality Report

- Generated (UTC): `2026-03-09T17:37:02.240799+00:00`
- Table: `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_raw`
- Mode: `raw`
- Rows: `7418485`
- Distinct rows (method dependent): `7418485`
- Rows with quality issues: `0` (0.00%)
- Period: `1998-01-01` -> `2025-12-01`
- Freshness: `128` days (`FAIL`)

| Check | Status | Failed | Ratio | Description |
|---|---|---:|---:|---|
| `invalid_year_parse` | `PASS` | 0 | 0.00% | Vuosi ei parsennu INT64-arvoksi. |
| `invalid_month_parse` | `PASS` | 0 | 0.00% | Kk ei parsennu tai ei ole välillä 1-12. |
| `missing_hallinnonala` | `PASS` | 14177 | 0.19% | Hallinnonala puuttuu. |
| `missing_momentti` | `PASS` | 3819 | 0.05% | Momentti tunnus ja nimi puuttuvat molemmat. |
| `invalid_nettokertyma_parse` | `PASS` | 0 | 0.00% | Nettokertymä ei parsennu NUMERIC-arvoksi. |
| `duplicate_natural_keys` | `FAIL` | 7175922 | 96.73% | Duplikaatit luonnollisella avaimella (vuosi, kk, hallinnonala, momentti, alamomentti). |
| `missing_months_before_latest_year` | `FAIL` | 2 | 0.00% | Puuttuvia kuukausia ennen viimeisintä vuotta. |

## Overall: `FAIL`
