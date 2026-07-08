# BigQuery Data Quality Report

- Generated (UTC): `2026-07-08T07:38:25.900828+00:00`
- Table: `budjettihaukka-gpt.valtiodata.valtiontalous_semantic_current`
- Mode: `semantic`
- Rows: `7537667`
- Distinct rows (method dependent): `7534067`
- Rows with quality issues: `19468` (0.26%)
- Period: `1998-01-01` -> `2026-05-01`
- Freshness: `68` days (`WARN`)

| Check | Status | Failed | Ratio | Description |
|---|---|---:|---:|---|
| `invalid_year_or_month` | `PASS` | 0 | 0.00% | Vuosi/kuukausi tulee olla validi analyysia varten semantic-view'ssa. |
| `invalid_nettokertyma_parse` | `PASS` | 0 | 0.00% | Semantic-view sisältää rivejä, joilla nettokertymä ei ole validi upstream-parsinnan mukaan. |
| `missing_hallinnonala` | `PASS` | 15203 | 0.20% | Hallinnonala puuttuu semantic-view'sta. |
| `missing_momentti` | `PASS` | 4271 | 0.06% | Momentti tunnus ja nimi puuttuvat molemmat semantic-view'sta. |
| `duplicate_row_fingerprint` | `WARN` | 3600 | 0.05% | Täsmälleen samat semantic-rivit duplikaatteina. |
| `missing_months_before_latest_year` | `PASS` | 0 | 0.00% | Puuttuvia kuukausia ennen viimeisintä vuotta semantic-view'ssa. |
| `schema_drift` | `PASS` | 0 | 0.00% | Live budjettihaukka-gpt.valtiodata.valtiontalous_raw schema matches accepted snapshot. |

## Overall: `WARN`
