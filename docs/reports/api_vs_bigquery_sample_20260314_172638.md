# Valtiokonttori API vs BigQuery Sample Comparison

- Generated (UTC): `2026-03-14T17:26:38.037297+00:00`
- BigQuery table: `valtion-budjetti-data.valtiodata_ingest_tmp_20260308.valtiontalous_semantic_v1`
- Sample method: `stratified_random_sample_by_period`
- Sample seed: `20260314`
- Sample periods: `2001-05, 2002-07, 2002-09, 2006-02, 2007-02, 2010-01, 2011-07, 2015-09, 2019-11, 2020-11, 2024-03, 2024-09`

## Aggregate results

- Full row exact matches: `11/12` (92%)

| Level | Equal groups | Joined groups | Match ratio | Only in API | Only in BigQuery | Total abs diff EUR | Max abs diff EUR |
|---|---:|---:|---:|---:|---:|---:|---:|
| `hallinnonala` | 166 | 180 | 92.22% | 14 | 0 | 5418424538.18 | 2455654902.83 |
| `kirjanpitoyksikko` | 939 | 1003 | 93.62% | 64 | 0 | 8040279137.82 | 2438754061.45 |
| `paaluokkaosasto` | 242 | 264 | 91.67% | 22 | 0 | 10179740226.54 | 2547351868.48 |
| `luku` | 1576 | 1696 | 92.92% | 120 | 0 | 10196879820.06 | 1729078867.61 |
| `momentti` | 5456 | 5888 | 92.66% | 432 | 0 | 10200041045.64 | 1678576000.00 |
| `alamomentti` | 12 | 13 | 92.31% | 1 | 0 | 2997238982.36 | 2997238982.36 |

## Period details

### 2001-05
- Full row exact match: `True` (api rows `33952`, bq rows `33952`, api sum `-667006468.75`, bq sum `-667006468.75`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2002-07
- Full row exact match: `True` (api rows `29876`, bq rows `29876`, api sum `143203544.29`, bq sum `143203544.29`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2002-09
- Full row exact match: `True` (api rows `32129`, bq rows `32129`, api sum `654319443.70`, bq sum `654319443.70`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2006-02
- Full row exact match: `True` (api rows `27211`, bq rows `27211`, api sum `-502391402.64`, bq sum `-502391402.64`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2007-02
- Full row exact match: `True` (api rows `22573`, bq rows `22573`, api sum `-458261012.05`, bq sum `-458261012.05`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2010-01
- Full row exact match: `False` (api rows `14443`, bq rows `0`, api sum `2997238982.36`, bq sum `0.00`)
  - `hallinnonala`: match `6.67%`, only_api `14`, only_bq `0`, total_abs_diff `5418424538.18`
  - `kirjanpitoyksikko`: match `5.88%`, only_api `64`, only_bq `0`, total_abs_diff `8040279137.82`
  - `paaluokkaosasto`: match `0.00%`, only_api `22`, only_bq `0`, total_abs_diff `10179740226.54`
  - `luku`: match `11.11%`, only_api `120`, only_bq `0`, total_abs_diff `10196879820.06`
  - `momentti`: match `29.64%`, only_api `432`, only_bq `0`, total_abs_diff `10200041045.64`
  - `alamomentti`: match `50.00%`, only_api `1`, only_bq `0`, total_abs_diff `2997238982.36`

### 2011-07
- Full row exact match: `True` (api rows `14665`, bq rows `14665`, api sum `544204442.34`, bq sum `544204442.34`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2015-09
- Full row exact match: `True` (api rows `13165`, bq rows `13165`, api sum `182726284.14`, bq sum `182726284.14`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2019-11
- Full row exact match: `True` (api rows `15946`, bq rows `15946`, api sum `-119478441.38`, bq sum `-119478441.38`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2020-11
- Full row exact match: `True` (api rows `15057`, bq rows `15057`, api sum `1300081605.11`, bq sum `1300081605.11`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2024-03
- Full row exact match: `True` (api rows `15168`, bq rows `15168`, api sum `922794161.67`, bq sum `922794161.67`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`

### 2024-09
- Full row exact match: `True` (api rows `15327`, bq rows `15327`, api sum `3500300831.29`, bq sum `3500300831.29`)
  - `hallinnonala`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `kirjanpitoyksikko`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `paaluokkaosasto`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `luku`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `momentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
  - `alamomentti`: match `100.00%`, only_api `0`, only_bq `0`, total_abs_diff `0.00`
