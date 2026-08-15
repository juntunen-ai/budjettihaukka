# Visualization-ready data mart

Budjettihaukan `visualization_mart` is an analytics contract for building a
single trustworthy chart without reverse-engineering source columns. It does
not parse natural language and it does not choose a visualization for the user.

## What is safe to use

| Object | Grain | Purpose |
|---|---|---|
| `analytics_fiscal_yearly_v1` | year × fiscal hierarchy | Wide fiscal fact with nominal, real, per-capita and GDP-relative values |
| `analytics_metric_series_v1` | year × fiscal hierarchy × metric | Long chart-friendly fact; includes unit and metric definition on every row |
| `analytics_budget_vs_actual_v2` | year × moment | Budget versus actual with partial-year and extreme-ratio quality gates |
| `analytics_final_accounts_reconciliation_v1` | year × fiscal side | Fail-closed reconciliation interface; official totals remain null until sourced |
| `analytics_macro_yearly_v1` | year | Population, CPI, GDP and central-government EDP debt denominators |
| `analytics_visualization_quality_v1` | metric × year | Missingness, coverage, structural-break flags and readiness score |
| `dim_visualization_metric_v1` | metric | Versioned definition, unit, sign, aggregation and missing-value rule |
| `dim_data_source_v1` | source | Official URL, accounting basis, update cadence and caveat |
| `dim_data_availability_v1` | domain | Integrated and explicitly unavailable subject areas |
| `dim_enrichment_join_contract_v1` | domain | Required keys, measures, join rule and publication gate for future enrichments |

`analytics_fiscal_yearly_v1` is a stable view over the partitioned and clustered
`analytics_fiscal_yearly_core_v1` table. Rebuild the mart after refreshing its
sources. This makes chart queries cheap and prevents the historical lineage
calculation from running for every end user.

## Interpretation rules

1. Filter `coverage_status = 'complete'` for annual comparisons. The current
   partial year is deliberately retained but visibly marked.
2. Use `metric_id`, `unit`, `price_basis` and `aggregation_rule` together.
   Per-capita values, ratios and percentages are non-additive.
3. A `NULL` value means unavailable according to `missing_means`; it is never
   silently converted to zero.
4. For time-series rankings, exclude or annotate rows where
   `has_structural_guardrail` is true.
5. `net_accumulation_real_cpi_eur` is a purchasing-power adjustment, not a
   public-service volume measure.
6. `net_accumulation_pct_gdp` compares two accounting frameworks. It does not
   turn budget accounting into national accounts.
7. `is_reconciled_to_audited_final_accounts` remains false until an official
   final-account reconciliation source is integrated.
8. Do not create recipient, procurement, efficiency or outcome claims while
   their domain status is `not_integrated`.

## Safe example queries

### Real expenditure trend by administrative branch

```sql
SELECT
  year,
  hallinnonala,
  SUM(value) / 1000000 AS million_eur,
  ANY_VALUE(real_base_year) AS price_base_year
FROM `PROJECT.DATASET.analytics_metric_series_v1`
WHERE metric_id = 'net_accumulation_real_cpi_eur'
  AND fiscal_side = 'expense'
  AND coverage_status = 'complete'
  AND NOT has_structural_guardrail
GROUP BY year, hallinnonala
ORDER BY year, hallinnonala;
```

Recommended chart: a line chart for a deliberately selected small set of
administrative branches. State the CPI base year in the subtitle.

### Per-capita moment trend

```sql
SELECT year, momentti_tunnusp, momentti_snimi, SUM(value) AS eur_per_person
FROM `PROJECT.DATASET.analytics_metric_series_v1`
WHERE metric_id = 'net_accumulation_per_capita_eur'
  AND momentti_tunnusp = @moment_code
  AND coverage_status = 'complete'
GROUP BY year, momentti_tunnusp, momentti_snimi
ORDER BY year;
```

Recommended chart: a single annotated line. Do not sum `eur_per_person` across
hierarchy rows after it has already been calculated at a higher aggregate.

### Budget versus actual

```sql
SELECT
  year,
  momentti_tunnusp,
  momentti_snimi,
  current_budget_eur / 1000000 AS budget_meur,
  actual_eur / 1000000 AS actual_meur,
  actual_to_budget_ratio
FROM `PROJECT.DATASET.analytics_budget_vs_actual_v2`
WHERE visualization_quality_status = 'ready'
  AND fiscal_side = 'expense'
  AND year BETWEEN @year_from AND @year_to;
```

Recommended chart: paired dots or bars. Keep extreme ratios in a separate
review table instead of stretching the chart scale.

### Debt in macro context

```sql
SELECT
  year,
  central_government_edp_debt_q4_meur,
  100 * SAFE_DIVIDE(central_government_edp_debt_q4_meur, gdp_current_prices_meur) AS debt_pct_gdp
FROM `PROJECT.DATASET.analytics_macro_yearly_v1`
WHERE central_government_edp_debt_q4_meur IS NOT NULL
ORDER BY year;
```

Label the series as *central-government EDP debt (S1311), Q4 stock*. Do not
label it simply “state debt”, because that is a different official concept.

## Refresh and deployment

```bash
# Fetch a deterministic official source snapshot.
.venv/bin/python scripts/load_visualization_reference_series.py --year-from 1998 --year-to 2026

# Load the snapshot explicitly.
.venv/bin/python scripts/load_visualization_reference_series.py \
  --year-from 1998 --year-to 2026 \
  --load-bigquery --project PROJECT --dataset DATASET

# Build the corrected mart after semantic v2 is available.
.venv/bin/python scripts/build_visualization_data_mart.py \
  --project PROJECT --dataset DATASET

# Temporary migration adapter for the current legacy yearly table.
.venv/bin/python scripts/build_visualization_data_mart.py \
  --project PROJECT --dataset DATASET \
  --yearly-source-mode legacy_mislabeled_maararahalaji

# Run low-cost acceptance checks over the materialized fact.
.venv/bin/python scripts/run_visualization_data_quality_checks.py \
  --project PROJECT --dataset DATASET
```

The migration adapter maps the old mislabeled `alamomentti_*` columns to
`maararahalaji_*`, publishes `alamomentti_*` as null, and exposes
`talousarviotili_available = false`. It must be removed by rebuilding in
`corrected` mode after semantic v2 is deployed.

## Known operational blocker

The current BigQuery project runs in sandbox mode. It enforces an expiration
shorter than 60 days and rejects attempts to clear table expiration until
billing is enabled. The reference snapshot is versioned in Git, so it remains
recoverable, but the warehouse is not durable without either:

- enabling billing and removing expirations from production assets, or
- scheduling refresh and rebuild jobs more frequently than the sandbox TTL.

This is an infrastructure limitation, not a data-quality status that should be
hidden from a professional user.
