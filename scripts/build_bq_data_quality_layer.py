#!/usr/bin/env python3
"""Build a typed/clean data quality layer and dimensions in BigQuery."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from google.api_core.exceptions import Forbidden
from google.cloud import bigquery

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import settings

logger = logging.getLogger("build_bq_data_quality_layer")


def _numeric_expr(raw_col: str) -> str:
    # Handles spaces, NBSP, comma decimal separator and unicode minus.
    return (
        f"SAFE_CAST(REPLACE(REPLACE(REPLACE(REPLACE("
        f"REGEXP_REPLACE(NULLIF(TRIM({raw_col}), ''), r'\\s+', ''), "
        f"'−', '-'), ' ', ''), ' ', ''), ',', '.') AS NUMERIC)"
    )


def _clean_text_expr(raw_col: str) -> str:
    return f"NULLIF(REGEXP_REPLACE(TRIM({raw_col}), r'\\s+', ' '), '')"


def _display_name_expr(expr: str) -> str:
    collapsed = f"REGEXP_REPLACE(TRIM({expr}), r'\\s+', ' ')"
    return (
        "CASE "
        f"WHEN {expr} IS NULL THEN NULL "
        f"WHEN REGEXP_CONTAINS({collapsed}, r'[A-ZÅÄÖ]') AND {collapsed} = UPPER({collapsed}) "
        f"THEN INITCAP(LOWER({collapsed})) "
        f"ELSE {collapsed} "
        "END"
    )


def _family_key_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} IS NULL THEN NULL "
        f"WHEN LENGTH({expr}) >= 35 THEN SUBSTR(LOWER({expr}), 1, 35) "
        f"ELSE LOWER({expr}) "
        "END"
    )


def _hierarchy_level_specs() -> list[tuple[str, str, str, str]]:
    return [
        ("hallinnonala", "ha_tunnus", "hallinnonala", "hallinnonala"),
        ("kirjanpitoyksikko", "tv_tunnus", "kirjanpitoyksikko", "kirjanpitoyksikko"),
        ("paaluokkaosasto", "paaluokkaosasto_tunnusp", "paaluokkaosasto_snimi", "paaluokkaosasto"),
        ("luku", "luku_tunnusp", "luku_snimi", "luku"),
        ("momentti", "momentti_tunnusp", "momentti_snimi", "momentti"),
        ("alamomentti", "alamomentti_tunnus", "alamomentti_snimi", "alamomentti"),
    ]


def _hierarchy_union_sql(curated_ref: str) -> str:
    parts: list[str] = []
    for level_name, code_col, name_col, prefix in _hierarchy_level_specs():
        parts.append(
            f"""
SELECT
  '{level_name}' AS level_name,
  vuosi,
  {code_col} AS code,
  {name_col} AS alias_name,
  {prefix}_display AS alias_display_name,
  {prefix}_family_key AS alias_family_key
FROM {curated_ref}
WHERE {code_col} IS NOT NULL
  AND {name_col} IS NOT NULL
"""
        )
    return "\nUNION ALL\n".join(parts)


def _hierarchy_helper_selects() -> list[str]:
    helper_selects: list[str] = []
    for _level_name, _code_col, name_col, prefix in _hierarchy_level_specs():
        display_expr = _display_name_expr(name_col)
        helper_selects.append(f"{display_expr} AS {prefix}_display")
        helper_selects.append(f"{_family_key_expr(display_expr)} AS {prefix}_family_key")
    return helper_selects


def _build_hierarchy_mapping_sql(
    project: str,
    dataset: str,
    curated_ref: str,
    create_object: str,
) -> str:
    return f"""
CREATE OR REPLACE {create_object} `{project}.{dataset}.dim_hierarchy_name_mapping` AS
WITH hierarchy AS (
  {_hierarchy_union_sql(curated_ref)}
),
normalized AS (
  SELECT
    level_name,
    vuosi,
    code,
    alias_name,
    alias_display_name,
    alias_family_key
  FROM hierarchy
  WHERE code IS NOT NULL
    AND alias_display_name IS NOT NULL
),
alias_ranges AS (
  SELECT
    level_name,
    code,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT alias_name ORDER BY alias_name), ' | ') AS alias_name,
    alias_display_name,
    alias_family_key,
    MIN(vuosi) AS valid_from_year,
    MAX(vuosi) AS valid_to_year,
    COUNT(*) AS row_count,
    COUNT(DISTINCT vuosi) AS distinct_years
  FROM normalized
  GROUP BY level_name, code, alias_display_name, alias_family_key
),
family_canonical AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      level_name,
      code,
      alias_family_key,
      alias_display_name AS canonical_name,
      ROW_NUMBER() OVER (
        PARTITION BY level_name, code, alias_family_key
        ORDER BY
          SUM(row_count) DESC,
          MAX(LENGTH(alias_display_name)) DESC,
          MAX(valid_to_year) DESC,
          alias_display_name DESC
      ) AS rn
    FROM alias_ranges
    GROUP BY level_name, code, alias_family_key, alias_display_name
  )
  WHERE rn = 1
),
family_summary AS (
  SELECT
    level_name,
    code,
    COUNT(DISTINCT alias_family_key) AS family_key_count,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT canonical_name ORDER BY canonical_name), ' | ') AS family_names
  FROM family_canonical
  GROUP BY level_name, code
),
same_year_conflicts AS (
  SELECT
    level_name,
    code,
    COUNT(*) AS same_year_conflict_years,
    ARRAY_TO_STRING(ARRAY_AGG(CAST(vuosi AS STRING) ORDER BY vuosi), ', ') AS conflict_years
  FROM (
    SELECT
      level_name,
      code,
      vuosi
    FROM normalized
    GROUP BY level_name, code, vuosi
    HAVING COUNT(DISTINCT alias_display_name) > 1
  )
  GROUP BY level_name, code
)
SELECT
  alias_ranges.level_name,
  alias_ranges.code,
  family_canonical.canonical_name,
  alias_ranges.alias_name,
  alias_ranges.alias_display_name,
  alias_ranges.alias_family_key,
  alias_ranges.valid_from_year,
  alias_ranges.valid_to_year,
  alias_ranges.distinct_years,
  alias_ranges.row_count,
  CASE
    WHEN alias_ranges.alias_display_name = family_canonical.canonical_name THEN 'canonical'
    ELSE 'formatting_noise'
  END AS alias_issue_category,
  COALESCE(family_summary.family_key_count, 1) AS family_key_count,
  COALESCE(family_summary.family_names, family_canonical.canonical_name) AS family_names,
  COALESCE(same_year_conflicts.same_year_conflict_years, 0) AS same_year_conflict_years,
  COALESCE(same_year_conflicts.same_year_conflict_years, 0) > 0 AS has_same_year_conflict,
  same_year_conflicts.conflict_years
FROM alias_ranges
JOIN family_canonical
  USING(level_name, code, alias_family_key)
LEFT JOIN family_summary
  USING(level_name, code)
LEFT JOIN same_year_conflicts
  USING(level_name, code)
"""


def _build_hierarchy_consistency_sql(
    project: str,
    dataset: str,
    curated_ref: str,
    create_object: str,
) -> str:
    return f"""
CREATE OR REPLACE {create_object} `{project}.{dataset}.dq_hierarchy_consistency` AS
WITH hierarchy AS (
  {_hierarchy_union_sql(curated_ref)}
),
normalized AS (
  SELECT
    level_name,
    vuosi,
    code,
    alias_name,
    alias_display_name,
    alias_family_key
  FROM hierarchy
  WHERE code IS NOT NULL
    AND alias_display_name IS NOT NULL
),
mapping AS (
  SELECT
    level_name,
    code,
    canonical_name,
    alias_name,
    alias_display_name,
    alias_family_key,
    valid_from_year,
    valid_to_year,
    alias_issue_category,
    family_key_count,
    family_names
  FROM `{project}.{dataset}.dim_hierarchy_name_mapping`
),
same_year_conflicts AS (
  SELECT
    normalized.level_name,
    normalized.code,
    canonical.canonical_name,
    'same_year_conflict' AS issue_category,
    normalized.vuosi AS affected_year,
    CAST(NULL AS INT64) AS valid_from_year,
    CAST(NULL AS INT64) AS valid_to_year,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT normalized.alias_display_name ORDER BY normalized.alias_display_name),
      ' | '
    ) AS alias_name,
    CAST(NULL AS STRING) AS alias_display_name,
    COUNT(DISTINCT normalized.alias_display_name) AS alias_count,
    COUNT(DISTINCT normalized.alias_family_key) AS family_key_count,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT normalized.alias_name ORDER BY normalized.alias_name),
      ' | '
    ) AS details
  FROM normalized
  JOIN (
    SELECT DISTINCT level_name, code, canonical_name
    FROM mapping
  ) AS canonical
    USING(level_name, code)
  GROUP BY
    normalized.level_name,
    normalized.code,
    canonical.canonical_name,
    normalized.vuosi
  HAVING COUNT(DISTINCT normalized.alias_display_name) > 1
),
alias_issues AS (
  SELECT
    level_name,
    code,
    canonical_name,
    alias_issue_category AS issue_category,
    CAST(NULL AS INT64) AS affected_year,
    valid_from_year,
    valid_to_year,
    alias_name,
    alias_display_name,
    CAST(NULL AS INT64) AS alias_count,
    CAST(NULL AS INT64) AS family_key_count,
    CONCAT('family=', alias_family_key) AS details
  FROM mapping
  WHERE alias_issue_category = 'formatting_noise'
),
historical_renames AS (
  SELECT
    level_name,
    code,
    canonical_name,
    'historical_rename' AS issue_category,
    CAST(NULL AS INT64) AS affected_year,
    MIN(valid_from_year) AS valid_from_year,
    MAX(valid_to_year) AS valid_to_year,
    MAX(alias_name) AS alias_name,
    canonical_name AS alias_display_name,
    CAST(NULL AS INT64) AS alias_count,
    MAX(family_key_count) AS family_key_count,
    MAX(family_names) AS details
  FROM mapping
  WHERE family_key_count > 1
  GROUP BY level_name, code, canonical_name
)
SELECT *
FROM same_year_conflicts
UNION ALL
SELECT *
FROM alias_issues
UNION ALL
SELECT *
FROM historical_renames
"""


def _run_query(client: bigquery.Client, sql: str, label: str, dry_run: bool = False) -> None:
    logger.info("Running step: %s", label)
    if dry_run:
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            ),
        )
        logger.info("Dry-run bytes for %s: %s", label, int(job.total_bytes_processed or 0))
        return
    client.query(sql).result()
    logger.info("Completed: %s", label)


def build_curated_sql(
    project: str,
    dataset: str,
    raw_table: str,
    curated_table: str,
    build_mode: str,
) -> str:
    table_ref = f"`{project}.{dataset}.{raw_table}`"
    target_ref = f"`{project}.{dataset}.{curated_table}`"
    hierarchy_helper_selects = ",\n  ".join(_hierarchy_helper_selects())
    if build_mode == "table":
        header = (
            f"CREATE OR REPLACE TABLE {target_ref}\n"
            "PARTITION BY period_date\n"
            "CLUSTER BY hallinnonala, momentti_tunnusp, alamomentti_tunnus\n"
            "AS"
        )
    else:
        header = f"CREATE OR REPLACE VIEW {target_ref} AS"
    return f"""
{header}
WITH normalized AS (
  SELECT
    SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
    SAFE_CAST(`Kk` AS INT64) AS kk,
    DATE(SAFE_CAST(`Vuosi` AS INT64), SAFE_CAST(`Kk` AS INT64), 1) AS period_date,
    NULLIF(TRIM(`Ha_Tunnus`), '') AS ha_tunnus,
    NULLIF(TRIM(`Hallinnonala`), '') AS hallinnonala,
    NULLIF(TRIM(`Tv_Tunnus`), '') AS tv_tunnus,
    NULLIF(TRIM(`Kirjanpitoyksikkö`), '') AS kirjanpitoyksikko,
    NULLIF(TRIM(`PaaluokkaOsasto_TunnusP`), '') AS paaluokkaosasto_tunnusp,
    NULLIF(TRIM(`PaaluokkaOsasto_sNimi`), '') AS paaluokkaosasto_snimi,
    NULLIF(TRIM(`Luku_TunnusP`), '') AS luku_tunnusp,
    NULLIF(TRIM(`Luku_sNimi`), '') AS luku_snimi,
    NULLIF(TRIM(`Momentti_TunnusP`), '') AS momentti_tunnusp,
    NULLIF(TRIM(`Momentti_sNimi`), '') AS momentti_snimi,
    NULLIF(TRIM(`TakpT_TunnusP`), '') AS takpt_tunnusp,
    NULLIF(TRIM(`TakpT_sNimi`), '') AS takpt_snimi,
    NULLIF(TRIM(`TakpTr_sNimi`), '') AS takptr_snimi,
    NULLIF(TRIM(`TakpMrL_Tunnus`), '') AS alamomentti_tunnus,
    NULLIF(TRIM(`TakpMrL_sNimi`), '') AS alamomentti_snimi,
    NULLIF(TRIM(`TakpT_Netto`), '') AS takpt_netto_raw,
    NULLIF(TRIM(`Tililuokka_Tunnus`), '') AS tililuokka_tunnus,
    NULLIF(TRIM(`Tililuokka_sNimi`), '') AS tililuokka_snimi,
    NULLIF(TRIM(`Ylatiliryhma_Tunnus`), '') AS ylatiliryhma_tunnus,
    NULLIF(TRIM(`Ylatiliryhma_sNimi`), '') AS ylatiliryhma_snimi,
    NULLIF(TRIM(`Tiliryhma_Tunnus`), '') AS tiliryhma_tunnus,
    NULLIF(TRIM(`Tiliryhma_sNimi`), '') AS tiliryhma_snimi,
    NULLIF(TRIM(`Tililaji_Tunnus`), '') AS tililaji_tunnus,
    NULLIF(TRIM(`Tililaji_sNimi`), '') AS tililaji_snimi,
    NULLIF(TRIM(`LkpT_Tunnus`), '') AS lkpt_tunnus,
    NULLIF(TRIM(`LkpT_sNimi`), '') AS lkpt_snimi,
    NULLIF(TRIM(`Alkuperäinen_talousarvio`), '') AS alkuperainen_talousarvio_raw,
    NULLIF(TRIM(`Lisätalousarvio`), '') AS lisatalousarvio_raw,
    NULLIF(TRIM(`Voimassaoleva_talousarvio`), '') AS voimassaoleva_talousarvio_raw,
    NULLIF(TRIM(`Käytettävissä`), '') AS kaytettavissa_raw,
    NULLIF(TRIM(`Alkusaldo`), '') AS alkusaldo_raw,
    NULLIF(TRIM(`Nettokertymä_ko_vuodelta`), '') AS nettokertyma_ko_vuodelta_raw,
    NULLIF(TRIM(`NettoKertymaAikVuosSiirrt`), '') AS nettokertymaaikvuossiirrt_raw,
    NULLIF(TRIM(`Nettokertymä`), '') AS nettokertyma_raw,
    NULLIF(TRIM(`Loppusaldo`), '') AS loppusaldo_raw,
    NULLIF(TRIM(`JakamatonDb`), '') AS jakamatondb_raw,
    NULLIF(TRIM(`JakamatonKr`), '') AS jakamatonkr_raw
  FROM {table_ref}
),
typed AS (
  SELECT
    *,
    {_numeric_expr("alkuperainen_talousarvio_raw")} AS alkuperainen_talousarvio,
    {_numeric_expr("lisatalousarvio_raw")} AS lisatalousarvio,
    {_numeric_expr("voimassaoleva_talousarvio_raw")} AS voimassaoleva_talousarvio,
    {_numeric_expr("kaytettavissa_raw")} AS kaytettavissa,
    {_numeric_expr("alkusaldo_raw")} AS alkusaldo,
    {_numeric_expr("nettokertyma_ko_vuodelta_raw")} AS nettokertyma_ko_vuodelta,
    {_numeric_expr("nettokertymaaikvuossiirrt_raw")} AS nettokertymaaikvuossiirrt,
    {_numeric_expr("nettokertyma_raw")} AS nettokertyma,
    {_numeric_expr("loppusaldo_raw")} AS loppusaldo,
    {_numeric_expr("jakamatondb_raw")} AS jakamatondb,
    {_numeric_expr("jakamatonkr_raw")} AS jakamatonkr
  FROM normalized
)
SELECT
  *,
  vuosi BETWEEN 1900 AND 2100 AS is_valid_year,
  kk BETWEEN 1 AND 12 AS is_valid_month,
  hallinnonala IS NOT NULL AS has_hallinnonala,
  COALESCE(momentti_tunnusp, momentti_snimi) IS NOT NULL AS has_momentti,
  (nettokertyma_raw IS NULL OR nettokertyma IS NOT NULL) AS has_valid_nettokertyma,
  (
    CAST(NOT (vuosi BETWEEN 1900 AND 2100) AS INT64) +
    CAST(NOT (kk BETWEEN 1 AND 12) AS INT64) +
    CAST(hallinnonala IS NULL AS INT64) +
    CAST(COALESCE(momentti_tunnusp, momentti_snimi) IS NULL AS INT64) +
    CAST(NOT (nettokertyma_raw IS NULL OR nettokertyma IS NOT NULL) AS INT64)
  ) AS quality_issue_count,
  TO_HEX(
    MD5(
      CONCAT(
        COALESCE(CAST(vuosi AS STRING), ''), '|',
        COALESCE(CAST(kk AS STRING), ''), '|',
        COALESCE(ha_tunnus, ''), '|',
        COALESCE(hallinnonala, ''), '|',
        COALESCE(tv_tunnus, ''), '|',
        COALESCE(kirjanpitoyksikko, ''), '|',
        COALESCE(momentti_tunnusp, ''), '|',
        COALESCE(momentti_snimi, ''), '|',
        COALESCE(alamomentti_tunnus, ''), '|',
        COALESCE(alamomentti_snimi, ''), '|',
        COALESCE(CAST(nettokertyma AS STRING), '')
      )
    )
  ) AS row_fingerprint,
  {hierarchy_helper_selects}
FROM typed
WHERE vuosi IS NOT NULL
  AND kk IS NOT NULL
"""


def build_dimensions_sql(project: str, dataset: str, curated_table: str, build_mode: str) -> list[tuple[str, str]]:
    curated_ref = f"`{project}.{dataset}.{curated_table}`"
    steps: list[tuple[str, str]] = []
    create_object = "TABLE" if build_mode == "table" else "VIEW"

    steps.append(
        (
            "dim_hallinnonala",
            f"""
CREATE OR REPLACE {create_object} `{project}.{dataset}.dim_hallinnonala` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(ha_tunnus, ''), '|', COALESCE(hallinnonala, '')))) AS hallinnonala_id,
  ha_tunnus,
  hallinnonala,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM {curated_ref}
WHERE hallinnonala IS NOT NULL
GROUP BY ha_tunnus, hallinnonala
""",
        )
    )

    steps.append(
        (
            "dim_momentti",
            f"""
CREATE OR REPLACE {create_object} `{project}.{dataset}.dim_momentti` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(momentti_tunnusp, ''), '|', COALESCE(momentti_snimi, '')))) AS momentti_id,
  momentti_tunnusp,
  momentti_snimi,
  ANY_VALUE(ha_tunnus) AS ha_tunnus,
  ANY_VALUE(hallinnonala) AS hallinnonala,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM {curated_ref}
WHERE COALESCE(momentti_tunnusp, momentti_snimi) IS NOT NULL
GROUP BY momentti_tunnusp, momentti_snimi
""",
        )
    )

    steps.append(
        (
            "dim_alamomentti",
            f"""
CREATE OR REPLACE {create_object} `{project}.{dataset}.dim_alamomentti` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(alamomentti_tunnus, ''), '|', COALESCE(alamomentti_snimi, '')))) AS alamomentti_id,
  alamomentti_tunnus,
  alamomentti_snimi,
  ANY_VALUE(momentti_tunnusp) AS momentti_tunnusp,
  ANY_VALUE(momentti_snimi) AS momentti_snimi,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM {curated_ref}
WHERE COALESCE(alamomentti_tunnus, alamomentti_snimi) IS NOT NULL
GROUP BY alamomentti_tunnus, alamomentti_snimi
""",
        )
    )

    steps.append(
        (
            "dim_hierarchy_name_mapping",
            _build_hierarchy_mapping_sql(project, dataset, curated_ref, create_object),
        )
    )

    steps.append(
        (
            "dq_hierarchy_consistency",
            _build_hierarchy_consistency_sql(project, dataset, curated_ref, create_object),
        )
    )

    steps.append(
        (
            "topic_alias",
            f"""
CREATE OR REPLACE {create_object} `{project}.{dataset}.dim_topic_alias` AS
SELECT * FROM UNNEST([
  STRUCT('korkeakoulutus' AS topic, 'korkeakoulu' AS alias, 'momentti_snimi' AS match_field, '%korkeakoul%' AS like_pattern),
  STRUCT('korkeakoulutus', 'yliopisto', 'momentti_snimi', '%yliopist%'),
  STRUCT('korkeakoulutus', 'ammattikorkeakoulu', 'momentti_snimi', '%ammattikorkeakoul%'),
  STRUCT('korkeakoulutus', 'opetus', 'hallinnonala', '%opetus%'),
  STRUCT('puolustus', 'puolustus', 'hallinnonala', '%puolustus%'),
  STRUCT('sosiaali_ja_terveys', 'sosiaali', 'hallinnonala', '%sosiaali%'),
  STRUCT('sosiaali_ja_terveys', 'terveys', 'hallinnonala', '%terveys%'),
  STRUCT('liikenne', 'liikenne', 'hallinnonala', '%liikenne%'),
  STRUCT('ymparisto', 'ymparisto', 'hallinnonala', '%ympäristö%'),
  STRUCT('ymparisto', 'ymparisto', 'hallinnonala', '%ymparisto%')
])
""",
        )
    )
    return steps


def build_semantic_view_sql(project: str, dataset: str, curated_table: str, semantic_view: str) -> str:
    join_clauses: list[str] = []
    helper_columns: list[str] = []
    canonical_expr_by_prefix: dict[str, str] = {}
    for level_name, code_col, name_col, prefix in _hierarchy_level_specs():
        alias = f"{prefix}_map"
        canonical_expr = f"COALESCE({alias}.canonical_name, source.{prefix}_display, source.{name_col})"
        canonical_expr_by_prefix[prefix] = canonical_expr
        join_clauses.append(
            f"""LEFT JOIN `{project}.{dataset}.dim_hierarchy_name_mapping` AS {alias}
  ON {alias}.level_name = '{level_name}'
 AND {alias}.code = source.{code_col}
 AND {alias}.alias_display_name = source.{prefix}_display
 AND source.vuosi BETWEEN {alias}.valid_from_year AND {alias}.valid_to_year"""
        )
        helper_columns.extend(
            [
                f"  source.{prefix}_display AS {prefix}_display,",
                f"  source.{prefix}_family_key AS {prefix}_family_key,",
                f"  {canonical_expr} AS {prefix}_canonical,",
                (
                    f"  COALESCE({alias}.alias_issue_category, 'canonical') "
                    f"AS {prefix}_alias_issue_category,"
                ),
                (
                    f"  COALESCE({alias}.has_same_year_conflict, FALSE) "
                    f"AS {prefix}_has_same_year_conflict,"
                ),
            ]
        )
    helper_columns_sql = "\n".join(helper_columns)
    joins_sql = "\n".join(join_clauses)
    momentti_canonical_expr = canonical_expr_by_prefix["momentti"]
    alamomentti_canonical_expr = canonical_expr_by_prefix["alamomentti"]
    return f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.{semantic_view}` AS
WITH source AS (
  SELECT *
  FROM `{project}.{dataset}.{curated_table}`
)
SELECT
  -- Raw-compatible names for existing SQL contracts/fallbacks
  source.vuosi AS `Vuosi`,
  source.kk AS `Kk`,
  source.ha_tunnus AS `Ha_Tunnus`,
  source.hallinnonala AS `Hallinnonala`,
  source.tv_tunnus AS `Tv_Tunnus`,
  source.kirjanpitoyksikko AS `Kirjanpitoyksikkö`,
  source.paaluokkaosasto_tunnusp AS `PaaluokkaOsasto_TunnusP`,
  source.paaluokkaosasto_snimi AS `PaaluokkaOsasto_sNimi`,
  source.luku_tunnusp AS `Luku_TunnusP`,
  source.luku_snimi AS `Luku_sNimi`,
  source.momentti_tunnusp AS `Momentti_TunnusP`,
  source.momentti_snimi AS `Momentti_sNimi`,
  source.alamomentti_tunnus AS `TakpMrL_Tunnus`,
  source.alamomentti_snimi AS `TakpMrL_sNimi`,
  source.alkuperainen_talousarvio AS `Alkuperäinen_talousarvio`,
  source.lisatalousarvio AS `Lisätalousarvio`,
  source.voimassaoleva_talousarvio AS `Voimassaoleva_talousarvio`,
  source.kaytettavissa AS `Käytettävissä`,
  source.alkusaldo AS `Alkusaldo`,
  source.nettokertyma_ko_vuodelta AS `Nettokertymä_ko_vuodelta`,
  source.nettokertyma AS `Nettokertymä`,
  source.loppusaldo AS `Loppusaldo`,

  -- Semantic helper columns (named so they do not collide with case-insensitive raw names)
  source.period_date,
  source.kirjanpitoyksikko,
  source.alamomentti_tunnus,
  source.alamomentti_snimi,
  source.nettokertyma,
  source.nettokertyma_ko_vuodelta,
{helper_columns_sql}
  CONCAT(COALESCE(source.momentti_tunnusp, '?'), ' ', COALESCE({momentti_canonical_expr}, '')) AS momentti_label,
  CONCAT(COALESCE(source.alamomentti_tunnus, '?'), ' ', COALESCE({alamomentti_canonical_expr}, '')) AS alamomentti_label,
  source.quality_issue_count,
  source.has_valid_nettokertyma,
  source.row_fingerprint
FROM source
{joins_sql}
WHERE source.is_valid_year
  AND source.is_valid_month
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build BigQuery data quality layer for Budjettihaukka.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--raw-table", default=settings.table)
    parser.add_argument("--curated-table", default="valtiontalous_curated_dq_v")
    parser.add_argument("--semantic-view", default="valtiontalous_semantic_v1")
    parser.add_argument(
        "--build-mode",
        choices=["view", "table"],
        default="view",
        help="Use 'view' for zero-copy free-tier compatible setup; 'table' for materialized curated table.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--render-sql-dir",
        default="",
        help="If set, writes SQL files locally and exits without submitting BigQuery jobs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    curated_sql = build_curated_sql(
        project=args.project,
        dataset=args.dataset,
        raw_table=args.raw_table,
        curated_table=args.curated_table,
        build_mode=args.build_mode,
    )
    dims_sql = build_dimensions_sql(
        project=args.project,
        dataset=args.dataset,
        curated_table=args.curated_table,
        build_mode=args.build_mode,
    )
    semantic_sql = build_semantic_view_sql(
        project=args.project,
        dataset=args.dataset,
        curated_table=args.curated_table,
        semantic_view=args.semantic_view,
    )

    if args.render_sql_dir:
        out_dir = Path(args.render_sql_dir).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "01_curated_table.sql").write_text(curated_sql + "\n", encoding="utf-8")
        for idx, (label, sql) in enumerate(dims_sql, start=2):
            (out_dir / f"{idx:02d}_{label}.sql").write_text(sql + "\n", encoding="utf-8")
        (out_dir / f"{len(dims_sql) + 2:02d}_semantic_view.sql").write_text(
            semantic_sql + "\n",
            encoding="utf-8",
        )
        logger.info("Rendered SQL bundle to %s", out_dir)
        return 0

    client = bigquery.Client(project=args.project)

    try:
        _run_query(
            client,
            curated_sql,
            label=f"curated_{args.build_mode}={args.curated_table}",
            dry_run=args.dry_run,
        )

        for label, sql in dims_sql:
            _run_query(client, sql, label=label, dry_run=args.dry_run)

        _run_query(
            client,
            semantic_sql,
            label=f"semantic_view={args.semantic_view}",
            dry_run=args.dry_run,
        )
    except Forbidden as exc:
        logger.error("Permission error while building DQ layer: %s", exc)
        logger.error(
            "Required IAM on dataset %s.%s: bigquery.tables.create + bigquery.tables.updateData",
            args.project,
            args.dataset,
        )
        return 2

    logger.info(
        "Data quality layer ready: %s.%s.%s + %s.%s.%s",
        args.project,
        args.dataset,
        args.curated_table,
        args.project,
        args.dataset,
        args.semantic_view,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
