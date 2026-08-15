#!/usr/bin/env python3
"""Build a typed/clean data quality layer and dimensions in BigQuery."""

from __future__ import annotations

import argparse
import json
import logging
import re
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


# Osa lähdevuosista on tuplakoodattu: UTF-8-tavut on luettu ISO-8859-10:nä
# (nordic), jolloin ä→'ÃĪ', ö→'Ãķ', Ö→'Ã', Ä→'Ã'. Skannaus
# 2026-07: nämä neljä sekvenssiä kattavat kaikki havaitut tapaukset (405
# nimeä), ja jokainen korjautuu ketjulla encode('iso8859-10').decode('utf-8').
MOJIBAKE_PAIRS = [
    ("\u00c3\u012a", "\u00e4"),  # -> a-umlaut
    ("\u00c3\u0137", "\u00f6"),  # -> o-umlaut
    ("\u00c3\u0096", "\u00d6"),  # -> O-umlaut (C1-kontrollitavu 0x96)
    ("\u00c3\u0084", "\u00c4"),  # -> A-umlaut (C1-kontrollitavu 0x84)
]


def _sql_literal(value: str) -> str:
    escaped = "".join(
        ch if ch.isprintable() and ch not in ("'", "\\") else f"\\u{ord(ch):04x}"
        for ch in value
    )
    return f"'{escaped}'"


def _mojibake_fix_expr(expr: str) -> str:
    for broken, fixed in MOJIBAKE_PAIRS:
        expr = f"REPLACE({expr}, {_sql_literal(broken)}, {_sql_literal(fixed)})"
    return expr


def _clean_text_expr(raw_col: str) -> str:
    return f"NULLIF(REGEXP_REPLACE(TRIM({_mojibake_fix_expr(raw_col)}), r'\\s+', ' '), '')"


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
    ]


def _alamomentti_candidate_expr() -> str:
    """Derive only the code part below an official moment code.

    Example: momentti 27.10.01. + talousarviotili 27.10.01.9.01. -> 9.01.
    This is only a candidate. The semantic view publishes an alamomentti only
    after an exact year-specific match against official_code_registry_v1.
    """
    return (
        "CASE "
        "WHEN REGEXP_CONTAINS(momentti_tunnusp, r'^\\d{2}\\.\\d{2}\\.\\d{2}\\.$') "
        " AND REGEXP_CONTAINS(talousarviotili_tunnusp, "
        "r'^\\d{2}\\.\\d{2}\\.\\d{2}\\.(?:\\d+\\.)+$') "
        " AND STARTS_WITH(talousarviotili_tunnusp, momentti_tunnusp) "
        " AND talousarviotili_tunnusp != momentti_tunnusp "
        "THEN SUBSTR(talousarviotili_tunnusp, LENGTH(momentti_tunnusp) + 1) "
        "ELSE NULL END"
    )


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


def _source_compat_cte(project: str, dataset: str, raw_table: str, raw_naming: str) -> str:
    """Expose the raw table under original Valtiokonttori header names.

    The legacy raw table keeps original headers (`Vuosi`, `Nettokertymä`);
    the ingest pipeline writes ASCII snake_case (`vuosi`, `nettokertyma`).
    The curated SQL below is written against original names, so normalized
    tables get an aliasing layer generated from the committed column map.
    """
    table_ref = f"`{project}.{dataset}.{raw_table}`"
    if raw_naming == "original":
        return f"SELECT * FROM {table_ref}"
    column_map_path = Path(__file__).resolve().parents[1] / "data" / "valtiokonttori_column_map.json"
    column_map: dict[str, str] = json.loads(column_map_path.read_text(encoding="utf-8"))["columns"]
    aliases = ",\n    ".join(
        f"`{normalized}` AS `{original}`" for original, normalized in column_map.items()
    )
    return f"SELECT\n    {aliases}\n  FROM {table_ref}"


def detect_raw_naming(client: "bigquery.Client", project: str, dataset: str, raw_table: str) -> str:
    columns = {field.name for field in client.get_table(f"{project}.{dataset}.{raw_table}").schema}
    return "original" if "Vuosi" in columns else "normalized"


def build_curated_sql(
    project: str,
    dataset: str,
    raw_table: str,
    curated_table: str,
    build_mode: str,
    raw_naming: str = "original",
) -> str:
    def _finalize(sql: str) -> str:
        # Raakatekstikentät kulkevat TRIM(CAST(`X` AS STRING)) -muodossa;
        # sovelletaan mojibake-korjaus jokaiseen niistä.
        return re.sub(
            r"TRIM\(CAST\(`([^`]+)` AS STRING\)\)",
            lambda m: "TRIM(" + _mojibake_fix_expr(f"CAST(`{m.group(1)}` AS STRING)") + ")",
            sql,
        )

    target_ref = f"`{project}.{dataset}.{curated_table}`"
    source_compat = _source_compat_cte(project, dataset, raw_table, raw_naming)
    hierarchy_helper_selects = ",\n  ".join(_hierarchy_helper_selects())
    if build_mode == "table":
        header = (
            f"CREATE OR REPLACE TABLE {target_ref}\n"
            "PARTITION BY period_date\n"
            "CLUSTER BY hallinnonala, momentti_tunnusp, talousarviotili_tunnusp\n"
            "AS"
        )
    else:
        header = f"CREATE OR REPLACE VIEW {target_ref} AS"
    return _finalize(f"""
{header}
WITH source_raw AS (
  {source_compat}
),
normalized AS (
  SELECT
    SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
    SAFE_CAST(`Kk` AS INT64) AS kk,
    DATE(SAFE_CAST(`Vuosi` AS INT64), SAFE_CAST(`Kk` AS INT64), 1) AS period_date,
    NULLIF(TRIM(CAST(`Ha_Tunnus` AS STRING)), '') AS ha_tunnus,
    NULLIF(TRIM(CAST(`Hallinnonala` AS STRING)), '') AS hallinnonala,
    NULLIF(TRIM(CAST(`Tv_Tunnus` AS STRING)), '') AS tv_tunnus,
    NULLIF(TRIM(CAST(`Kirjanpitoyksikkö` AS STRING)), '') AS kirjanpitoyksikko,
    NULLIF(TRIM(CAST(`PaaluokkaOsasto_TunnusP` AS STRING)), '') AS paaluokkaosasto_tunnusp,
    NULLIF(TRIM(CAST(`PaaluokkaOsasto_sNimi` AS STRING)), '') AS paaluokkaosasto_snimi,
    NULLIF(TRIM(CAST(`Luku_TunnusP` AS STRING)), '') AS luku_tunnusp,
    NULLIF(TRIM(CAST(`Luku_sNimi` AS STRING)), '') AS luku_snimi,
    NULLIF(TRIM(CAST(`Momentti_TunnusP` AS STRING)), '') AS momentti_tunnusp,
    NULLIF(TRIM(CAST(`Momentti_sNimi` AS STRING)), '') AS momentti_snimi,
    NULLIF(TRIM(CAST(`TakpT_TunnusP` AS STRING)), '') AS talousarviotili_tunnusp,
    NULLIF(TRIM(CAST(`TakpT_sNimi` AS STRING)), '') AS talousarviotili_snimi,
    NULLIF(TRIM(CAST(`TakpTr_sNimi` AS STRING)), '') AS takptr_snimi,
    NULLIF(TRIM(CAST(`TakpMrL_Tunnus` AS STRING)), '') AS maararahalaji_tunnus,
    NULLIF(TRIM(CAST(`TakpMrL_sNimi` AS STRING)), '') AS maararahalaji_snimi,
    NULLIF(TRIM(CAST(`TakpT_Netto` AS STRING)), '') AS takpt_netto_raw,
    NULLIF(TRIM(CAST(`Tililuokka_Tunnus` AS STRING)), '') AS tililuokka_tunnus,
    NULLIF(TRIM(CAST(`Tililuokka_sNimi` AS STRING)), '') AS tililuokka_snimi,
    NULLIF(TRIM(CAST(`Ylatiliryhma_Tunnus` AS STRING)), '') AS ylatiliryhma_tunnus,
    NULLIF(TRIM(CAST(`Ylatiliryhma_sNimi` AS STRING)), '') AS ylatiliryhma_snimi,
    NULLIF(TRIM(CAST(`Tiliryhma_Tunnus` AS STRING)), '') AS tiliryhma_tunnus,
    NULLIF(TRIM(CAST(`Tiliryhma_sNimi` AS STRING)), '') AS tiliryhma_snimi,
    NULLIF(TRIM(CAST(`Tililaji_Tunnus` AS STRING)), '') AS tililaji_tunnus,
    NULLIF(TRIM(CAST(`Tililaji_sNimi` AS STRING)), '') AS tililaji_snimi,
    NULLIF(TRIM(CAST(`LkpT_Tunnus` AS STRING)), '') AS lkpt_tunnus,
    NULLIF(TRIM(CAST(`LkpT_sNimi` AS STRING)), '') AS lkpt_snimi,
    NULLIF(TRIM(CAST(`Alkuperäinen_talousarvio` AS STRING)), '') AS alkuperainen_talousarvio_raw,
    NULLIF(TRIM(CAST(`Lisätalousarvio` AS STRING)), '') AS lisatalousarvio_raw,
    NULLIF(TRIM(CAST(`Voimassaoleva_talousarvio` AS STRING)), '') AS voimassaoleva_talousarvio_raw,
    NULLIF(TRIM(CAST(`Käytettävissä` AS STRING)), '') AS kaytettavissa_raw,
    NULLIF(TRIM(CAST(`Alkusaldo` AS STRING)), '') AS alkusaldo_raw,
    NULLIF(TRIM(CAST(`Nettokertymä_ko_vuodelta` AS STRING)), '') AS nettokertyma_ko_vuodelta_raw,
    NULLIF(TRIM(CAST(`NettoKertymaAikVuosSiirrt` AS STRING)), '') AS nettokertymaaikvuossiirrt_raw,
    NULLIF(TRIM(CAST(`Nettokertymä` AS STRING)), '') AS nettokertyma_raw,
    NULLIF(TRIM(CAST(`Loppusaldo` AS STRING)), '') AS loppusaldo_raw,
    NULLIF(TRIM(CAST(`JakamatonDb` AS STRING)), '') AS jakamatondb_raw,
    NULLIF(TRIM(CAST(`JakamatonKr` AS STRING)), '') AS jakamatonkr_raw
  FROM source_raw
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
  {_alamomentti_candidate_expr()} AS alamomentti_tunnus_candidate,
  CASE
    WHEN {_alamomentti_candidate_expr()} IS NOT NULL THEN talousarviotili_snimi
    ELSE NULL
  END AS alamomentti_snimi_candidate,
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
  -- Fingerprint must cover EVERY content column: the source export has no
  -- transaction id, and rows may legitimately differ only in accounting-code
  -- dimensions (tililuokka/tiliryhma/lkp). A narrower hash flags such rows
  -- as false duplicates.
  TO_HEX(
    MD5(
      CONCAT(
        COALESCE(CAST(vuosi AS STRING), ''), '|',
        COALESCE(CAST(kk AS STRING), ''), '|',
        COALESCE(ha_tunnus, ''), '|',
        COALESCE(hallinnonala, ''), '|',
        COALESCE(tv_tunnus, ''), '|',
        COALESCE(kirjanpitoyksikko, ''), '|',
        COALESCE(paaluokkaosasto_tunnusp, ''), '|',
        COALESCE(paaluokkaosasto_snimi, ''), '|',
        COALESCE(luku_tunnusp, ''), '|',
        COALESCE(luku_snimi, ''), '|',
        COALESCE(momentti_tunnusp, ''), '|',
        COALESCE(momentti_snimi, ''), '|',
        COALESCE(takptr_snimi, ''), '|',
        COALESCE(talousarviotili_tunnusp, ''), '|',
        COALESCE(talousarviotili_snimi, ''), '|',
        COALESCE(maararahalaji_tunnus, ''), '|',
        COALESCE(maararahalaji_snimi, ''), '|',
        COALESCE(takpt_netto_raw, ''), '|',
        COALESCE(tililuokka_tunnus, ''), '|',
        COALESCE(tililuokka_snimi, ''), '|',
        COALESCE(ylatiliryhma_tunnus, ''), '|',
        COALESCE(ylatiliryhma_snimi, ''), '|',
        COALESCE(tiliryhma_tunnus, ''), '|',
        COALESCE(tiliryhma_snimi, ''), '|',
        COALESCE(tililaji_tunnus, ''), '|',
        COALESCE(tililaji_snimi, ''), '|',
        COALESCE(lkpt_tunnus, ''), '|',
        COALESCE(lkpt_snimi, ''), '|',
        COALESCE(alkuperainen_talousarvio_raw, ''), '|',
        COALESCE(lisatalousarvio_raw, ''), '|',
        COALESCE(voimassaoleva_talousarvio_raw, ''), '|',
        COALESCE(kaytettavissa_raw, ''), '|',
        COALESCE(alkusaldo_raw, ''), '|',
        COALESCE(nettokertyma_ko_vuodelta_raw, ''), '|',
        COALESCE(nettokertymaaikvuossiirrt_raw, ''), '|',
        COALESCE(nettokertyma_raw, ''), '|',
        COALESCE(loppusaldo_raw, ''), '|',
        COALESCE(jakamatondb_raw, ''), '|',
        COALESCE(jakamatonkr_raw, '')
      )
    )
  ) AS row_fingerprint,
  {hierarchy_helper_selects}
FROM typed
WHERE vuosi IS NOT NULL
  AND kk IS NOT NULL
""")


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
            "dim_maararahalaji",
            f"""
CREATE OR REPLACE {create_object} `{project}.{dataset}.dim_maararahalaji` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(maararahalaji_tunnus, ''), '|', COALESCE(maararahalaji_snimi, '')))) AS maararahalaji_id,
  maararahalaji_tunnus,
  maararahalaji_snimi,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM {curated_ref}
WHERE COALESCE(maararahalaji_tunnus, maararahalaji_snimi) IS NOT NULL
GROUP BY maararahalaji_tunnus, maararahalaji_snimi
""",
        )
    )

    steps.append(
        (
            "dim_talousarviotili",
            f"""
CREATE OR REPLACE {create_object} `{project}.{dataset}.dim_talousarviotili` AS
SELECT
  TO_HEX(MD5(CONCAT(COALESCE(talousarviotili_tunnusp, ''), '|', COALESCE(talousarviotili_snimi, '')))) AS talousarviotili_id,
  talousarviotili_tunnusp,
  talousarviotili_snimi,
  ANY_VALUE(momentti_tunnusp) AS momentti_tunnusp,
  ANY_VALUE(momentti_snimi) AS momentti_snimi,
  MIN(vuosi) AS first_year,
  MAX(vuosi) AS last_year,
  COUNT(*) AS row_count
FROM {curated_ref}
WHERE COALESCE(talousarviotili_tunnusp, talousarviotili_snimi) IS NOT NULL
GROUP BY talousarviotili_tunnusp, talousarviotili_snimi
""",
        )
    )

    steps.append(
        (
            "dim_alamomentti",
            f"""
-- Fail closed: this dimension remains empty until the official registry
-- contains year-specific talousarviotili/alamomentti rows.
CREATE OR REPLACE {create_object} `{project}.{dataset}.dim_alamomentti` AS
SELECT DISTINCT
  source.vuosi,
  source.momentti_tunnusp,
  source.talousarviotili_tunnusp,
  source.alamomentti_tunnus_candidate AS alamomentti_tunnus,
  COALESCE(registry.name_fi, source.alamomentti_snimi_candidate) AS alamomentti_snimi,
  registry.code_dotted AS official_code_dotted,
  'official_code_registry_v1' AS validation_source
FROM {curated_ref} AS source
JOIN `{project}.{dataset}.official_code_registry_v1` AS registry
  ON registry.year = source.vuosi
 AND registry.level IN ('talousarviotili', 'alamomentti')
 AND registry.code_dotted = source.talousarviotili_tunnusp
WHERE source.alamomentti_tunnus_candidate IS NOT NULL
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
  source.maararahalaji_tunnus,
  source.maararahalaji_snimi,
  source.talousarviotili_tunnusp,
  source.talousarviotili_snimi,
  source.alamomentti_tunnus_candidate,
  source.alamomentti_snimi_candidate,
  validated_alamomentti.alamomentti_tunnus,
  validated_alamomentti.alamomentti_snimi,
  validated_alamomentti.alamomentti_tunnus IS NOT NULL AS alamomentti_is_validated,
  CASE
    WHEN source.alamomentti_tunnus_candidate IS NULL THEN 'not_derivable'
    WHEN validated_alamomentti.alamomentti_tunnus IS NULL THEN 'not_in_official_chart'
    ELSE 'validated'
  END AS alamomentti_validation_status,
  validated_alamomentti.validation_source AS alamomentti_validation_source,
  source.nettokertyma,
  source.nettokertyma_ko_vuodelta,
{helper_columns_sql}
  CONCAT(COALESCE(source.momentti_tunnusp, '?'), ' ', COALESCE({momentti_canonical_expr}, '')) AS momentti_label,
  CASE
    WHEN validated_alamomentti.alamomentti_tunnus IS NULL THEN NULL
    ELSE CONCAT(validated_alamomentti.alamomentti_tunnus, ' ', COALESCE(validated_alamomentti.alamomentti_snimi, ''))
  END AS alamomentti_label,
  source.quality_issue_count,
  source.has_valid_nettokertyma,
  source.row_fingerprint
FROM source
{joins_sql}
LEFT JOIN `{project}.{dataset}.dim_alamomentti` AS validated_alamomentti
  ON validated_alamomentti.vuosi = source.vuosi
 AND validated_alamomentti.momentti_tunnusp = source.momentti_tunnusp
 AND validated_alamomentti.talousarviotili_tunnusp = source.talousarviotili_tunnusp
WHERE source.is_valid_year
  AND source.is_valid_month
"""


def build_yearly_agg_sql(project: str, dataset: str, semantic_view: str, yearly_agg_table: str) -> str:
    return f"""
CREATE OR REPLACE TABLE `{project}.{dataset}.{yearly_agg_table}`
PARTITION BY RANGE_BUCKET(vuosi, GENERATE_ARRAY(1998, 2026, 1))
CLUSTER BY hallinnonala, momentti_tunnusp, talousarviotili_tunnusp AS
SELECT
  SAFE_CAST(`Vuosi` AS INT64) AS vuosi,
  COALESCE(NULLIF(hallinnonala_canonical, ''), `Hallinnonala`) AS hallinnonala,
  NULLIF(`Ha_Tunnus`, '') AS ha_tunnus,
  NULLIF(`Tv_Tunnus`, '') AS tv_tunnus,
  NULLIF(`Kirjanpitoyksikkö`, '') AS kirjanpitoyksikko,
  NULLIF(`Momentti_TunnusP`, '') AS momentti_tunnusp,
  COALESCE(NULLIF(momentti_canonical, ''), NULLIF(`Momentti_sNimi`, '')) AS momentti_snimi,
  NULLIF(maararahalaji_tunnus, '') AS maararahalaji_tunnus,
  NULLIF(maararahalaji_snimi, '') AS maararahalaji_snimi,
  NULLIF(talousarviotili_tunnusp, '') AS talousarviotili_tunnusp,
  NULLIF(talousarviotili_snimi, '') AS talousarviotili_snimi,
  IF(alamomentti_is_validated, NULLIF(alamomentti_tunnus, ''), NULL) AS alamomentti_tunnus,
  IF(alamomentti_is_validated, NULLIF(alamomentti_snimi, ''), NULL) AS alamomentti_snimi,
  LOGICAL_AND(alamomentti_is_validated) AS alamomentti_is_validated,
  SUM(SAFE_CAST(`Nettokertymä` AS NUMERIC)) AS nettokertyma_sum,
  COUNT(*) AS source_rows
FROM `{project}.{dataset}.{semantic_view}`
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
"""


def build_promotion_sql(project: str, dataset: str, semantic_view: str, alias: str) -> str:
    return f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.{alias}` AS
SELECT * FROM `{project}.{dataset}.{semantic_view}`
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build BigQuery data quality layer for Budjettihaukka.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--raw-table", default=settings.raw_table)
    parser.add_argument("--curated-table", default="valtiontalous_curated_dq_v")
    parser.add_argument(
        "--semantic-version",
        type=int,
        default=1,
        help="Semantic layer version N; builds valtiontalous_semantic_v{N}. Older versions are left in place for rollback.",
    )
    parser.add_argument(
        "--semantic-view",
        default="",
        help="Explicit semantic view name; overrides --semantic-version naming.",
    )
    parser.add_argument(
        "--promote-alias",
        default="valtiontalous_semantic_current",
        help="Stable view alias the app reads (BUDJETTIHAUKKA_TABLE default).",
    )
    parser.add_argument(
        "--no-promote",
        action="store_true",
        help="Build the versioned layer without repointing the promotion alias.",
    )
    parser.add_argument(
        "--promote-only",
        action="store_true",
        help="Only repoint the promotion alias to --semantic-version (rollback/promote without rebuilding).",
    )
    parser.add_argument("--yearly-agg-table", default="valtiontalous_yearly_agg_v1")
    parser.add_argument(
        "--raw-naming",
        choices=["auto", "original", "normalized"],
        default="auto",
        help="Raw table column naming: original Valtiokonttori headers or ingest-normalized snake_case. 'auto' inspects the live table ('original' in --render-sql-dir mode).",
    )
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

    semantic_view = args.semantic_view or f"valtiontalous_semantic_v{args.semantic_version}"
    promotion_sql = build_promotion_sql(
        project=args.project,
        dataset=args.dataset,
        semantic_view=semantic_view,
        alias=args.promote_alias,
    )

    if args.promote_only:
        client = bigquery.Client(project=args.project)
        _run_query(client, promotion_sql, label=f"promote {args.promote_alias} -> {semantic_view}", dry_run=args.dry_run)
        logger.info("Promoted %s.%s.%s -> %s", args.project, args.dataset, args.promote_alias, semantic_view)
        return 0

    raw_naming = args.raw_naming
    if raw_naming == "auto":
        if args.render_sql_dir:
            raw_naming = "original"
        else:
            raw_naming = detect_raw_naming(
                bigquery.Client(project=args.project), args.project, args.dataset, args.raw_table
            )
            logger.info("Detected raw table naming: %s", raw_naming)

    curated_sql = build_curated_sql(
        project=args.project,
        dataset=args.dataset,
        raw_table=args.raw_table,
        curated_table=args.curated_table,
        build_mode=args.build_mode,
        raw_naming=raw_naming,
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
        semantic_view=semantic_view,
    )
    yearly_agg_sql = build_yearly_agg_sql(
        project=args.project,
        dataset=args.dataset,
        semantic_view=semantic_view,
        yearly_agg_table=args.yearly_agg_table,
    )

    if args.render_sql_dir:
        out_dir = Path(args.render_sql_dir).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "01_curated_table.sql").write_text(curated_sql.rstrip() + "\n", encoding="utf-8")
        for idx, (label, sql) in enumerate(dims_sql, start=2):
            (out_dir / f"{idx:02d}_{label}.sql").write_text(sql.rstrip() + "\n", encoding="utf-8")
        next_idx = len(dims_sql) + 2
        (out_dir / f"{next_idx:02d}_semantic_view.sql").write_text(
            semantic_sql.rstrip() + "\n",
            encoding="utf-8",
        )
        (out_dir / f"{next_idx + 1:02d}_yearly_agg.sql").write_text(yearly_agg_sql.rstrip() + "\n", encoding="utf-8")
        if not args.no_promote:
            (out_dir / f"{next_idx + 2:02d}_promote_alias.sql").write_text(promotion_sql.rstrip() + "\n", encoding="utf-8")
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
            label=f"semantic_view={semantic_view}",
            dry_run=args.dry_run,
        )

        _run_query(
            client,
            yearly_agg_sql,
            label=f"yearly_agg={args.yearly_agg_table}",
            dry_run=args.dry_run,
        )

        if not args.no_promote:
            _run_query(
                client,
                promotion_sql,
                label=f"promote {args.promote_alias} -> {semantic_view}",
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
        "Data quality layer ready: %s.%s.%s + %s.%s.%s%s",
        args.project,
        args.dataset,
        args.curated_table,
        args.project,
        args.dataset,
        semantic_view,
        "" if args.no_promote else f" (promoted as {args.promote_alias})",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
