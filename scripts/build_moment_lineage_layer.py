#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings
from utils.budget_semantics import fiscal_side_case_sql

logger = logging.getLogger("build_moment_lineage_layer")

TOKEN_STOPWORDS = [
    "valtion",
    "valtionosuus",
    "momentti",
    "määräraha",
    "maararaha",
    "rahoitus",
    "toiminta",
    "toimintaan",
    "menot",
    "menojen",
    "tulot",
    "tuet",
    "tuki",
    "avustus",
    "maksu",
    "maksut",
    "yleiset",
    "yhteiset",
    "muut",
    "eräät",
    "erityiset",
    "sekä",
    "joka",
    "joiden",
    "vuonna",
    "vuosina",
    "palvelut",
    "palvelu",
    "kehittäminen",
    "kehittaminen",
    "käyttö",
    "kaytto",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build historical moment lineage views in BigQuery.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _sql_string_array(values: list[str]) -> str:
    return "[" + ", ".join("'" + value.replace("'", "\\'") + "'" for value in values) + "]"


def _run_bq_query(sql: str, *, project: str, dry_run: bool = False) -> None:
    cmd = ["bq", f"--project_id={project}", "query", "--nouse_legacy_sql"]
    if dry_run:
        cmd.append("--dry_run")
    logger.info("Running bq query (%s)", "dry-run" if dry_run else "apply")
    result = subprocess.run(cmd, input=sql, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout or "bq query failed")


def _is_known_dry_run_dependency_error(error_text: str, built_names: list[str], dataset: str) -> bool:
    lower = (error_text or "").lower()
    if "not found: table" not in lower:
        return False
    return any(f"{dataset}.{name}".lower() in lower for name in built_names)


def build_sql_map(project: str, dataset: str) -> dict[str, str]:
    stopwords = _sql_string_array(TOKEN_STOPWORDS)
    yearly_base_ref = f"`{project}.{dataset}.valtiontalous_yearly_agg_v1`"
    evidence_ref = f"`{project}.{dataset}.vm_budget_semantic_evidence`"
    lineage_ref = f"`{project}.{dataset}.moment_lineage_v1`"
    guardrail_ref = f"`{project}.{dataset}.moment_structural_change_guardrails_v1`"

    moment_context_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.moment_semantic_context_v1` AS
WITH tokenized AS (
  SELECT
    hierarchy_code AS momentti_tunnusp,
    content_url,
    evidence_confidence,
    token
  FROM {evidence_ref},
  UNNEST(REGEXP_EXTRACT_ALL(LOWER(CONCAT(COALESCE(heading, ''), ' ', COALESCE(snippet, ''))), r'[a-zåäö]{{4,}}')) AS token
  WHERE hierarchy_level = 'momentti'
    AND hierarchy_code IS NOT NULL
    AND TRIM(hierarchy_code) != ''
    AND token NOT IN UNNEST({stopwords})
),
aggregated AS (
  SELECT
    momentti_tunnusp,
    ARRAY_AGG(DISTINCT token ORDER BY token) AS context_tokens,
    COUNT(*) AS token_rows,
    COUNT(DISTINCT content_url) AS evidence_urls,
    MAX(evidence_confidence) AS max_evidence_confidence,
    ARRAY_AGG(DISTINCT content_url ORDER BY content_url LIMIT 3) AS sample_urls
  FROM tokenized
  GROUP BY momentti_tunnusp
)
SELECT *
FROM aggregated
"""

    fiscal_side_expr = fiscal_side_case_sql(
        code_expr="momentti_tunnusp",
        name_expr="momentti_snimi",
        hallinnonala_expr="hallinnonala",
    )

    node_catalog_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.moment_node_catalog_v1` AS
WITH yearly AS (
  SELECT
    vuosi,
    hallinnonala,
    ha_tunnus,
    momentti_tunnusp,
    momentti_snimi,
    nettokertyma_sum,
    {fiscal_side_expr} AS fiscal_side
  FROM {yearly_base_ref}
  WHERE momentti_tunnusp IS NOT NULL
    AND TRIM(momentti_tunnusp) != ''
    AND momentti_snimi IS NOT NULL
    AND TRIM(momentti_snimi) != ''
),
filtered AS (
  SELECT
    CONCAT(momentti_tunnusp, '||', LOWER(TRIM(momentti_snimi))) AS moment_node_id,
    vuosi,
    hallinnonala,
    ha_tunnus,
    momentti_tunnusp,
    momentti_snimi,
    nettokertyma_sum,
    fiscal_side,
    REGEXP_EXTRACT(momentti_tunnusp, r'^(\\d+\\.\\d+\\.)') AS luku_prefix,
    ARRAY(
      SELECT DISTINCT token
      FROM UNNEST(REGEXP_EXTRACT_ALL(LOWER(momentti_snimi), r'[a-zåäö]{{4,}}')) AS token
      WHERE token NOT IN UNNEST({stopwords})
      ORDER BY token
    ) AS name_tokens
  FROM yearly
  WHERE fiscal_side != 'technical'
    AND LOWER(momentti_tunnusp) != 'tapahtumia'
    AND LOWER(momentti_snimi) NOT LIKE '%vain liikekirjanpidossa%'
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY moment_node_id ORDER BY vuosi ASC, ABS(nettokertyma_sum) DESC) AS rn_first,
    ROW_NUMBER() OVER (PARTITION BY moment_node_id ORDER BY vuosi DESC, ABS(nettokertyma_sum) DESC) AS rn_last,
    ROW_NUMBER() OVER (PARTITION BY moment_node_id ORDER BY ABS(nettokertyma_sum) DESC, vuosi DESC) AS rn_peak
  FROM filtered
)
SELECT
  r.moment_node_id,
  r.momentti_tunnusp,
  r.momentti_snimi,
  ARRAY_AGG(r.hallinnonala IGNORE NULLS ORDER BY r.vuosi DESC LIMIT 1)[SAFE_OFFSET(0)] AS hallinnonala,
  ARRAY_AGG(r.ha_tunnus IGNORE NULLS ORDER BY r.vuosi DESC LIMIT 1)[SAFE_OFFSET(0)] AS ha_tunnus,
  ARRAY_AGG(r.fiscal_side IGNORE NULLS ORDER BY r.vuosi DESC LIMIT 1)[SAFE_OFFSET(0)] AS fiscal_side,
  ARRAY_AGG(r.luku_prefix IGNORE NULLS ORDER BY r.vuosi DESC LIMIT 1)[SAFE_OFFSET(0)] AS luku_prefix,
  ANY_VALUE(r.name_tokens) AS name_tokens,
  ARRAY_TO_STRING(ANY_VALUE(r.name_tokens), ' ') AS name_signature,
  MIN(r.vuosi) AS first_year,
  MAX(r.vuosi) AS last_year,
  COUNT(DISTINCT r.vuosi) AS active_years,
  ARRAY_AGG(DISTINCT r.vuosi ORDER BY r.vuosi) AS active_years_list,
  MAX(IF(r.rn_first = 1, r.nettokertyma_sum, NULL)) AS first_year_value,
  MAX(IF(r.rn_last = 1, r.nettokertyma_sum, NULL)) AS last_year_value,
  MAX(IF(r.rn_peak = 1, r.nettokertyma_sum, NULL)) AS peak_value,
  MAX(IF(r.rn_peak = 1, r.vuosi, NULL)) AS peak_year,
  COUNT(*) AS yearly_rows,
  ctx.context_tokens,
  ctx.token_rows AS context_token_rows,
  ctx.evidence_urls,
  ctx.max_evidence_confidence,
  ctx.sample_urls
FROM ranked r
LEFT JOIN `{project}.{dataset}.moment_semantic_context_v1` ctx
  ON r.momentti_tunnusp = ctx.momentti_tunnusp
GROUP BY
  r.moment_node_id,
  r.momentti_tunnusp,
  r.momentti_snimi,
  ctx.context_tokens,
  ctx.token_rows,
  ctx.evidence_urls,
  ctx.max_evidence_confidence,
  ctx.sample_urls
"""

    lineage_candidates_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.moment_lineage_candidates_v1` AS
WITH nodes AS (
  SELECT *
  FROM `{project}.{dataset}.moment_node_catalog_v1`
  WHERE first_year IS NOT NULL
    AND last_year IS NOT NULL
),
pairs AS (
  SELECT
    src.moment_node_id AS source_node_id,
    src.momentti_tunnusp AS source_momentti_tunnusp,
    src.momentti_snimi AS source_momentti_snimi,
    src.hallinnonala AS source_hallinnonala,
    src.ha_tunnus AS source_ha_tunnus,
    src.fiscal_side AS source_fiscal_side,
    src.luku_prefix AS source_luku_prefix,
    src.last_year AS source_last_year,
    src.last_year_value AS source_last_value,
    src.active_years AS source_active_years,
    src.name_tokens AS source_name_tokens,
    src.context_tokens AS source_context_tokens,
    tgt.moment_node_id AS target_node_id,
    tgt.momentti_tunnusp AS target_momentti_tunnusp,
    tgt.momentti_snimi AS target_momentti_snimi,
    tgt.hallinnonala AS target_hallinnonala,
    tgt.ha_tunnus AS target_ha_tunnus,
    tgt.fiscal_side AS target_fiscal_side,
    tgt.luku_prefix AS target_luku_prefix,
    tgt.first_year AS target_first_year,
    tgt.first_year_value AS target_first_value,
    tgt.active_years AS target_active_years,
    tgt.name_tokens AS target_name_tokens,
    tgt.context_tokens AS target_context_tokens,
    tgt.first_year - src.last_year AS year_gap,
    src.momentti_tunnusp = tgt.momentti_tunnusp AS same_code,
    src.ha_tunnus = tgt.ha_tunnus AS same_hallinnonala,
    src.luku_prefix = tgt.luku_prefix AS same_luku_prefix,
    LOWER(TRIM(src.momentti_snimi)) = LOWER(TRIM(tgt.momentti_snimi)) AS exact_name_match,
    ARRAY(
      SELECT DISTINCT tok
      FROM UNNEST(IFNULL(src.name_tokens, [])) AS tok
      WHERE tok IN UNNEST(IFNULL(tgt.name_tokens, []))
      ORDER BY tok
    ) AS shared_name_tokens,
    ARRAY(
      SELECT DISTINCT tok
      FROM UNNEST(IFNULL(src.context_tokens, [])) AS tok
      WHERE tok IN UNNEST(IFNULL(tgt.context_tokens, []))
      ORDER BY tok
    ) AS shared_context_tokens,
    SAFE_DIVIDE(
      LEAST(ABS(COALESCE(src.last_year_value, 0)), ABS(COALESCE(tgt.first_year_value, 0))),
      NULLIF(GREATEST(ABS(COALESCE(src.last_year_value, 0)), ABS(COALESCE(tgt.first_year_value, 0))), 0)
    ) AS amount_ratio
  FROM nodes src
  JOIN nodes tgt
    ON src.moment_node_id != tgt.moment_node_id
   AND src.fiscal_side = tgt.fiscal_side
   AND src.last_year <= tgt.first_year
   AND tgt.first_year BETWEEN src.last_year AND src.last_year + 2
),
scored AS (
  SELECT
    *,
    ARRAY_LENGTH(shared_name_tokens) AS shared_name_token_count,
    ARRAY_LENGTH(
      ARRAY(
        SELECT DISTINCT tok
        FROM UNNEST(ARRAY_CONCAT(IFNULL(source_name_tokens, []), IFNULL(target_name_tokens, []))) AS tok
        ORDER BY tok
      )
    ) AS union_name_token_count,
    ARRAY_LENGTH(shared_context_tokens) AS shared_context_token_count,
    ARRAY_LENGTH(
      ARRAY(
        SELECT DISTINCT tok
        FROM UNNEST(ARRAY_CONCAT(IFNULL(source_context_tokens, []), IFNULL(target_context_tokens, []))) AS tok
        ORDER BY tok
      )
    ) AS union_context_token_count
  FROM pairs
),
filtered AS (
  SELECT
    *,
    SAFE_DIVIDE(shared_name_token_count, NULLIF(union_name_token_count, 0)) AS name_token_jaccard,
    SAFE_DIVIDE(shared_context_token_count, NULLIF(union_context_token_count, 0)) AS context_token_jaccard,
    (
      IF(exact_name_match, 0.42, 0.0) +
      0.30 * SAFE_DIVIDE(shared_name_token_count, NULLIF(union_name_token_count, 0)) +
      0.12 * SAFE_DIVIDE(shared_context_token_count, NULLIF(union_context_token_count, 0)) +
      IF(same_hallinnonala, 0.08, 0.0) +
      IF(same_luku_prefix, 0.08, 0.0) +
      IF(same_code, 0.10, 0.0) +
      0.15 * COALESCE(amount_ratio, 0.0) +
      IF(year_gap = 0, 0.08, IF(year_gap = 1, 0.04, 0.0))
    ) AS candidate_confidence
  FROM scored
  WHERE exact_name_match
     OR SAFE_DIVIDE(shared_name_token_count, NULLIF(union_name_token_count, 0)) >= 0.34
     OR SAFE_DIVIDE(shared_context_token_count, NULLIF(union_context_token_count, 0)) >= 0.25
)
SELECT
  *,
  ROW_NUMBER() OVER (
    PARTITION BY source_node_id
    ORDER BY candidate_confidence DESC, same_code DESC, exact_name_match DESC, same_hallinnonala DESC, target_first_year ASC, target_momentti_tunnusp ASC
  ) AS source_rank,
  ROW_NUMBER() OVER (
    PARTITION BY target_node_id
    ORDER BY candidate_confidence DESC, same_code DESC, exact_name_match DESC, same_hallinnonala DESC, source_last_year DESC, source_momentti_tunnusp ASC
  ) AS target_rank
FROM filtered
WHERE candidate_confidence >= 0.42
"""

    lineage_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.moment_lineage_v1` AS
WITH candidates AS (
  SELECT *
  FROM `{project}.{dataset}.moment_lineage_candidates_v1`
),
direct_edges AS (
  SELECT
    CONCAT(source_node_id, '->', target_node_id) AS relation_id,
    source_node_id,
    target_node_id,
    CASE
      WHEN same_code THEN 'rename'
      WHEN exact_name_match AND same_hallinnonala AND same_luku_prefix THEN 'rename'
      WHEN exact_name_match THEN 'moved'
      WHEN name_token_jaccard >= 0.78 AND same_hallinnonala AND same_luku_prefix THEN 'rename'
      ELSE 'moved'
    END AS relation_type,
    source_momentti_tunnusp,
    source_momentti_snimi,
    source_hallinnonala,
    source_ha_tunnus,
    source_last_year,
    source_last_value,
    target_momentti_tunnusp,
    target_momentti_snimi,
    target_hallinnonala,
    target_ha_tunnus,
    target_first_year,
    target_first_value,
    source_fiscal_side AS fiscal_side,
    year_gap,
    same_code,
    same_hallinnonala,
    same_luku_prefix,
    exact_name_match,
    shared_name_tokens,
    shared_context_tokens,
    name_token_jaccard,
    context_token_jaccard,
    amount_ratio,
    candidate_confidence AS lineage_confidence,
    'direct' AS lineage_basis,
    TRUE AS recommended_for_guardrail
  FROM candidates
  WHERE source_rank = 1
    AND target_rank = 1
    AND candidate_confidence >= 0.62
),
remaining AS (
  SELECT c.*
  FROM candidates c
  LEFT JOIN direct_edges d
    ON c.source_node_id = d.source_node_id
   OR c.target_node_id = d.target_node_id
  WHERE d.relation_id IS NULL
    AND c.candidate_confidence >= 0.50
    AND c.year_gap <= 1
),
split_groups AS (
  SELECT
    source_node_id,
    source_momentti_tunnusp,
    source_momentti_snimi,
    source_hallinnonala,
    source_ha_tunnus,
    source_last_year,
    source_last_value,
    source_fiscal_side,
    COUNT(DISTINCT target_node_id) AS target_count,
    AVG(candidate_confidence) AS avg_confidence,
    SAFE_DIVIDE(
      SUM(ABS(COALESCE(target_first_value, 0))),
      NULLIF(ABS(MAX(COALESCE(source_last_value, 0))), 0)
    ) AS amount_coverage
  FROM remaining
  GROUP BY source_node_id, source_momentti_tunnusp, source_momentti_snimi, source_hallinnonala, source_ha_tunnus, source_last_year, source_last_value, source_fiscal_side
  HAVING target_count BETWEEN 2 AND 4
     AND avg_confidence >= 0.55
     AND amount_coverage BETWEEN 0.45 AND 1.55
),
split_edges AS (
  SELECT
    CONCAT(r.source_node_id, '->', r.target_node_id, '::split') AS relation_id,
    r.source_node_id,
    r.target_node_id,
    'split' AS relation_type,
    r.source_momentti_tunnusp,
    r.source_momentti_snimi,
    r.source_hallinnonala,
    r.source_ha_tunnus,
    r.source_last_year,
    r.source_last_value,
    r.target_momentti_tunnusp,
    r.target_momentti_snimi,
    r.target_hallinnonala,
    r.target_ha_tunnus,
    r.target_first_year,
    r.target_first_value,
    r.source_fiscal_side AS fiscal_side,
    r.year_gap,
    r.same_code,
    r.same_hallinnonala,
    r.same_luku_prefix,
    r.exact_name_match,
    r.shared_name_tokens,
    r.shared_context_tokens,
    r.name_token_jaccard,
    r.context_token_jaccard,
    r.amount_ratio,
    LEAST(0.98, GREATEST(r.candidate_confidence, sg.avg_confidence)) AS lineage_confidence,
    'split' AS lineage_basis,
    TRUE AS recommended_for_guardrail
  FROM remaining r
  JOIN split_groups sg USING(source_node_id, source_momentti_tunnusp, source_momentti_snimi, source_hallinnonala, source_ha_tunnus, source_last_year, source_last_value, source_fiscal_side)
),
merge_groups AS (
  SELECT
    target_node_id,
    target_momentti_tunnusp,
    target_momentti_snimi,
    target_hallinnonala,
    target_ha_tunnus,
    target_first_year,
    target_first_value,
    target_fiscal_side,
    COUNT(DISTINCT source_node_id) AS source_count,
    AVG(candidate_confidence) AS avg_confidence,
    SAFE_DIVIDE(
      SUM(ABS(COALESCE(source_last_value, 0))),
      NULLIF(ABS(MAX(COALESCE(target_first_value, 0))), 0)
    ) AS amount_coverage
  FROM remaining
  GROUP BY target_node_id, target_momentti_tunnusp, target_momentti_snimi, target_hallinnonala, target_ha_tunnus, target_first_year, target_first_value, target_fiscal_side
  HAVING source_count BETWEEN 2 AND 4
     AND avg_confidence >= 0.55
     AND amount_coverage BETWEEN 0.45 AND 1.55
),
merge_edges AS (
  SELECT
    CONCAT(r.source_node_id, '->', r.target_node_id, '::merge') AS relation_id,
    r.source_node_id,
    r.target_node_id,
    'merge' AS relation_type,
    r.source_momentti_tunnusp,
    r.source_momentti_snimi,
    r.source_hallinnonala,
    r.source_ha_tunnus,
    r.source_last_year,
    r.source_last_value,
    r.target_momentti_tunnusp,
    r.target_momentti_snimi,
    r.target_hallinnonala,
    r.target_ha_tunnus,
    r.target_first_year,
    r.target_first_value,
    r.source_fiscal_side AS fiscal_side,
    r.year_gap,
    r.same_code,
    r.same_hallinnonala,
    r.same_luku_prefix,
    r.exact_name_match,
    r.shared_name_tokens,
    r.shared_context_tokens,
    r.name_token_jaccard,
    r.context_token_jaccard,
    r.amount_ratio,
    LEAST(0.98, GREATEST(r.candidate_confidence, mg.avg_confidence)) AS lineage_confidence,
    'merge' AS lineage_basis,
    TRUE AS recommended_for_guardrail
  FROM remaining r
  JOIN merge_groups mg USING(target_node_id, target_momentti_tunnusp, target_momentti_snimi, target_hallinnonala, target_ha_tunnus, target_first_year, target_first_value, target_fiscal_side)
),
unioned AS (
  SELECT * FROM direct_edges
  UNION ALL
  SELECT * FROM split_edges
  UNION ALL
  SELECT * FROM merge_edges
),
deduped AS (
  SELECT
    *,
    CASE relation_type WHEN 'rename' THEN 1 WHEN 'moved' THEN 2 WHEN 'split' THEN 3 WHEN 'merge' THEN 4 ELSE 9 END AS precedence
  FROM unioned
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_node_id, target_node_id
    ORDER BY
      CASE relation_type WHEN 'rename' THEN 1 WHEN 'moved' THEN 2 WHEN 'split' THEN 3 WHEN 'merge' THEN 4 ELSE 9 END,
      lineage_confidence DESC,
      relation_id ASC
  ) = 1
)
SELECT
  relation_id,
  relation_type,
  source_node_id,
  source_momentti_tunnusp,
  source_momentti_snimi,
  source_hallinnonala,
  source_ha_tunnus,
  source_last_year,
  source_last_value,
  target_node_id,
  target_momentti_tunnusp,
  target_momentti_snimi,
  target_hallinnonala,
  target_ha_tunnus,
  target_first_year,
  target_first_value,
  fiscal_side,
  year_gap,
  same_code,
  same_hallinnonala,
  same_luku_prefix,
  exact_name_match,
  shared_name_tokens,
  shared_context_tokens,
  name_token_jaccard,
  context_token_jaccard,
  amount_ratio,
  lineage_confidence,
  lineage_basis,
  recommended_for_guardrail,
  CONCAT(
    relation_type,
    ': ',
    source_momentti_tunnusp, ' ', source_momentti_snimi,
    ' -> ',
    target_momentti_tunnusp, ' ', target_momentti_snimi
  ) AS lineage_note
FROM deduped
"""

    guardrail_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.moment_structural_change_guardrails_v1` AS
WITH base AS (
  SELECT *
  FROM {lineage_ref}
  WHERE recommended_for_guardrail = TRUE
    AND lineage_confidence >= 0.70
),
source_events AS (
  SELECT
    relation_id,
    relation_type,
    'source' AS event_role,
    source_node_id AS moment_node_id,
    source_momentti_tunnusp AS momentti_tunnusp,
    source_momentti_snimi AS momentti_snimi,
    source_hallinnonala AS hallinnonala,
    source_last_year AS event_year,
    target_momentti_tunnusp AS counterpart_momentti_tunnusp,
    target_momentti_snimi AS counterpart_momentti_snimi,
    target_first_year AS transition_counterpart_year,
    fiscal_side,
    lineage_confidence,
    relation_type IN ('rename', 'moved', 'split', 'merge') AS should_exclude_from_change_rankings,
    lineage_note AS guardrail_note
  FROM base
),
target_events AS (
  SELECT
    relation_id,
    relation_type,
    'target' AS event_role,
    target_node_id AS moment_node_id,
    target_momentti_tunnusp AS momentti_tunnusp,
    target_momentti_snimi AS momentti_snimi,
    target_hallinnonala AS hallinnonala,
    target_first_year AS event_year,
    source_momentti_tunnusp AS counterpart_momentti_tunnusp,
    source_momentti_snimi AS counterpart_momentti_snimi,
    source_last_year AS transition_counterpart_year,
    fiscal_side,
    lineage_confidence,
    relation_type IN ('rename', 'moved', 'split', 'merge') AS should_exclude_from_change_rankings,
    lineage_note AS guardrail_note
  FROM base
)
SELECT * FROM source_events
UNION ALL
SELECT * FROM target_events
"""

    guarded_yearly_sql = f"""
CREATE OR REPLACE VIEW `{project}.{dataset}.valtiontalous_yearly_agg_guarded_v1` AS
SELECT
  y.vuosi,
  y.hallinnonala,
  y.ha_tunnus,
  y.tv_tunnus,
  y.kirjanpitoyksikko,
  y.momentti_tunnusp,
  y.momentti_snimi,
  y.alamomentti_tunnus,
  y.alamomentti_snimi,
  y.nettokertyma_sum,
  y.source_rows,
  COALESCE(LOGICAL_OR(g.should_exclude_from_change_rankings), FALSE) AS has_structural_guardrail,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT g.relation_type IGNORE NULLS ORDER BY g.relation_type), ', ') AS structural_relation_types,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT g.event_role IGNORE NULLS ORDER BY g.event_role), ', ') AS structural_event_roles,
  MAX(g.lineage_confidence) AS structural_guardrail_confidence,
  ARRAY_AGG(DISTINCT g.guardrail_note IGNORE NULLS ORDER BY g.guardrail_note LIMIT 3) AS structural_guardrail_notes
FROM {yearly_base_ref} y
LEFT JOIN {guardrail_ref} g
  ON y.momentti_tunnusp = g.momentti_tunnusp
 AND y.vuosi = g.event_year
GROUP BY
  y.vuosi,
  y.hallinnonala,
  y.ha_tunnus,
  y.tv_tunnus,
  y.kirjanpitoyksikko,
  y.momentti_tunnusp,
  y.momentti_snimi,
  y.alamomentti_tunnus,
  y.alamomentti_snimi,
  y.nettokertyma_sum,
  y.source_rows
"""

    return {
        "moment_semantic_context_v1": moment_context_sql,
        "moment_node_catalog_v1": node_catalog_sql,
        "moment_lineage_candidates_v1": lineage_candidates_sql,
        "moment_lineage_v1": lineage_sql,
        "moment_structural_change_guardrails_v1": guardrail_sql,
        "valtiontalous_yearly_agg_guarded_v1": guarded_yearly_sql,
    }


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sql_map = build_sql_map(args.project, args.dataset)
    built_names: list[str] = []
    for name, sql in sql_map.items():
        logger.info("Building view: %s", name)
        try:
            _run_bq_query(sql, project=args.project, dry_run=args.dry_run)
        except RuntimeError as exc:
            if args.dry_run and _is_known_dry_run_dependency_error(str(exc), built_names, args.dataset):
                logger.warning("Skipping dry-run dependency error for %s: %s", name, exc)
                built_names.append(name)
                continue
            raise
        built_names.append(name)
    logger.info("Built %s lineage views%s", len(sql_map), " (dry-run)" if args.dry_run else "")


if __name__ == "__main__":
    main()
