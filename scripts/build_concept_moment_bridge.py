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

logger = logging.getLogger("build_concept_moment_bridge")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build concept_moment_bridge_v3 view in BigQuery.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--view-name", default="concept_moment_bridge_v3")
    parser.add_argument("--helper-view-name", default="concept_runtime_scope_v1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def build_bridge_sql(project: str, dataset: str, view_name: str, helper_view_name: str) -> str:
    concept_ref = f"`{project}.{dataset}.{settings.ontology_table_prefix}_concept`"
    alias_ref = f"`{project}.{dataset}.{settings.ontology_table_prefix}_alias`"
    rule_ref = f"`{project}.{dataset}.{settings.ontology_table_prefix}_membership_rule`"
    mapping_ref = f"`{project}.{dataset}.dim_hierarchy_name_mapping`"
    evidence_ref = f"`{project}.{dataset}.vm_budget_semantic_evidence`"
    segments_ref = f"`{project}.{dataset}.vm_budget_document_segments`"
    view_ref = f"`{project}.{dataset}.{view_name}`"
    helper_ref = f"`{project}.{dataset}.{helper_view_name}`"

    return f"""
CREATE OR REPLACE VIEW {view_ref} AS
WITH concept_meta AS (
  SELECT
    concept_id,
    label_fi AS concept_label,
    policy_theme,
    risk_level,
    default_fiscal_side
  FROM {concept_ref}
),
alias_terms AS (
  SELECT DISTINCT concept_id, LOWER(TRIM(alias)) AS alias
  FROM {alias_ref}
  WHERE lang = 'fi'
    AND LENGTH(TRIM(alias)) >= 5
    AND COALESCE(review_status, 'reviewed') != 'blocked'
    AND COALESCE(precision_score, 0.0) >= 0.55
  UNION DISTINCT
  SELECT concept_id, LOWER(TRIM(concept_label)) AS alias
  FROM concept_meta
  WHERE LENGTH(TRIM(concept_label)) >= 5
),
include_rule_base AS (
  SELECT
    concept_id,
    hierarchy_level,
    match_type,
    value,
    valid_from_year,
    valid_to_year,
    confidence,
    rule_id,
    rule_scope
  FROM {rule_ref}
  WHERE rule_scope = 'include'
),
exclude_rule_base AS (
  SELECT
    concept_id,
    hierarchy_level,
    match_type,
    value,
    valid_from_year,
    valid_to_year,
    confidence,
    rule_id,
    rule_scope
  FROM {rule_ref}
  WHERE rule_scope = 'exclude'
),
hierarchy_mapping AS (
  SELECT
    level_name AS hierarchy_level,
    code AS hierarchy_code,
    canonical_name,
    alias_display_name,
    alias_name,
    valid_from_year,
    valid_to_year,
    alias_issue_category,
    family_key_count,
    has_same_year_conflict
  FROM {mapping_ref}
  WHERE level_name IN ('hallinnonala', 'kirjanpitoyksikko', 'luku', 'momentti', 'alamomentti')
),
expanded_rules AS (
  SELECT
    rb.rule_scope,
    rb.concept_id,
    cm.concept_label,
    cm.policy_theme,
    cm.risk_level,
    cm.default_fiscal_side,
    hm.hierarchy_level,
    hm.hierarchy_code,
    hm.canonical_name,
    CASE
      WHEN rb.valid_from_year IS NULL THEN hm.valid_from_year
      WHEN hm.valid_from_year IS NULL THEN rb.valid_from_year
      ELSE GREATEST(rb.valid_from_year, hm.valid_from_year)
    END AS valid_from_year,
    CASE
      WHEN rb.valid_to_year IS NULL THEN hm.valid_to_year
      WHEN hm.valid_to_year IS NULL THEN rb.valid_to_year
      ELSE LEAST(rb.valid_to_year, hm.valid_to_year)
    END AS valid_to_year,
    rb.rule_id,
    rb.confidence AS rule_confidence,
    hm.alias_issue_category,
    hm.has_same_year_conflict
  FROM (
    SELECT * FROM include_rule_base
    UNION ALL
    SELECT * FROM exclude_rule_base
  ) rb
  JOIN concept_meta cm USING(concept_id)
  JOIN hierarchy_mapping hm
    ON rb.hierarchy_level = hm.hierarchy_level
  WHERE (
      (rb.match_type = 'exact_code' AND hm.hierarchy_code = rb.value)
      OR (rb.match_type = 'code_prefix' AND STARTS_WITH(hm.hierarchy_code, rb.value))
      OR (rb.match_type = 'canonical_exact' AND LOWER(hm.canonical_name) = LOWER(rb.value))
      OR (rb.match_type = 'canonical_name_pattern' AND LOWER(hm.canonical_name) LIKE LOWER(rb.value))
      OR (rb.match_type = 'name_pattern' AND (
          LOWER(COALESCE(hm.alias_display_name, '')) LIKE LOWER(rb.value)
          OR LOWER(COALESCE(hm.alias_name, '')) LIKE LOWER(rb.value)
      ))
  )
),
expanded_rules_valid AS (
  SELECT *
  FROM expanded_rules
  WHERE (valid_from_year IS NULL OR valid_to_year IS NULL OR valid_from_year <= valid_to_year)
),
segment_stats AS (
  SELECT
    content_hash,
    ARRAY_LENGTH(IFNULL(momentti_codes, [])) AS momentti_code_count,
    ARRAY_LENGTH(IFNULL(luku_codes, [])) AS luku_code_count,
    ARRAY_LENGTH(IFNULL(osasto_codes, [])) AS osasto_code_count
  FROM {segments_ref}
),
safe_evidence AS (
  SELECT
    e.content_hash,
    e.year,
    e.content_kind,
    e.hierarchy_level,
    e.hierarchy_code,
    e.heading,
    e.snippet,
    e.content_url,
    e.evidence_confidence,
    COALESCE(s.momentti_code_count, 0) AS momentti_code_count,
    COALESCE(s.luku_code_count, 0) AS luku_code_count,
    COALESCE(s.osasto_code_count, 0) AS osasto_code_count,
    CASE
      WHEN e.content_kind IN ('detailed_justification', 'general_justification') THEN 1.00
      WHEN e.hierarchy_level = 'momentti' AND COALESCE(s.momentti_code_count, 0) BETWEEN 1 AND 5 THEN 0.88
      WHEN e.hierarchy_level = 'luku' AND COALESCE(s.luku_code_count, 0) BETWEEN 1 AND 3 THEN 0.78
      WHEN e.hierarchy_level = 'osasto' AND COALESCE(s.osasto_code_count, 0) BETWEEN 1 AND 2 THEN 0.72
      ELSE 0.0
    END AS safety_weight
  FROM {evidence_ref} e
  LEFT JOIN segment_stats s USING(content_hash)
  WHERE e.hierarchy_level IN ('osasto', 'luku', 'momentti')
),
safe_evidence_filtered AS (
  SELECT *
  FROM safe_evidence
  WHERE safety_weight > 0
),
evidence_matches AS (
  SELECT
    alias_match.concept_id,
    cm.concept_label,
    cm.policy_theme,
    cm.risk_level,
    cm.default_fiscal_side,
    CASE
      WHEN se.hierarchy_level = 'osasto' THEN 'hallinnonala'
      ELSE se.hierarchy_level
    END AS hierarchy_level,
    CAST(se.hierarchy_code AS STRING) AS hierarchy_code,
    CAST(NULL AS STRING) AS bridge_display_name,
    MIN(se.year) AS valid_from_year,
    MAX(se.year) AS valid_to_year,
    COUNT(*) AS evidence_hits,
    MAX(se.evidence_confidence * se.safety_weight) AS evidence_match_confidence,
    ARRAY_AGG(DISTINCT se.content_kind IGNORE NULLS) AS evidence_kinds,
    ARRAY_AGG(
      STRUCT(
        se.evidence_confidence * se.safety_weight AS score,
        se.content_url AS content_url,
        se.snippet AS snippet
      )
      ORDER BY se.evidence_confidence * se.safety_weight DESC, se.content_url DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS top_evidence
  FROM alias_terms alias_match
  JOIN concept_meta cm USING(concept_id)
  JOIN safe_evidence_filtered se
    ON LOWER(CONCAT(COALESCE(se.heading, ''), ' ', COALESCE(se.snippet, ''))) LIKE CONCAT('%', alias_match.alias, '%')
  WHERE CAST(se.hierarchy_code AS STRING) IS NOT NULL
    AND TRIM(CAST(se.hierarchy_code AS STRING)) != ''
  GROUP BY alias_match.concept_id, cm.concept_label, cm.policy_theme, cm.risk_level, cm.default_fiscal_side, hierarchy_level, hierarchy_code
),
bridge_candidates AS (
  SELECT
    concept_id,
    concept_label,
    policy_theme,
    risk_level,
    default_fiscal_side,
    hierarchy_level,
    hierarchy_code,
    canonical_name AS bridge_display_name,
    has_same_year_conflict,
    valid_from_year,
    valid_to_year,
    CASE WHEN rule_scope = 'exclude' THEN 'ontology_exclude' ELSE 'ontology_rule' END AS bridge_source,
    CAST(NULL AS INT64) AS evidence_hits,
    CAST(NULL AS FLOAT64) AS evidence_match_confidence,
    rule_confidence,
    rule_scope = 'include' AS has_rule_support,
    rule_scope = 'exclude' AS has_exclude_support,
    CAST(NULL AS STRING) AS sample_content_url,
    CAST(NULL AS STRING) AS sample_snippet
  FROM expanded_rules_valid
  UNION ALL
  SELECT
    concept_id,
    concept_label,
    policy_theme,
    risk_level,
    default_fiscal_side,
    hierarchy_level,
    hierarchy_code,
    bridge_display_name,
    FALSE AS has_same_year_conflict,
    valid_from_year,
    valid_to_year,
    'vm_evidence' AS bridge_source,
    evidence_hits,
    evidence_match_confidence,
    CAST(NULL AS FLOAT64) AS rule_confidence,
    FALSE AS has_rule_support,
    FALSE AS has_exclude_support,
    top_evidence.content_url AS sample_content_url,
    top_evidence.snippet AS sample_snippet
  FROM evidence_matches
),
bridge_aggregated AS (
  SELECT
    concept_id,
    concept_label,
    policy_theme,
    risk_level,
    default_fiscal_side,
    hierarchy_level,
    hierarchy_code,
    bridge_display_name,
    has_same_year_conflict,
    MIN(valid_from_year) AS valid_from_year,
    MAX(valid_to_year) AS valid_to_year,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT bridge_source ORDER BY bridge_source), ', ') AS bridge_sources,
    SUM(COALESCE(evidence_hits, 0)) AS evidence_hits,
    LOGICAL_OR(has_rule_support) AS has_rule_support,
    LOGICAL_OR(has_exclude_support) AS has_exclude_support,
    MAX(COALESCE(rule_confidence, 0.0)) AS max_rule_confidence,
    MAX(COALESCE(evidence_match_confidence, 0.0)) AS max_evidence_confidence,
    ARRAY_AGG(sample_content_url IGNORE NULLS ORDER BY sample_content_url DESC LIMIT 1)[SAFE_OFFSET(0)] AS sample_content_url,
    ARRAY_AGG(sample_snippet IGNORE NULLS ORDER BY sample_snippet DESC LIMIT 1)[SAFE_OFFSET(0)] AS sample_snippet
  FROM bridge_candidates
  GROUP BY concept_id, concept_label, policy_theme, risk_level, default_fiscal_side, hierarchy_level, hierarchy_code, bridge_display_name, has_same_year_conflict
)
SELECT
  concept_id,
  concept_label,
  policy_theme,
  risk_level,
  default_fiscal_side,
  hierarchy_level,
  hierarchy_code,
  bridge_display_name,
  has_same_year_conflict,
  valid_from_year,
  valid_to_year,
  bridge_sources,
  evidence_hits,
  evidence_hits AS evidence_count,
  has_rule_support,
  has_exclude_support,
  CASE
    WHEN has_exclude_support THEN 'exclude'
    WHEN has_rule_support AND max_rule_confidence >= 0.95 AND evidence_hits >= 1 THEN 'direct'
    WHEN has_rule_support THEN 'composite'
    ELSE 'proxy'
  END AS membership_type,
  CASE
    WHEN has_rule_support THEN LEAST(0.995, GREATEST(max_rule_confidence, 0.82 + 0.02 * evidence_hits))
    ELSE LEAST(0.89, 0.45 + 0.08 * evidence_hits)
  END AS bridge_confidence,
  hierarchy_level IN ('hallinnonala', 'kirjanpitoyksikko', 'momentti', 'alamomentti')
    AND (
      has_rule_support
      OR (evidence_hits >= 2 AND max_evidence_confidence >= 0.92)
    )
    AND NOT has_exclude_support AS recommended_for_runtime,
  CASE
    WHEN has_exclude_support THEN 'Konseptin eksplisiittinen poissulku ontologiasta.'
    WHEN has_rule_support AND evidence_hits >= 1 THEN 'Suora konseptituki ontologiasta ja VM-evidenssistä.'
    WHEN has_rule_support THEN 'Ontologiatuettu koottu budjettimomentti.'
    ELSE 'VM-evidenssiin perustuva proxy-ehdokas, joka vaatii varovaisuutta.'
  END AS notes,
  sample_content_url,
  sample_snippet
FROM bridge_aggregated
;

CREATE OR REPLACE VIEW {helper_ref} AS
SELECT *
FROM {view_ref}
WHERE recommended_for_runtime = TRUE
  AND membership_type != 'exclude'
"""


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sql = build_bridge_sql(args.project, args.dataset, args.view_name, args.helper_view_name)
    cmd = [
        "bq",
        "query",
        "--project_id",
        args.project,
        "--nouse_legacy_sql",
    ]
    if args.dry_run:
        cmd.append("--dry_run")
    subprocess.run(cmd, input=sql, text=True, check=True)
    if args.dry_run:
        logger.info(
            "Dry-run completed for %s.%s.%s (+ %s)",
            args.project,
            args.dataset,
            args.view_name,
            args.helper_view_name,
        )
        return
    logger.info(
        "Built views %s.%s.%s and %s.%s.%s",
        args.project,
        args.dataset,
        args.view_name,
        args.project,
        args.dataset,
        args.helper_view_name,
    )


if __name__ == "__main__":
    main()
