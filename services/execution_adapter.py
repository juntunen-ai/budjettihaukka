from __future__ import annotations

from utils.analysis_spec_utils import AnalysisSpec
from utils.bigquery_utils import execute_analysis_spec


def execute_via_bigquery(question: str, analysis_spec: AnalysisSpec) -> dict:
    """Deterministic execution adapter for BigQuery-backed analytics.

    The adapter exposes a stable service-layer entry point while keeping SQL
    generation inside controlled templates and query builders.
    """
    return execute_analysis_spec(question, analysis_spec, allow_llm_query_plan=False)
