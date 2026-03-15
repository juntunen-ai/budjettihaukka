from __future__ import annotations

import pandas as pd

from utils.analysis_spec_utils import AnalysisSpec
from utils.bigquery_utils import get_budget_moment_evidence
from services.execution_adapter import execute_via_bigquery


def run_analysis(question: str, analysis_spec: AnalysisSpec) -> dict:
    return execute_via_bigquery(question, analysis_spec)


def collect_used_moments(question: str, results_df: pd.DataFrame, analysis_spec: AnalysisSpec, limit: int = 30) -> tuple[list[dict], str | None]:
    evidence = get_budget_moment_evidence(
        question=question,
        results_df=results_df,
        analysis_spec=analysis_spec,
        limit=limit,
    )
    evidence_df = evidence.get("evidence_df")
    if not isinstance(evidence_df, pd.DataFrame) or evidence_df.empty:
        return [], evidence.get("error")
    clean = evidence_df.copy().where(pd.notna(evidence_df), None)
    return clean.to_dict(orient="records"), evidence.get("error")
