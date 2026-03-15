from __future__ import annotations

from typing import Any

import pandas as pd

from domain.contracts import AnalyticsFrame
from utils.analysis_spec_utils import AnalysisSpec
from utils.semantic_query_contracts import normalize_contract_result


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    clean = df.copy().where(pd.notna(df), None)
    return clean.to_dict(orient="records")


def _frame_type(spec: AnalysisSpec, query_contract: str | None, df: pd.DataFrame) -> str:
    if query_contract in {"top_growth_moment", "top_growth_alamoment"} or spec.intent == "top_growth":
        return "ranking"
    if spec.intent in {"trend", "growth", "seasonality", "overview", "composition"} and "vuosi" in df.columns:
        return "time_series"
    return "table"


def build_analytics_frame(df: pd.DataFrame, spec: AnalysisSpec, query_contract: str | None) -> AnalyticsFrame:
    canonical = normalize_contract_result(df, query_contract, spec) if query_contract else pd.DataFrame()
    if canonical is not None and not canonical.empty:
        return AnalyticsFrame(
            frame_type=_frame_type(spec, query_contract, canonical),
            columns=list(canonical.columns),
            rows=_records(canonical),
            row_count=len(canonical),
        )

    work = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    selected_cols: list[str] = []
    for candidate in ("vuosi", "kk", "hallinnonala", "kirjanpitoyksikko", "momentti_tunnusp", "momentti_snimi", "alamomentti_tunnus", "alamomentti_snimi", "nettokertyma_sum", "muutos_eur", "muutos_pct", "kasvu_eur", "kasvu_pct"):
        if candidate in work.columns:
            selected_cols.append(candidate)
    frame_df = work[selected_cols].copy() if selected_cols else work
    return AnalyticsFrame(
        frame_type=_frame_type(spec, query_contract, frame_df),
        columns=list(frame_df.columns),
        rows=_records(frame_df),
        row_count=len(frame_df),
    )
