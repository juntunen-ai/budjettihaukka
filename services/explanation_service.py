from __future__ import annotations

from domain.contracts import AnalyticsFrame, ResolvedAnalysis, VisualizationPlan


def build_explanation(
    base_explanation: str,
    resolved: ResolvedAnalysis,
    analytics_frame: AnalyticsFrame | None,
    used_moment_count: int,
    visualization_plan: VisualizationPlan | None,
) -> str:
    parts: list[str] = []
    if base_explanation:
        parts.append(base_explanation)
    if resolved.concept_label:
        parts.append(f"Tulkittu käsite: {resolved.concept_label}.")
    if resolved.fiscal_side and resolved.fiscal_side not in {"unknown", "mixed"}:
        parts.append(f"Semanttinen budjettipuoli: {resolved.fiscal_side}.")
    if resolved.observability_class:
        parts.append(f"Vastauksen havaittavuusluokka: {resolved.observability_class}.")
    if resolved.observability_reason and resolved.observability_class in {"proxy", "unsupported", "composite"}:
        parts.append(resolved.observability_reason)
    if analytics_frame is not None and analytics_frame.row_count:
        parts.append(f"Vakioskeemaan normalisoitu analytiikkakehys: {analytics_frame.frame_type}, rivejä {analytics_frame.row_count}.")
    if visualization_plan and visualization_plan.primary_chart:
        parts.append(f"Suositeltu ensisijainen visualisointi: {visualization_plan.primary_chart}.")
    if used_moment_count:
        parts.append(f"Mukana näytetään {used_moment_count} käytettyä budjettimomenttia.")
    return " ".join(parts).strip()
