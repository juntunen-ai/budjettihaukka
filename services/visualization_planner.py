from __future__ import annotations

from domain.contracts import AnalyticsFrame, VisualizationPlan
from utils.analysis_spec_utils import AnalysisSpec
from utils.semantic_query_contracts import contract_template_order
from utils.visualization_plan_utils import extract_intent_signals, template_order


CHART_MAP = {
    "trend": "trend_line",
    "growth": "growth_bar",
    "top_growth": "ranking_bar",
    "top_cuts": "ranking_bar",
    "revenue_decline": "ranking_bar",
    "composition": "stacked_area",
    "top_categories": "category_bar",
    "seasonality": "seasonality_heatmap",
}


def build_visualization_plan(
    question: str,
    analysis_spec: AnalysisSpec,
    query_contract: str | None,
    analytics_frame: AnalyticsFrame | None,
) -> VisualizationPlan | None:
    if analytics_frame is None or analytics_frame.row_count == 0:
        return None
    templates = contract_template_order(query_contract) if query_contract else []
    if not templates:
        templates = template_order(analysis_spec, extract_intent_signals(question))
    primary = CHART_MAP.get(templates[0]) if templates else None
    secondary = CHART_MAP.get(templates[1]) if len(templates) > 1 else None
    return VisualizationPlan(
        templates=list(templates),
        primary_chart=primary,
        secondary_chart=secondary,
        table_view=True,
        show_used_moments=True,
    )
