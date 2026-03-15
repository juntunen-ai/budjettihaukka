from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from utils.analysis_spec_utils import AnalysisSpec, ClarificationField


@dataclass(frozen=True)
class ClarificationPrompt:
    field: str
    question: str
    options: list[str]
    recommended: str


@dataclass
class ParsedQuestion:
    question: str
    analysis_spec: AnalysisSpec
    clarification_required: bool
    missing_required_fields: list[str] = field(default_factory=list)
    clarification_prompts: list[ClarificationPrompt] = field(default_factory=list)


@dataclass
class ResolvedAnalysis:
    question: str
    analysis_spec: AnalysisSpec
    concept_id: str | None
    concept_label: str | None
    include_rules: list[dict[str, Any]] = field(default_factory=list)
    exclude_rules: list[dict[str, Any]] = field(default_factory=list)
    ambiguity_notes: list[str] = field(default_factory=list)


@dataclass
class AnalyticsFrame:
    frame_type: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int


@dataclass
class VisualizationPlan:
    templates: list[str]
    primary_chart: str | None
    secondary_chart: str | None
    table_view: bool = True
    show_used_moments: bool = True


@dataclass
class AnalyzeResult:
    status: str
    question: str
    execution_question: str
    analysis_spec: AnalysisSpec
    resolved_analysis: ResolvedAnalysis
    analytics_frame: AnalyticsFrame | None
    visualization_plan: VisualizationPlan | None
    result_rows: list[dict[str, Any]] = field(default_factory=list)
    result_columns: list[str] = field(default_factory=list)
    used_moments: list[dict[str, Any]] = field(default_factory=list)
    explanation: str = ""
    query_id: str | None = None
    query_source: str | None = None
    query_contract: str | None = None
    sql_query: str | None = None
    dry_run_bytes: int | None = None
    retries: int = 0
    error: str | None = None
    error_class: str | None = None
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


def clarification_from_field(field: ClarificationField) -> ClarificationPrompt:
    return ClarificationPrompt(
        field=field.field,
        question=field.question,
        options=list(field.options),
        recommended=field.recommended,
    )


def serialize_analysis_spec(spec: AnalysisSpec) -> dict[str, Any]:
    payload = asdict(spec)
    payload["clarifications"] = [asdict(clarification_from_field(item)) for item in spec.clarifications]
    return payload


def deserialize_analysis_spec(payload: dict[str, Any]) -> AnalysisSpec:
    clarifications = [
        ClarificationField(
            field=item["field"],
            question=item["question"],
            options=tuple(item.get("options", [])),
            recommended=item.get("recommended", ""),
        )
        for item in payload.get("clarifications", [])
    ]
    normalized = dict(payload)
    normalized["clarifications"] = clarifications
    return AnalysisSpec(**normalized)
