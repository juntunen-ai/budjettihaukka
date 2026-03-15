from __future__ import annotations

from config import settings
from domain.contracts import ParsedQuestion, clarification_from_field
from utils.analysis_spec_utils import AnalysisSpec, apply_clarifications_to_question, infer_analysis_spec


def parse_question(question: str) -> ParsedQuestion:
    spec = infer_analysis_spec(question)
    clarification_required = bool(spec.clarifications) and spec.confidence < settings.clarification_required_confidence
    missing_required_fields = [item.field for item in spec.clarifications] if clarification_required else []
    prompts = [clarification_from_field(item) for item in spec.clarifications]
    return ParsedQuestion(
        question=question,
        analysis_spec=spec,
        clarification_required=clarification_required,
        missing_required_fields=missing_required_fields,
        clarification_prompts=prompts,
    )


def apply_user_clarifications(question: str, selections: dict[str, str] | None) -> str:
    return apply_clarifications_to_question(question, selections or {})


def reparse_with_clarifications(question: str, selections: dict[str, str] | None) -> ParsedQuestion:
    execution_question = apply_user_clarifications(question, selections)
    return parse_question(execution_question)
