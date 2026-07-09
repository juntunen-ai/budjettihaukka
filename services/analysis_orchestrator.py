from __future__ import annotations

import pandas as pd

from domain.contracts import AnalyzeResult
from services.analytics_engine import collect_used_moments, run_analysis
from services.analytics_frames import build_analytics_frame
from services.answer_verifier import verify_answer
from services.explanation_service import build_explanation
from services.ontology_resolver import resolve_analysis
from services.semantic_parser import apply_user_clarifications, parse_question, reparse_with_clarifications
from services.visualization_planner import build_visualization_plan


def _raw_rows(results_df: pd.DataFrame) -> tuple[list[dict], list[str]]:
    if results_df is None or results_df.empty:
        return [], []
    clean = results_df.copy().where(pd.notna(results_df), None)
    return clean.to_dict(orient="records"), list(clean.columns)


def analyze_question(question: str, clarifications: dict[str, str] | None = None) -> AnalyzeResult:
    parsed = parse_question(question)
    if parsed.clarification_required and not clarifications:
        resolved = resolve_analysis(question, parsed.analysis_spec)
        return AnalyzeResult(
            status="clarification_required",
            question=question,
            execution_question=question,
            analysis_spec=parsed.analysis_spec,
            resolved_analysis=resolved,
            analytics_frame=None,
            visualization_plan=None,
            explanation="Tarvitaan tarkennus ennen analyysin ajoa.",
            warnings=[prompt.question for prompt in parsed.clarification_prompts],
            metadata={
                "clarification_prompts": [prompt.__dict__ for prompt in parsed.clarification_prompts],
                "missing_required_fields": parsed.missing_required_fields,
            },
        )

    execution_question = apply_user_clarifications(question, clarifications)
    final_parsed = reparse_with_clarifications(question, clarifications) if clarifications else parsed
    resolved = resolve_analysis(execution_question, final_parsed.analysis_spec)
    execution = run_analysis(execution_question, final_parsed.analysis_spec)
    results_df = execution.get("results_df") if isinstance(execution.get("results_df"), pd.DataFrame) else pd.DataFrame()
    result_rows, result_columns = _raw_rows(results_df)
    used_moments, evidence_error = collect_used_moments(
        execution_question,
        results_df,
        final_parsed.analysis_spec,
    )
    analytics_frame = build_analytics_frame(results_df, final_parsed.analysis_spec, execution.get("query_contract")) if not results_df.empty else None
    visualization_plan = build_visualization_plan(
        execution_question,
        final_parsed.analysis_spec,
        execution.get("query_contract"),
        analytics_frame,
    )
    explanation = build_explanation(
        execution.get("explanation", ""),
        resolved,
        analytics_frame,
        len(used_moments),
        visualization_plan,
    )
    warnings: list[str] = []
    if evidence_error:
        warnings.append(evidence_error)
    verification = verify_answer(
        question=execution_question,
        analysis_spec=final_parsed.analysis_spec,
        resolved=resolved,
        result_rows=result_rows,
        used_moments=used_moments,
        execution_error=execution.get("error"),
    )
    warnings.extend(verification.warnings)

    if execution.get("error"):
        status = "error"
    elif verification.verification_status == "needs_clarification":
        status = "clarification_required"
    elif verification.verification_status == "unsupported":
        status = "unsupported"
    else:
        status = "success"
    return AnalyzeResult(
        status=status,
        question=question,
        execution_question=execution_question,
        analysis_spec=final_parsed.analysis_spec,
        resolved_analysis=resolved,
        analytics_frame=analytics_frame,
        visualization_plan=visualization_plan,
        result_rows=result_rows,
        result_columns=result_columns,
        used_moments=used_moments,
        explanation=explanation,
        query_id=execution.get("query_id"),
        query_source=execution.get("query_source"),
        query_contract=execution.get("query_contract"),
        sql_query=execution.get("sql_query"),
        dry_run_bytes=execution.get("dry_run_bytes"),
        retries=int(execution.get("query_retries") or 0),
        error=execution.get("error"),
        error_class=execution.get("error_class"),
        verification_status=verification.verification_status,
        warnings=warnings,
        metadata={
            "query_plan": execution.get("query_plan"),
            "verification": verification.metadata,
            "definition_meta": execution.get("definition_meta"),
        },
    )
