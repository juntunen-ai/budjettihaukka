from __future__ import annotations

import os
import secrets
from dataclasses import asdict, is_dataclass
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from api.models import AnalyzeRequest, AnalyzeResponse
from api.auth import AuthenticatedUser, require_user
from config import settings
from domain.contracts import AnalyzeResult, serialize_analysis_spec
from services.analysis_orchestrator import analyze_question
from utils.question_library_utils import log_question_library_entry, read_question_library


app = FastAPI(
    title="Budjettihaukka Analytics API",
    version="2.2.0",
    description="AI-native analytics API for ontology-driven budget analysis.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def _serialize(value: Any) -> Any:
    if value is None:
        return None
    if is_dataclass(value):
        return asdict(value)
    return value


def _to_response(result: AnalyzeResult) -> AnalyzeResponse:
    return AnalyzeResponse(
        status=result.status,
        question=result.question,
        execution_question=result.execution_question,
        analysis_spec=serialize_analysis_spec(result.analysis_spec),
        resolved_analysis=asdict(result.resolved_analysis),
        analytics_frame=_serialize(result.analytics_frame),
        visualization_plan=_serialize(result.visualization_plan),
        result_rows=result.result_rows,
        result_columns=result.result_columns,
        used_moments=result.used_moments,
        explanation=result.explanation,
        query_id=result.query_id,
        query_source=result.query_source,
        query_contract=result.query_contract,
        sql_query=result.sql_query,
        dry_run_bytes=result.dry_run_bytes,
        retries=result.retries,
        error=result.error,
        error_class=result.error_class,
        verification_status=result.verification_status,
        warnings=result.warnings,
        metadata=result.metadata,
    )


@app.get("/health")
@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "budjettihaukka-api",
        "revision": os.getenv("K_REVISION", "local"),
    }


def _log_analyze_request(request: AnalyzeRequest, result: AnalyzeResult) -> None:
    spec = result.analysis_spec
    ui_context = request.ui_context if isinstance(request.ui_context, dict) else {}
    log_question_library_entry(
        {
            "session_id": str(ui_context.get("session_id") or ""),
            "surface": str(ui_context.get("surface") or "api"),
            "language": request.language,
            "status": result.status,
            "question": request.question,
            "clarification_required": result.status == "clarification_required",
            "clarification_choices": request.clarifications,
            "clarification_missing_fields": result.metadata.get("missing_required_fields", []),
            "intent": spec.intent,
            "metric": spec.metric,
            "fiscal_side": spec.fiscal_side,
            "entity_level": spec.entity_level,
            "growth_type": spec.growth_type,
            "time_from": spec.time_from,
            "time_to": spec.time_to,
            "requested_time_from": spec.requested_time_from,
            "requested_time_to": spec.requested_time_to,
            "confidence": spec.confidence,
            "resolved_concept_id": result.resolved_analysis.concept_id,
            "resolved_concept_label": result.resolved_analysis.concept_label,
            "query_source": result.query_source,
            "query_contract": result.query_contract,
            "query_id": result.query_id,
            "used_moment_count": len(result.used_moments),
            "result_row_count": len(result.result_rows),
            "verification_status": result.verification_status,
            "error_class": result.error_class,
            "error_message": result.error,
        }
    )


@app.post("/v1/analyze", response_model=AnalyzeResponse)
def analyze(
    request: AnalyzeRequest,
    _user: Annotated[AuthenticatedUser, Depends(require_user)],
) -> AnalyzeResponse:
    result = analyze_question(
        request.question,
        clarifications=request.clarifications,
    )
    _log_analyze_request(request, result)
    return _to_response(result)


def _require_admin_key(x_admin_key: Annotated[str | None, Header()] = None) -> None:
    expected = settings.admin_api_key
    if not expected:
        if os.getenv("K_SERVICE"):
            raise HTTPException(status_code=404, detail="Admin API is not configured")
        return
    if not x_admin_key or not secrets.compare_digest(x_admin_key, expected):
        raise HTTPException(status_code=401, detail="Invalid admin key")


@app.get("/v1/admin/question-library")
def question_library(
    _user: Annotated[AuthenticatedUser, Depends(require_user)],
    limit: Annotated[int, Query(ge=1, le=5000)] = 5000,
    x_admin_key: Annotated[str | None, Header()] = None,
) -> dict[str, list[dict[str, Any]]]:
    _require_admin_key(x_admin_key)
    return {"rows": read_question_library(limit=limit)}
