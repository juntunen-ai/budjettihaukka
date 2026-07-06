from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.models import AnalyzeRequest, AnalyzeResponse
from config import settings
from domain.contracts import AnalyzeResult, serialize_analysis_spec
from services.analysis_orchestrator import analyze_question


app = FastAPI(
    title="Budjettihaukka Analytics API",
    version="0.0.2",
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
        warnings=result.warnings,
        metadata=result.metadata,
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/analyze", response_model=AnalyzeResponse)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    result = analyze_question(
        request.question,
        clarifications=request.clarifications,
    )
    return _to_response(result)
