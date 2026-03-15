from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class AnalyzeRequest(BaseModel):
    question: str
    clarifications: dict[str, str] = Field(default_factory=dict)
    language: str = "fi"
    ui_context: dict[str, Any] = Field(default_factory=dict)


class AnalyzeResponse(BaseModel):
    status: str
    question: str
    execution_question: str
    analysis_spec: dict[str, Any]
    resolved_analysis: dict[str, Any]
    analytics_frame: dict[str, Any] | None = None
    visualization_plan: dict[str, Any] | None = None
    result_rows: list[dict[str, Any]] = Field(default_factory=list)
    result_columns: list[str] = Field(default_factory=list)
    used_moments: list[dict[str, Any]] = Field(default_factory=list)
    explanation: str = ""
    query_id: str | None = None
    query_source: str | None = None
    query_contract: str | None = None
    sql_query: str | None = None
    dry_run_bytes: int | None = None
    retries: int = 0
    error: str | None = None
    error_class: str | None = None
    warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
