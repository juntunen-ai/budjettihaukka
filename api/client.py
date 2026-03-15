from __future__ import annotations

from dataclasses import asdict
from typing import Any

import requests

from config import settings
from services.analysis_orchestrator import analyze_question


class BudgetAnalyticsClient:
    def __init__(self, base_url: str | None = None, use_http: bool | None = None):
        self.base_url = (base_url or settings.analytics_api_url).rstrip("/")
        self.use_http = settings.use_backend_api if use_http is None else use_http

    def analyze(
        self,
        question: str,
        clarifications: dict[str, str] | None = None,
        language: str = "fi",
        ui_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "question": question,
            "clarifications": clarifications or {},
            "language": language,
            "ui_context": ui_context or {},
        }
        if self.use_http:
            response = requests.post(f"{self.base_url}/v1/analyze", json=payload, timeout=120)
            response.raise_for_status()
            return response.json()

        result = analyze_question(question, clarifications=clarifications or {})
        return {
            "status": result.status,
            "question": result.question,
            "execution_question": result.execution_question,
            "analysis_spec": asdict(result.analysis_spec),
            "resolved_analysis": asdict(result.resolved_analysis),
            "analytics_frame": asdict(result.analytics_frame) if result.analytics_frame else None,
            "visualization_plan": asdict(result.visualization_plan) if result.visualization_plan else None,
            "result_rows": result.result_rows,
            "result_columns": result.result_columns,
            "used_moments": result.used_moments,
            "explanation": result.explanation,
            "query_id": result.query_id,
            "query_source": result.query_source,
            "query_contract": result.query_contract,
            "sql_query": result.sql_query,
            "dry_run_bytes": result.dry_run_bytes,
            "retries": result.retries,
            "error": result.error,
            "error_class": result.error_class,
            "warnings": result.warnings,
            "metadata": result.metadata,
        }
