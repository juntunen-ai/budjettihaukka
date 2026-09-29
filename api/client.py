from __future__ import annotations

from typing import Any

import requests

from config import settings


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
        if not self.use_http:
            raise RuntimeError("Analyysimoottori ei kuulu tähän julkiseen lähdekoodiin.")
        payload = {
            "question": question,
            "clarifications": clarifications or {},
            "language": language,
            "ui_context": ui_context or {},
        }
        response = requests.post(f"{self.base_url}/v1/analyze", json=payload, timeout=120)
        response.raise_for_status()
        return response.json()
