from __future__ import annotations

import os
import secrets
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from api.models import AnalyzeRequest, AnalyzeResponse
from api.auth import AuthenticatedUser, require_user
from config import settings

_ENGINE_DETAIL = "Analyysimoottori ei kuulu tähän julkiseen lähdekoodiin."


app = FastAPI(
    title="Budjettihaukka Analytics API",
    version="3.0.0",
    description="Public application shell. The analysis engine is not part of this source tree.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/health")
@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "budjettihaukka-api",
        "revision": os.getenv("K_REVISION", "local"),
    }


@app.post("/v1/analyze", response_model=AnalyzeResponse)
def analyze(
    request: AnalyzeRequest,
    _user: Annotated[AuthenticatedUser, Depends(require_user)],
) -> AnalyzeResponse:
    del request
    raise HTTPException(status_code=501, detail=_ENGINE_DETAIL)


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
    del limit
    raise HTTPException(status_code=501, detail=_ENGINE_DETAIL)
