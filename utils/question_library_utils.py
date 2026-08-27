from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import settings

logger = logging.getLogger(__name__)


def _use_firestore() -> bool:
    backend = settings.question_library_backend
    return backend == "firestore" or (backend == "auto" and bool(os.getenv("K_SERVICE")))


def _firestore_client():
    from google.cloud import firestore

    project = settings.runtime_project_id or settings.project_id
    return firestore.Client(project=project, database=settings.firestore_database)


def _resolve_question_library_path() -> Path:
    path = Path(settings.question_library_log_path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def log_question_library_entry(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return

    question = str(payload.get("question") or "").strip()
    if not question:
        return

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        **payload,
        "question": question,
    }
    if _use_firestore():
        try:
            _firestore_client().collection(settings.firestore_question_collection).add(row)
            return
        except Exception as exc:
            logger.warning("Firestore question library logging failed: %s", exc)
            if settings.question_library_backend == "firestore":
                return

    path = _resolve_question_library_path()
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as exc:
        logger.warning("Question library logging failed: %s", exc)


def read_question_library(limit: int | None = None) -> list[dict[str, Any]]:
    if _use_firestore():
        try:
            from google.cloud import firestore

            query = _firestore_client().collection(settings.firestore_question_collection).order_by(
                "ts",
                direction=firestore.Query.DESCENDING,
            )
            if limit and limit > 0:
                query = query.limit(limit)
            rows = []
            for snapshot in query.stream():
                row = snapshot.to_dict()
                if isinstance(row, dict):
                    rows.append(row)
            return rows
        except Exception as exc:
            logger.warning("Firestore question library read failed: %s", exc)
            if settings.question_library_backend == "firestore":
                return []

    path = _resolve_question_library_path()
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(parsed)
        except Exception:
            continue

    if limit and limit > 0:
        return rows[-limit:]
    return rows
