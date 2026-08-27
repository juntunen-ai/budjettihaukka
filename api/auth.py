from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated, Any

import firebase_admin
from fastapi import Header, HTTPException
from firebase_admin import auth as firebase_auth

from config import settings


@dataclass(frozen=True)
class AuthenticatedUser:
    uid: str
    email: str | None = None
    name: str | None = None


@lru_cache(maxsize=1)
def _firebase_app() -> firebase_admin.App:
    project_id = settings.firebase_auth_project_id or settings.runtime_project_id
    options = {"projectId": project_id} if project_id else None
    try:
        return firebase_admin.get_app()
    except ValueError:
        return firebase_admin.initialize_app(options=options)


def _decode_bearer_token(authorization: str | None) -> dict[str, Any]:
    if not authorization:
        raise HTTPException(status_code=401, detail="Google-kirjautuminen vaaditaan")

    scheme, separator, token = authorization.partition(" ")
    if separator != " " or scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(status_code=401, detail="Virheellinen Authorization-otsake")

    try:
        return firebase_auth.verify_id_token(token.strip(), app=_firebase_app())
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Kirjautumistunnus ei ole voimassa") from exc


def require_user(
    authorization: Annotated[str | None, Header()] = None,
) -> AuthenticatedUser:
    if not settings.require_auth:
        return AuthenticatedUser(uid="local-development")

    decoded = _decode_bearer_token(authorization)
    uid = str(decoded.get("uid") or decoded.get("sub") or "").strip()
    if not uid:
        raise HTTPException(status_code=401, detail="Kirjautumistunnuksesta puuttuu käyttäjätunnus")

    return AuthenticatedUser(
        uid=uid,
        email=str(decoded.get("email") or "").strip() or None,
        name=str(decoded.get("name") or "").strip() or None,
    )
