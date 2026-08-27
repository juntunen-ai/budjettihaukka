#!/usr/bin/env python3
from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import api.auth as auth_module


def _expect_unauthorized(authorization: str | None) -> None:
    try:
        auth_module.require_user(authorization)
    except HTTPException as exc:
        assert exc.status_code == 401, exc
    else:
        raise AssertionError("Authentication failure should return HTTP 401")


def main() -> None:
    original_settings = auth_module.settings
    original_firebase_app = auth_module._firebase_app
    original_verify = auth_module.firebase_auth.verify_id_token

    try:
        auth_module.settings = replace(original_settings, require_auth=False)
        assert auth_module.require_user(None).uid == "local-development"

        auth_module.settings = replace(
            original_settings,
            require_auth=True,
            firebase_auth_project_id="test-project",
        )
        _expect_unauthorized(None)
        _expect_unauthorized("Basic credentials")
        _expect_unauthorized("Bearer ")

        auth_module._firebase_app = lambda: object()  # type: ignore[assignment]
        auth_module.firebase_auth.verify_id_token = lambda token, app: {
            "uid": "google-user-123",
            "email": "user@example.com",
            "name": "Test User",
        }
        user = auth_module.require_user("Bearer valid-token")
        assert user.uid == "google-user-123"
        assert user.email == "user@example.com"
        assert user.name == "Test User"

        auth_module.firebase_auth.verify_id_token = lambda token, app: {}
        _expect_unauthorized("Bearer token-without-user")

        def reject_token(token: str, app: object) -> dict[str, str]:
            raise ValueError("expired")

        auth_module.firebase_auth.verify_id_token = reject_token
        _expect_unauthorized("Bearer expired-token")
    finally:
        auth_module.settings = original_settings
        auth_module._firebase_app = original_firebase_app  # type: ignore[assignment]
        auth_module.firebase_auth.verify_id_token = original_verify

    print("Firebase authentication tests passed")


if __name__ == "__main__":
    main()
