from __future__ import annotations

import secrets
from typing import Optional

from fastapi import Header, HTTPException, status


def _configured_admin_token() -> str:
    import os

    return str(os.getenv("APP_ADMIN_TOKEN", "")).strip()


def _authorization_bearer_token(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    scheme, _, token = text.partition(" ")
    if scheme.lower() != "bearer":
        return ""
    return token.strip()


def require_admin_access(
    x_admin_token: Optional[str] = Header(default=None),
    authorization: Optional[str] = Header(default=None),
) -> None:
    expected = _configured_admin_token()
    if not expected:
        return

    provided = str(x_admin_token or "").strip() or _authorization_bearer_token(authorization)
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="admin auth required",
        )
