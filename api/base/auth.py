"""API key authentication dependency for FastAPI."""

from __future__ import annotations

import os
from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
_EXPECTED_KEY = os.getenv("API_KEY", "")


async def verify_api_key(
    request: Request, api_key: str | None = Security(API_KEY_HEADER)
) -> None:
    """Verify X-API-Key header.

    If API_KEY env var is not set, authentication is skipped (dev mode).
    Health endpoint is always excluded.
    """
    if not _EXPECTED_KEY:
        return

    if request.url.path == "/health":
        return

    if api_key != _EXPECTED_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
