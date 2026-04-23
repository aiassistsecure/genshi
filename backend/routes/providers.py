"""Proxy AiAssist /v1/providers — uses user-supplied BYOK key if provided, falls back to server key."""
from __future__ import annotations
import os
from fastapi import APIRouter, Header, HTTPException
import httpx

router = APIRouter()

AIASSIST_BASE = os.environ.get("AIASSIST_BASE_URL", "https://api.aiassist.net")


@router.get("/providers")
async def providers(
    x_aiassist_key: str | None = Header(default=None),
    x_aiassist_provider: str | None = Header(default=None),
):
    key = (x_aiassist_key or os.environ.get("AIASSIST_API_KEY", "")).strip()
    if not key:
        raise HTTPException(400, "No AiAssist API key configured")
    headers = {"Authorization": f"Bearer {key}", "X-API-Key": key}
    if x_aiassist_provider:
        headers["X-AiAssist-Provider"] = x_aiassist_provider
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{AIASSIST_BASE}/v1/providers", headers=headers)
    except httpx.HTTPError as e:
        raise HTTPException(502, f"AiAssist unreachable: {e}")
    if r.status_code == 401:
        raise HTTPException(401, "Invalid AiAssist API key")
    if r.status_code >= 400:
        raise HTTPException(r.status_code, f"AiAssist error: {r.text[:200]}")
    return r.json()
