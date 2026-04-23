"""AiAssist LLM client — direct httpx so we can inject X-AiAssist-Provider per call."""
from __future__ import annotations
import os
import json
import httpx
from typing import Any, Optional

DEFAULT_MODEL = os.environ.get("AIASSIST_MODEL", "")
BASE_URL = os.environ.get("AIASSIST_BASE_URL", "https://api.aiassist.net")


class LLMError(Exception):
    pass


def _headers(api_key: str, provider: Optional[str]) -> dict[str, str]:
    h = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "X-API-Key": api_key,
        "User-Agent": "Genshi/0.1",
    }
    if provider:
        h["X-AiAssist-Provider"] = provider
    return h


async def _chat_raw(messages: list[dict], api_key: str, model: Optional[str], provider: Optional[str]) -> str:
    async def _post(payload: dict) -> httpx.Response:
        async with httpx.AsyncClient(timeout=60.0) as c:
            return await c.post(f"{BASE_URL}/v1/chat/completions", json=payload, headers=_headers(api_key, provider))

    payload: dict[str, Any] = {"messages": messages}
    if model:
        payload["model"] = model
    try:
        r = await _post(payload)
        # Auto-fallback: if the configured model isn't available, retry once
        # without the model override so the gateway picks its default. Common
        # when the user's Settings has a stale model slug for a provider that
        # rotated its catalog (e.g. claude-opus-4-7 → claude-opus-4).
        if r.status_code == 400 and model and (
            "does not exist" in r.text or "not found" in r.text.lower()
            or "model_not_found" in r.text or "invalid model" in r.text.lower()
        ):
            payload.pop("model", None)
            r = await _post(payload)
    except httpx.HTTPError as e:
        raise LLMError(f"AiAssist network error: {e}") from e
    if r.status_code == 401:
        raise LLMError("Invalid AiAssist API key")
    if r.status_code >= 400:
        raise LLMError(f"AiAssist {r.status_code}: {r.text[:300]}")
    try:
        data = r.json()
        return data["choices"][0]["message"]["content"] or ""
    except Exception as e:
        raise LLMError(f"Unexpected AiAssist response: {r.text[:300]}") from e


async def chat_text(messages: list[dict], api_key: Optional[str] = None, model: Optional[str] = None, provider: Optional[str] = None) -> str:
    key = (api_key or os.environ.get("AIASSIST_API_KEY", "")).strip()
    if not key:
        raise LLMError("Missing AIASSIST_API_KEY")
    return await _chat_raw(messages, key, model or DEFAULT_MODEL or None, provider)


async def chat_json(messages: list[dict], api_key: Optional[str] = None, model: Optional[str] = None, provider: Optional[str] = None) -> Any:
    raw = await chat_text(messages, api_key=api_key, model=model, provider=provider)
    return _parse_json(raw)


def _parse_json(s: str) -> Any:
    if not s:
        raise LLMError("Empty LLM response")
    txt = s.strip()
    if txt.startswith("```"):
        txt = txt.strip("`")
        if txt.lower().startswith("json"):
            txt = txt[4:]
        txt = txt.strip()
        if "```" in txt:
            txt = txt.split("```")[0].strip()
    try:
        return json.loads(txt)
    except Exception:
        for opener, closer in (("{", "}"), ("[", "]")):
            i = txt.find(opener); j = txt.rfind(closer)
            if i != -1 and j != -1 and j > i:
                try:
                    return json.loads(txt[i:j + 1])
                except Exception:
                    continue
        raise LLMError(f"Could not parse JSON from LLM: {s[:200]}")
