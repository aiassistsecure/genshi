"""AiAS Intelligence SDK wrapper — supplementary signal scanning."""
from __future__ import annotations
import os
import asyncio
from typing import Any, Optional

try:
    from aias_intelligence import AiASIntelligenceAsync  # type: ignore
    HAS_INTEL = True
except Exception:
    HAS_INTEL = False


async def scan_signals(keywords: list[str], sources: Optional[list[str]] = None, limit: int = 20, api_key: Optional[str] = None) -> list[dict]:
    """Scan free signal sources for raw mentions. Returns [] if SDK unavailable or fails."""
    if not HAS_INTEL:
        return []
    key = api_key or os.environ.get("AIASSIST_API_KEY", "")
    if not key:
        return []
    sources = sources or ["reddit", "hackernews", "devto", "producthunt", "indiehackers"]
    try:
        async with AiASIntelligenceAsync(api_key=key) as client:  # type: ignore
            res = await client.scan(sources=sources, keywords=keywords, limit=limit)
        data = (res or {}).get("data", {}) if isinstance(res, dict) else {}
        items = data.get("results") or []
        return [x for x in items if isinstance(x, dict)]
    except Exception:
        return []
