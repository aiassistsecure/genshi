"""Fill in the blanks: for each row, run ONE Netrows google_search using the
row's identity, then ask the LLM to extract values for every blank cell from
the SERP snippets."""
from __future__ import annotations
import asyncio
import json
from typing import Any, Optional

from ..sources.netrows import NetrowsClient
from .llm import chat_json, LLMError


def _val(c: Any) -> Any:
    return c.get("value") if isinstance(c, dict) else c


def _row_identity(cells: dict, headers: list[str]) -> str:
    """Pick the strongest identifying values to use as the SERP query seed."""
    bits: list[str] = []
    # Priority order — most discriminating first
    priority = [
        "company name", "company", "name", "full name", "contact name",
        "website", "domain", "linkedin url", "github url",
        "email", "address", "phone",
    ]
    seen_lc: set[str] = set()
    for p in priority:
        for h in headers:
            if h.lower().replace("_", " ").strip() == p:
                v = _val(cells.get(h))
                if isinstance(v, (str, int, float)) and str(v).strip():
                    s = str(v).strip()
                    if s.lower() not in seen_lc:
                        bits.append(s); seen_lc.add(s.lower())
                break
        if len(bits) >= 3: break
    # Fallback: first 2 non-empty cells
    if not bits:
        for h in headers:
            v = _val(cells.get(h))
            if isinstance(v, (str, int, float)) and str(v).strip():
                bits.append(str(v).strip())
                if len(bits) >= 2: break
    return " ".join(bits)


def _blank_headers(cells: dict, headers: list[str]) -> list[str]:
    out = []
    for h in headers:
        v = _val(cells.get(h))
        if v in (None, "", [], {}): out.append(h)
    return out


async def _fill_one_row(
    nc: NetrowsClient,
    row_id: str,
    cells: dict,
    headers: list[str],
    sheet_query: str,
    aiassist_key: Optional[str],
    model: Optional[str],
    provider: Optional[str],
) -> tuple[str, dict, int, str]:
    """Returns (row_id, updated_cells, filled_count, error). Mutates a copy of cells."""
    blanks = _blank_headers(cells, headers)
    if not blanks:
        return row_id, cells, 0, ""

    identity = _row_identity(cells, headers)
    if not identity:
        return row_id, cells, 0, "no identity"

    # Build a SERP query that biases toward the missing fields
    blank_terms = " ".join(blanks[:6])
    serp_query = f"{identity} {blank_terms}".strip()

    try:
        payload = await nc.google_search(serp_query, limit=10)
    except Exception as e:
        return row_id, cells, 0, f"serp: {str(e)[:120]}"

    # Extract snippet-shaped entries
    items = []
    if isinstance(payload, dict):
        for k in ("items", "results", "data", "organic_results", "organic"):
            v = payload.get(k)
            if isinstance(v, list): items = v; break
    snippets = []
    for it in (items or [])[:10]:
        if not isinstance(it, dict): continue
        snippets.append({
            "title": it.get("title") or it.get("name"),
            "url": it.get("url") or it.get("link"),
            "snippet": it.get("snippet") or it.get("description") or it.get("body"),
        })

    if not snippets:
        return row_id, cells, 0, "no SERP results"

    # Provide the row context the LLM can use to disambiguate
    context = {h: _val(cells.get(h)) for h in headers if _val(cells.get(h)) not in (None, "")}

    sys = (
        "You are filling missing cells in a spreadsheet row from web search snippets. "
        "Use ONLY information clearly supported by the snippets — do NOT guess. "
        "If a snippet is ambiguous or doesn't mention a field, set that field to null. "
        "Output strict JSON: {\"values\": {\"<header>\": <value-or-null>, ...}}. "
        "The keys MUST be EXACTLY these headers (case-sensitive)."
    )
    user = json.dumps({
        "headers_to_fill": blanks,
        "all_headers": headers,
        "existing_row_context": context,
        "sheet_query": sheet_query,
        "search_results": snippets,
    })

    try:
        out = await chat_json(
            [{"role": "system", "content": sys}, {"role": "user", "content": user}],
            api_key=aiassist_key, model=model, provider=provider,
        )
    except LLMError as e:
        return row_id, cells, 0, f"llm: {str(e)[:120]}"

    vals = (out.get("values") if isinstance(out, dict) else {}) or {}
    filled = 0
    new_cells = dict(cells)
    for h in blanks:
        v = vals.get(h)
        if v is None:
            v = vals.get(h.lower()) or vals.get(h.lower().replace(" ", "_"))
        if v in (None, "", [], {}): continue
        existing = cells.get(h) if isinstance(cells.get(h), dict) else {}
        new_cells[h] = {
            **existing,
            "value": v,
            "source": "serp",
            "confidence": "medium",
        }
        filled += 1
    return row_id, new_cells, filled, ""


async def fill_blanks_for_sheet(
    rows: list[dict],
    headers: list[str],
    sheet_query: str,
    netrows_key: Optional[str] = None,
    aiassist_key: Optional[str] = None,
    aiassist_model: Optional[str] = None,
    aiassist_provider: Optional[str] = None,
    concurrency: int = 5,
) -> dict[str, dict]:
    """rows = [{id, cells}, ...]. Returns {row_id: updated_cells} for rows that changed."""
    sem = asyncio.Semaphore(concurrency)
    results: dict[str, dict] = {}
    errors: list[str] = []

    async with NetrowsClient(api_key=netrows_key) as nc:
        async def go(r):
            async with sem:
                return await _fill_one_row(
                    nc, r["id"], r["cells"], headers, sheet_query,
                    aiassist_key, aiassist_model, aiassist_provider,
                )
        out = await asyncio.gather(*[go(r) for r in rows], return_exceptions=True)

    total_filled = 0
    for o in out:
        if isinstance(o, Exception):
            errors.append(str(o)[:120]); continue
        rid, cells, filled, err = o
        if err: errors.append(f"{rid[:6]}: {err}")
        if filled > 0:
            results[rid] = cells
            total_filled += filled

    return {"updated": results, "filled_cells": total_filled,
            "rows_touched": len(results), "errors": errors[:10]}
