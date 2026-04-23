"""Generation route — kicks off pipeline, streams progress over SSE, persists rows."""
from __future__ import annotations
import asyncio
import json
import time
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from ..db import get_db, SessionLocal
from ..models import Sheet, Row
from ..schemas import GenerateRequest
from ..enrichment.orchestrator import generate_rows

router = APIRouter()

# In-memory job registry. Each entry:
#   {"queue": asyncio.Queue, "task": asyncio.Task, "done": bool, "rows": [],
#    "error": str, "started_at": float, "stage": str, "calls": int}
# `stage` and `calls` are sniffed from events as they pass through so the
# heartbeat tick can report what's actually in flight.
_jobs: dict[str, dict] = {}


def _is_live(job: dict) -> bool:
    return bool(job) and not job.get("done")


@router.post("/sheets/{sheet_id}/generate")
async def start_generation(sheet_id: str, payload: GenerateRequest, db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s:
        raise HTTPException(404, "Sheet not found")
    # Stuck-state recovery: if the sheet says "generating" but no live job
    # exists in this process (likely killed by a server restart), allow the
    # caller to start a fresh generation instead of refusing forever.
    existing = _jobs.get(sheet_id)
    if _is_live(existing):
        raise HTTPException(409, "Generation already in progress")

    s.status = "generating"; s.error = ""
    db.commit()

    queue: asyncio.Queue = asyncio.Queue()
    job = {"queue": queue, "done": False, "rows": [], "error": "",
           "started_at": time.monotonic(), "stage": "starting", "calls": 0}
    _jobs[sheet_id] = job

    # ----- Heartbeat: emit a `tick` every 3s with elapsed + last stage + call
    # count so the UI shows continuous activity even when the orchestrator is
    # in a long parallel-gather (no other events fire for 5-30s otherwise).
    async def heartbeat():
        try:
            while not job["done"]:
                await asyncio.sleep(3.0)
                if job["done"]:
                    break
                await queue.put({
                    "type": "tick",
                    "stage": job.get("stage") or "",
                    "elapsed_ms": int((time.monotonic() - job["started_at"]) * 1000),
                    "calls": job.get("calls", 0),
                })
        except asyncio.CancelledError:
            pass

    # ----- Sniffer: wraps the orchestrator's queue so we can update job state
    # in-flight without changing the orchestrator API.
    orch_queue: asyncio.Queue = asyncio.Queue()

    async def relay():
        while True:
            ev = await orch_queue.get()
            t = ev.get("type")
            if t == "stage":
                job["stage"] = ev.get("stage") or job["stage"]
            elif t == "source_call":
                job["calls"] = job.get("calls", 0) + 1
            await queue.put(ev)
            if t in ("__end__", "done"):
                break

    relay_task = asyncio.create_task(relay())
    hb_task = asyncio.create_task(heartbeat())

    async def run():
        try:
            rows = await generate_rows(
                headers=s.headers, query=s.query or "",
                row_limit=payload.row_limit,
                sources_override=payload.sources,
                netrows_key=payload.netrows_key_override,
                aiassist_key=payload.aiassist_key_override,
                aiassist_model=payload.aiassist_model,
                aiassist_provider=payload.aiassist_provider,
                progress=orch_queue,
            )
            job["rows"] = rows
            with SessionLocal() as db2:
                sheet2 = db2.query(Sheet).get(sheet_id)
                if sheet2:
                    for old in list(sheet2.rows):
                        db2.delete(old)
                    for i, cells in enumerate(rows):
                        db2.add(Row(sheet_id=sheet_id, position=i, cells=cells))
                    sheet2.status = "ready"
                    db2.commit()
            await queue.put({"type": "persisted", "rows": len(rows),
                             "elapsed_ms": int((time.monotonic() - job["started_at"]) * 1000)})
        except Exception as e:
            job["error"] = str(e)
            with SessionLocal() as db2:
                sheet2 = db2.query(Sheet).get(sheet_id)
                if sheet2:
                    sheet2.status = "error"
                    sheet2.error = str(e)[:500]
                    db2.commit()
            await queue.put({"type": "error", "error": str(e)})
        finally:
            job["done"] = True
            await orch_queue.put({"type": "__end__"})  # let relay drain
            hb_task.cancel()
            await queue.put({"type": "__end__"})

    job["task"] = asyncio.create_task(run())
    return {"sheet_id": sheet_id, "status": "started"}


@router.get("/sheets/{sheet_id}/stream")
async def stream(sheet_id: str):
    job = _jobs.get(sheet_id)
    if not job:
        # Stale-job recovery: the sheet may still say "generating" but the
        # in-memory job is gone (server restart). Emit a one-shot `stale`
        # event so the frontend can show a recovery banner instead of
        # silently retrying forever.
        async def stale_gen():
            yield {"event": "stale",
                   "data": json.dumps({"type": "stale",
                                       "hint": "Generation was interrupted (server restart). Click Regenerate to resume."})}
            yield {"event": "end", "data": "{}"}
        return EventSourceResponse(stale_gen())
    queue: asyncio.Queue = job["queue"]

    async def gen():
        while True:
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=120.0)
            except asyncio.TimeoutError:
                yield {"event": "ping", "data": "{}"}
                continue
            if ev.get("type") == "__end__":
                yield {"event": "end", "data": "{}"}
                break
            yield {"event": ev.get("type", "message"), "data": json.dumps(ev)}

    return EventSourceResponse(gen())


@router.get("/sheets/{sheet_id}/job")
def job_status(sheet_id: str):
    job = _jobs.get(sheet_id)
    if not job:
        return {"exists": False}
    return {"exists": True, "done": job["done"], "error": job["error"],
            "row_count": len(job["rows"]), "stage": job.get("stage", ""),
            "calls": job.get("calls", 0)}


@router.post("/sheets/{sheet_id}/reset")
def reset_stuck(sheet_id: str, db: Session = Depends(get_db)):
    """Clear a stuck `generating` status when no live job exists. Safe to call
    any time; refuses if a live job is actually running."""
    job = _jobs.get(sheet_id)
    if _is_live(job):
        raise HTTPException(409, "Generation is actually running; refusing to reset")
    s = db.query(Sheet).get(sheet_id)
    if not s:
        raise HTTPException(404, "Sheet not found")
    s.status = "draft" if not s.rows else "ready"
    s.error = ""
    db.commit()
    _jobs.pop(sheet_id, None)
    return {"ok": True, "status": s.status}
