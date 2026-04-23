"""Generation route — kicks off pipeline, streams progress over SSE, persists rows."""
from __future__ import annotations
import asyncio
import json
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from ..db import get_db, SessionLocal
from ..models import Sheet, Row
from ..schemas import GenerateRequest
from ..enrichment.orchestrator import generate_rows

router = APIRouter()

# In-memory job registry. Each entry: {"queue": asyncio.Queue, "task": asyncio.Task, "done": bool, "rows": [], "error": str}
_jobs: dict[str, dict] = {}


@router.post("/sheets/{sheet_id}/generate")
async def start_generation(sheet_id: str, payload: GenerateRequest, db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s: raise HTTPException(404, "Sheet not found")
    if sheet_id in _jobs and not _jobs[sheet_id].get("done"):
        raise HTTPException(409, "Generation already in progress")
    s.status = "generating"; s.error = ""
    db.commit()

    queue: asyncio.Queue = asyncio.Queue()
    job = {"queue": queue, "done": False, "rows": [], "error": ""}
    _jobs[sheet_id] = job

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
                progress=queue,
            )
            job["rows"] = rows
            # Persist
            with SessionLocal() as db2:
                sheet2 = db2.query(Sheet).get(sheet_id)
                if sheet2:
                    # Replace existing rows
                    for old in list(sheet2.rows):
                        db2.delete(old)
                    for i, cells in enumerate(rows):
                        db2.add(Row(sheet_id=sheet_id, position=i, cells=cells))
                    sheet2.status = "ready"
                    db2.commit()
            await queue.put({"type": "persisted", "rows": len(rows)})
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
            await queue.put({"type": "__end__"})

    job["task"] = asyncio.create_task(run())
    return {"sheet_id": sheet_id, "status": "started"}


@router.get("/sheets/{sheet_id}/stream")
async def stream(sheet_id: str):
    job = _jobs.get(sheet_id)
    if not job:
        raise HTTPException(404, "No generation in progress")
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
    return {"exists": True, "done": job["done"], "error": job["error"], "row_count": len(job["rows"])}
