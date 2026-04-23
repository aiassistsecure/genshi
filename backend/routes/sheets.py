from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any
from ..db import get_db
from ..models import Sheet, Row
from ..schemas import SheetCreate, SheetUpdate, SheetOut, SheetSummary, RowOut, CellPatch
from ..enrichment.orchestrator import re_enrich_cell
from ..enrichment.fill_blanks import fill_blanks_for_sheet
from ..enrichment.llm import LLMError
from pydantic import BaseModel
from typing import Optional


class FillBlanksPayload(BaseModel):
    netrows_key_override: Optional[str] = None
    aiassist_key_override: Optional[str] = None
    aiassist_model: Optional[str] = None
    aiassist_provider: Optional[str] = None

router = APIRouter()


def _to_out(s: Sheet) -> SheetOut:
    return SheetOut(
        id=s.id, name=s.name, headers=s.headers or [], query=s.query or "",
        status=s.status, error=s.error or "", created_at=s.created_at, updated_at=s.updated_at,
        rows=[RowOut(id=r.id, position=r.position, cells=r.cells or {}) for r in s.rows],
    )


@router.get("/sheets", response_model=list[SheetSummary])
def list_sheets(db: Session = Depends(get_db)):
    sheets = db.query(Sheet).order_by(Sheet.updated_at.desc()).all()
    return [
        SheetSummary(
            id=s.id, name=s.name, headers=s.headers or [], status=s.status,
            row_count=len(s.rows), created_at=s.created_at, updated_at=s.updated_at,
        ) for s in sheets
    ]


@router.post("/sheets", response_model=SheetOut)
def create_sheet(payload: SheetCreate, db: Session = Depends(get_db)):
    if not payload.headers:
        raise HTTPException(400, "At least one header is required")
    s = Sheet(name=payload.name, headers=payload.headers, query=payload.query, status="draft")
    db.add(s); db.commit(); db.refresh(s)
    return _to_out(s)


@router.get("/sheets/{sheet_id}", response_model=SheetOut)
def get_sheet(sheet_id: str, db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s: raise HTTPException(404, "Sheet not found")
    return _to_out(s)


@router.patch("/sheets/{sheet_id}", response_model=SheetOut)
def update_sheet(sheet_id: str, payload: SheetUpdate, db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s: raise HTTPException(404, "Sheet not found")
    if payload.name is not None: s.name = payload.name
    if payload.headers is not None: s.headers = payload.headers
    if payload.query is not None: s.query = payload.query
    db.commit(); db.refresh(s)
    return _to_out(s)


@router.delete("/sheets/{sheet_id}")
def delete_sheet(sheet_id: str, db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s: raise HTTPException(404, "Sheet not found")
    db.delete(s); db.commit()
    return {"deleted": sheet_id}


@router.patch("/sheets/{sheet_id}/rows/{row_id}/cells/{header}")
async def update_cell(sheet_id: str, row_id: str, header: str, payload: CellPatch, db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s: raise HTTPException(404, "Sheet not found")
    if header not in (s.headers or []):
        raise HTTPException(400, "Unknown header")
    r = db.query(Row).get(row_id)
    if not r or r.sheet_id != sheet_id:
        raise HTTPException(404, "Row not found")
    cells = dict(r.cells or {})
    if payload.re_enrich:
        try:
            new_cell = await re_enrich_cell(
                cells, header, s.headers, s.query or "",
                aiassist_key=payload.aiassist_key_override,
                model=payload.aiassist_model,
                provider=payload.aiassist_provider,
            )
        except LLMError as e:
            raise HTTPException(422, f"AI re-enrich failed: {e}")
        cells[header] = new_cell
    else:
        existing = cells.get(header) or {}
        if not isinstance(existing, dict):
            existing = {}
        existing.update({
            "value": payload.value,
            "source": "user",
            "confidence": "user",
            "fetched_at": existing.get("fetched_at"),
        })
        cells[header] = existing
    r.cells = cells
    db.commit(); db.refresh(r)
    return {"row_id": r.id, "cell": cells[header]}


@router.post("/sheets/{sheet_id}/rows")
def add_row(sheet_id: str, db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s: raise HTTPException(404, "Sheet not found")
    pos = len(s.rows)
    r = Row(sheet_id=sheet_id, position=pos, cells={h: {"value": None, "source": "user", "confidence": "low"} for h in s.headers})
    db.add(r); db.commit(); db.refresh(r)
    return RowOut(id=r.id, position=r.position, cells=r.cells or {})


@router.post("/sheets/{sheet_id}/fill-blanks")
async def fill_blanks(sheet_id: str, payload: FillBlanksPayload, db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s: raise HTTPException(404, "Sheet not found")
    if not s.rows:
        return {"filled_cells": 0, "rows_touched": 0, "errors": []}
    rows = [{"id": r.id, "cells": dict(r.cells or {})} for r in s.rows]
    res = await fill_blanks_for_sheet(
        rows, s.headers or [], s.query or "",
        netrows_key=payload.netrows_key_override,
        aiassist_key=payload.aiassist_key_override,
        aiassist_model=payload.aiassist_model,
        aiassist_provider=payload.aiassist_provider,
    )
    # Persist changes
    if res["updated"]:
        by_id = {r.id: r for r in s.rows}
        for rid, cells in res["updated"].items():
            row = by_id.get(rid)
            if row is not None:
                row.cells = cells
        db.commit()
    return {k: v for k, v in res.items() if k != "updated"}


@router.delete("/sheets/{sheet_id}/rows/{row_id}")
def delete_row(sheet_id: str, row_id: str, db: Session = Depends(get_db)):
    r = db.query(Row).get(row_id)
    if not r or r.sheet_id != sheet_id: raise HTTPException(404, "Row not found")
    db.delete(r); db.commit()
    return {"deleted": row_id}
