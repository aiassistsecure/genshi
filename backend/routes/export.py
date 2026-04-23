from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session
from ..db import get_db
from ..models import Sheet
from ..export import to_csv, to_xlsx

router = APIRouter()


@router.get("/sheets/{sheet_id}/export")
def export_sheet(sheet_id: str, format: str = "csv", db: Session = Depends(get_db)):
    s = db.query(Sheet).get(sheet_id)
    if not s: raise HTTPException(404, "Sheet not found")
    safe_name = "".join(c for c in (s.name or "sheet") if c.isalnum() or c in "-_") or "sheet"
    if format == "xlsx":
        data = to_xlsx(s)
        return Response(
            content=data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{safe_name}.xlsx"'},
        )
    data = to_csv(s)
    return Response(
        content=data, media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}.csv"'},
    )
