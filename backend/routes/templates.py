from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..db import get_db
from ..models import Template
from ..schemas import TemplateOut

router = APIRouter()


@router.get("/templates", response_model=list[TemplateOut])
def list_templates(db: Session = Depends(get_db)):
    ts = db.query(Template).order_by(Template.builtin.desc(), Template.name).all()
    return [
        TemplateOut(
            id=t.id, name=t.name, description=t.description or "",
            headers=t.headers or [], suggested_query=t.suggested_query or "",
            builtin=t.builtin or 0,
        ) for t in ts
    ]
