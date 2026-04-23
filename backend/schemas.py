from pydantic import BaseModel, Field
from typing import Any, Optional
from datetime import datetime


class SheetCreate(BaseModel):
    name: str
    headers: list[str]
    query: str = ""


class SheetUpdate(BaseModel):
    name: Optional[str] = None
    headers: Optional[list[str]] = None
    query: Optional[str] = None


class GenerateRequest(BaseModel):
    row_limit: int = 15
    sources: Optional[list[str]] = None  # if None, LLM/router picks
    netrows_key_override: Optional[str] = None
    aiassist_key_override: Optional[str] = None
    aiassist_model: Optional[str] = None
    aiassist_provider: Optional[str] = None


class CellPatch(BaseModel):
    value: Any
    re_enrich: bool = False
    aiassist_key_override: Optional[str] = None
    aiassist_model: Optional[str] = None
    aiassist_provider: Optional[str] = None


class RowOut(BaseModel):
    id: str
    position: int
    cells: dict[str, Any]


class SheetOut(BaseModel):
    id: str
    name: str
    headers: list[str]
    query: str
    status: str
    error: str
    created_at: datetime
    updated_at: datetime
    rows: list[RowOut] = []


class SheetSummary(BaseModel):
    id: str
    name: str
    headers: list[str]
    status: str
    row_count: int
    created_at: datetime
    updated_at: datetime


class TemplateOut(BaseModel):
    id: str
    name: str
    description: str
    headers: list[str]
    suggested_query: str
    builtin: int
