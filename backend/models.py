from sqlalchemy import Column, String, Integer, JSON, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid
from .db import Base


def _id():
    return uuid.uuid4().hex[:12]


class Sheet(Base):
    __tablename__ = "sheets"
    id = Column(String, primary_key=True, default=_id)
    name = Column(String, nullable=False)
    headers = Column(JSON, nullable=False)  # list[str]
    query = Column(Text, default="")
    status = Column(String, default="draft")  # draft | generating | ready | error
    error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    rows = relationship("Row", back_populates="sheet", cascade="all, delete-orphan", order_by="Row.position")


class Row(Base):
    __tablename__ = "rows"
    id = Column(String, primary_key=True, default=_id)
    sheet_id = Column(String, ForeignKey("sheets.id"), nullable=False)
    position = Column(Integer, default=0)
    # cells: dict[header_key, {value, source, confidence, fetched_at, alternatives}]
    cells = Column(JSON, default=dict)
    sheet = relationship("Sheet", back_populates="rows")


class Template(Base):
    __tablename__ = "templates"
    id = Column(String, primary_key=True, default=_id)
    name = Column(String, nullable=False)
    description = Column(Text, default="")
    headers = Column(JSON, nullable=False)
    suggested_query = Column(Text, default="")
    builtin = Column(Integer, default=0)
