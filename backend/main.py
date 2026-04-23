from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .db import Base, engine
from . import models  # noqa: F401  -- ensures tables register
from .templates_seed import seed_templates
from .routes import sheets, generate, templates, export, health, providers

app = FastAPI(title="Genshi API", version="0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)
seed_templates()

app.include_router(health.router, prefix="/api")
app.include_router(sheets.router, prefix="/api")
app.include_router(generate.router, prefix="/api")
app.include_router(templates.router, prefix="/api")
app.include_router(export.router, prefix="/api")
app.include_router(providers.router, prefix="/api")
