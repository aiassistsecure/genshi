import os
from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health():
    return {
        "ok": True,
        "has_netrows_key": bool(os.environ.get("NETROWS_API_KEY")),
        "has_aiassist_key": bool(os.environ.get("AIASSIST_API_KEY")),
    }
