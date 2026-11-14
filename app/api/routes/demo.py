"""GET /v1/demo/config: whether the demo page may offer simulated faults (D27)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.deps import Principal, require_role
from app.core.config import settings

router = APIRouter(tags=["demo"])


@router.get("/demo/config")
def demo_config(_: Principal = Depends(require_role("viewer"))) -> dict[str, bool]:
    """Any signed-in role. Exactly one key, from this API process's own
    setting: nothing else from configuration, the environment or other
    services, and the API never asks the consumer."""
    return {"demo_faults_enabled": bool(settings.demo_faults_enabled)}
