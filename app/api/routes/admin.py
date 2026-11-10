"""GET /v1/admin/reconciliation. Operator only (blueprint §7)."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.deps import Principal, require_role
from app.services.reconciliation import run_checks

router = APIRouter(tags=["admin"])


@router.get("/admin/reconciliation")
def reconciliation(_: Principal = Depends(require_role("operator"))) -> dict:
    """Run the four invariant checks now, record them, and return them.

    The shape is §7's: {"checked_at", "checks": [{"name", "status", ...}]}.
    Every value is an integer count or a decimal string, never a float.
    """
    return run_checks(record=True).as_dict()
