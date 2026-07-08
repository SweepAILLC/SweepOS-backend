"""Clients API — CSV / bulk import route."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.api.clients.helpers import effective_org_id
from app.db.session import get_db
from app.models.user import User
from app.schemas.client import ClientImportRequest, ClientImportResponse

router = APIRouter()


@router.post("/import", response_model=ClientImportResponse, status_code=status.HTTP_200_OK)
def import_clients_endpoint(
    payload: ClientImportRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Bulk import clients from CSV rows (JSON payload parsed client-side)."""
    if not payload.rows:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No rows provided.",
        )
    if len(payload.rows) > 500:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maximum 500 rows per import.",
        )

    org_id = effective_org_id(current_user)

    from app.services.client_csv_import import import_clients

    result = import_clients(
        db,
        org_id,
        payload.rows,
        default_pipeline_column=payload.default_pipeline_column,
        run_lifecycle_reconcile=payload.run_lifecycle_reconcile,
        source_filename=payload.source_filename,
    )

    if not result.get("success", True):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=result.get("failed_rows", [{}])[0].get("error", "Import failed"),
        )

    return result
