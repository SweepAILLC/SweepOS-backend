"""Clients API package — aggregates domain routers under /clients."""
from typing import List

from fastapi import APIRouter, status

from app.api.clients import automation, checkins, crud, insights, payments, terminal
from app.schemas.client import Client as ClientSchema

router = APIRouter(tags=["clients"])

# Static/collection routes before /{client_id} paths (see README.md)
router.include_router(terminal.router)
router.include_router(insights.router)
router.include_router(automation.router)
router.include_router(checkins.router)
router.include_router(payments.router)

# Collection list/create must live on this router (FastAPI disallows "" on included sub-routers)
router.add_api_route(
    "",
    crud.list_clients,
    methods=["GET"],
    response_model=List[ClientSchema],
    name="list_clients",
)
router.add_api_route(
    "",
    crud.create_client,
    methods=["POST"],
    response_model=ClientSchema,
    status_code=status.HTTP_201_CREATED,
    name="create_client",
)
router.include_router(crud.router)
