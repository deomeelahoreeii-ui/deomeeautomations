from __future__ import annotations

import nats
from fastapi import APIRouter

from whatsapp_gateway.api_common import *
from whatsapp_gateway.configuration.audiences import *
from whatsapp_gateway.configuration.audiences import router as _audiences_router
from whatsapp_gateway.configuration.catalog import *
from whatsapp_gateway.configuration.catalog import router as _catalog_router
from whatsapp_gateway.configuration.profiles import *
from whatsapp_gateway.configuration.profiles import router as _profiles_router
from whatsapp_gateway.directory.router import *
from whatsapp_gateway.directory.router import router as _directory_router
from whatsapp_gateway.dispatch.delivery import *
from whatsapp_gateway.dispatch.delivery import router as _delivery_router
from whatsapp_gateway.dispatch.groups import *
from whatsapp_gateway.dispatch.groups import router as _groups_router
from whatsapp_gateway.dispatch.settings import *
from whatsapp_gateway.dispatch.settings import router as _settings_router
from whatsapp_gateway.dispatch.templates import *
from whatsapp_gateway.dispatch.templates import router as _templates_router
from whatsapp_gateway.gateway.connection import *
from whatsapp_gateway.gateway.connection import router as _connection_router

_CHILD_ROUTERS = (
    _audiences_router,
    _catalog_router,
    _profiles_router,
    _directory_router,
    _delivery_router,
    _groups_router,
    _settings_router,
    _templates_router,
    _connection_router,
)

router = APIRouter()
for _child_router in _CHILD_ROUTERS:
    router.include_router(_child_router)

_expected_route_count = sum(len(item.routes) for item in _CHILD_ROUTERS)
if _expected_route_count and len(router.routes) != _expected_route_count:
    # Preserve the exact child route objects if an installed FastAPI/Starlette
    # version does not copy them through include_router as expected.
    router.routes.clear()
    for _child_router in _CHILD_ROUTERS:
        router.routes.extend(_child_router.routes)

if not router.routes:
    raise RuntimeError("WhatsApp API router composition produced no routes")
