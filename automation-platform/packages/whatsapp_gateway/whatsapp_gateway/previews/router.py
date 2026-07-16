"""Canonical router composition for WhatsApp dispatch previews.

The direct route-list fallback preserves compatibility with FastAPI/Starlette
versions or import orders where ``include_router`` does not populate the
facade router as expected during module initialization.
"""

from fastapi import APIRouter

from whatsapp_gateway.previews.approval import router as approval_router
from whatsapp_gateway.previews.artifacts import router as artifacts_router
from whatsapp_gateway.previews.contact_links import router as contact_links_router
from whatsapp_gateway.previews.creation import router as creation_router
from whatsapp_gateway.previews.deletion import router as deletion_router
from whatsapp_gateway.previews.deliveries import router as deliveries_router
from whatsapp_gateway.previews.options import router as options_router
from whatsapp_gateway.previews.queries import router as queries_router

CHILD_ROUTERS = (
    options_router,
    creation_router,
    queries_router,
    approval_router,
    deletion_router,
    deliveries_router,
    artifacts_router,
    contact_links_router,
)

router = APIRouter()
for child_router in CHILD_ROUTERS:
    router.include_router(child_router)

_expected_route_count = sum(len(child_router.routes) for child_router in CHILD_ROUTERS)
if _expected_route_count and len(router.routes) != _expected_route_count:
    # This is the same defensive composition strategy used by the refactored
    # main WhatsApp API facade. Preserve the exact original route objects.
    router.routes.clear()
    for child_router in CHILD_ROUTERS:
        router.routes.extend(child_router.routes)

if not router.routes:
    child_counts = [len(child_router.routes) for child_router in CHILD_ROUTERS]
    raise RuntimeError(
        "WhatsApp preview router composition produced no routes; "
        f"child route counts={child_counts}"
    )
