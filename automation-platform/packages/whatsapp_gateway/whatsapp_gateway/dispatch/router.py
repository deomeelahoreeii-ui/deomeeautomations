from fastapi import APIRouter

from whatsapp_gateway.dispatch.activity_log import router as activity_log_router
from whatsapp_gateway.dispatch.deliveries import router as deliveries_router
from whatsapp_gateway.dispatch.groups import router as groups_router
from whatsapp_gateway.dispatch.recipients import router as recipients_router
from whatsapp_gateway.dispatch.settings import router as settings_router
from whatsapp_gateway.dispatch.templates import router as templates_router
from whatsapp_gateway.dispatch.test_messages import router as test_messages_router

router = APIRouter()
router.include_router(test_messages_router)
router.include_router(deliveries_router)
router.include_router(recipients_router)
router.include_router(groups_router)
router.include_router(templates_router)
router.include_router(settings_router)
router.include_router(activity_log_router)
