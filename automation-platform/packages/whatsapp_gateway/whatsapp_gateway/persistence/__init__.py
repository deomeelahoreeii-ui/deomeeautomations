"""Persistence models grouped by responsibility.

Import from :mod:`whatsapp_gateway.models` for the stable public API.
"""

from whatsapp_gateway.persistence.account import WhatsAppAccount
from whatsapp_gateway.persistence.configuration import (
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppDispatchProfile,
    WhatsAppRecipientScope,
    WhatsAppReportType,
    WhatsAppSettings,
    WhatsAppTemplate,
)
from whatsapp_gateway.persistence.deliveries import WhatsAppActivity, WhatsAppDelivery
from whatsapp_gateway.persistence.directory import (
    WhatsAppContactLink,
    WhatsAppDirectoryContact,
    WhatsAppDirectoryGroup,
    WhatsAppGroup,
    WhatsAppGroupMember,
    WhatsAppIdentityAlias,
)
from whatsapp_gateway.persistence.inbound import (
    WhatsAppInboundAttachment,
    WhatsAppInboundExportItem,
    WhatsAppInboundExportRun,
    WhatsAppInboundHistoryRequest,
    WhatsAppInboundMessage,
)
from whatsapp_gateway.persistence.previews import (
    WhatsAppDispatchApproval,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
)

__all__ = [
    "WhatsAppAccount",
    "WhatsAppDirectoryGroup",
    "WhatsAppDirectoryContact",
    "WhatsAppIdentityAlias",
    "WhatsAppGroupMember",
    "WhatsAppGroup",
    "WhatsAppApplication",
    "WhatsAppReportType",
    "WhatsAppRecipientScope",
    "WhatsAppAudience",
    "WhatsAppAudienceMember",
    "WhatsAppTemplate",
    "WhatsAppDispatchProfile",
    "WhatsAppContactLink",
    "WhatsAppDispatchPreview",
    "WhatsAppDispatchPreviewArtifact",
    "WhatsAppDispatchPreviewDelivery",
    "WhatsAppDispatchApproval",
    "WhatsAppDelivery",
    "WhatsAppActivity",
    "WhatsAppSettings",
    "WhatsAppInboundMessage",
    "WhatsAppInboundAttachment",
    "WhatsAppInboundHistoryRequest",
    "WhatsAppInboundExportRun",
    "WhatsAppInboundExportItem",
]
