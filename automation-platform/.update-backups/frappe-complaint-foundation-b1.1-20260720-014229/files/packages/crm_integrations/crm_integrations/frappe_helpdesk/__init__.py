"""Frappe Helpdesk integration for native CRM complaint cases."""

from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskClient
from crm_integrations.frappe_helpdesk.service import ComplaintHelpdeskSyncService

__all__ = ["FrappeHelpdeskClient", "ComplaintHelpdeskSyncService"]
