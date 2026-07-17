"""Native CRM complaint domain.

The package owns durable complaint identity and evidence records. Capture
providers such as WhatsApp may contribute documents, but do not own the case.
"""

from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintFieldObservation,
    ComplaintMatch,
    ComplaintReply,
    DocumentExtraction,
    PaperlessPublication,
)

__all__ = [
    "ComplaintCase",
    "ComplaintDocument",
    "ComplaintDocumentCaseLink",
    "ComplaintFieldObservation",
    "ComplaintMatch",
    "ComplaintReply",
    "DocumentExtraction",
    "PaperlessPublication",
]
