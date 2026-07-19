from __future__ import annotations

import uuid
from typing import Literal

from pydantic import BaseModel

class PreviewInput(BaseModel):
    source_job_id: uuid.UUID
    dispatch_profile_id: uuid.UUID
class BulkPreviewInput(BaseModel):
    source_job_id: uuid.UUID
    dispatch_profile_ids: list[uuid.UUID]
class PreviewIdsInput(BaseModel):
    preview_ids: list[uuid.UUID]
class BulkPreviewApprovalInput(PreviewIdsInput):
    acknowledge_warnings: bool = False
    acknowledge_exclusions: bool = False
    approved_by: str = "web-operator"
class ContactLinkInput(BaseModel):
    entity_type: Literal["officer", "school_head"]
    entity_id: uuid.UUID
class PreviewApprovalInput(BaseModel):
    acknowledge_warnings: bool = False
    acknowledge_exclusions: bool = False
    approved_by: str = "web-operator"
