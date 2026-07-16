
from __future__ import annotations

from dataclasses import dataclass

@dataclass(frozen=True)
class IdentityRepairRow:
    message_row_id: str
    account_id: str
    message_id: str
    chat_scope: str
    remote_jid: str
    participant_jid: str | None
    sender_jid: str
    previous_contact_id: str | None
    resolved_contact_id: str | None
    resolution: str
    action: str
