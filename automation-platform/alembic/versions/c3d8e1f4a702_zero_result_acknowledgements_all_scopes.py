"""zero-result acknowledgements for Markaz, Wing and consolidated profiles

Revision ID: c3d8e1f4a702
Revises: b2c7d9e1f304
Create Date: 2026-07-20 14:00:00
"""

from collections.abc import Sequence
import uuid

import sqlalchemy as sa
from alembic import op

revision: str = "c3d8e1f4a702"
down_revision: str | None = "b2c7d9e1f304"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NAMESPACE = uuid.UUID("9e69b636-4ca4-4b88-b49c-6f69c242b7f1")

# Existing Tehsil dormant variants from f1a2b3c4d5e6 are intentionally kept.
# These rows make the same configurable message pool available to every live
# zero-result profile. Runtime fallbacks remain as a safety net.
TARGETS = (
    ("markaz_dormant_summary", "individual", "aeo", "markaz"),
    ("wing_summary", "group", "wing", "wing"),
    ("wing_summary", "group", "district", "district"),
    ("consolidated_action_digest", "individual", "aeo", "markaz"),
    ("consolidated_action_digest", "group", "markaz", "markaz"),
    ("consolidated_action_digest", "group", "tehsil", "tehsil"),
    ("consolidated_action_digest", "group", "wing", "wing"),
    ("consolidated_action_digest", "group", "district", "district"),
)

DORMANT_BODIES = (
    "✅ *Zero dormancy achieved by {{scope_name}}.*\nToday’s Anti-Dengue review found no dormant {{wing}} schools as of {{checked_at}}. Thank you for keeping portal activities updated.",
    "🌟 *Good update for {{scope_name}}:* the latest report shows zero dormant {{wing}} schools today. Please continue timely reporting.",
    "🏆 *Today’s status for {{scope_name}}:* zero dormant {{wing}} schools were identified. Thank you for the prompt updates.",
)

DIGEST_BODIES = (
    "✅ *{{scope_name}} has achieved full Anti-Dengue compliance today.*\nZero dormant schools and no outstanding distance/timing review items were found as of {{checked_at}}. Thank you for maintaining timely and accurate portal activity.",
    "🌟 *Excellent status for {{scope_name}}.*\nToday’s consolidated review shows zero dormancy and zero pending distance or timing issues. Please continue the same standard of reporting.",
    "🏆 *{{scope_name}} is clear in today’s Anti-Dengue review.*\nZero dormant schools and zero actionable distance/timing findings were identified. Thank you for the prompt compliance.",
)


def _rows():
    for report_key, channel, scope_key, level in TARGETS:
        bodies = DIGEST_BODIES if report_key == "consolidated_action_digest" else DORMANT_BODIES
        for index, body in enumerate(bodies, start=1):
            key = f"antidengue_zero_ack_{report_key}_{channel}_{scope_key}_{index}"
            yield {
                "id": str(uuid.uuid5(_NAMESPACE, key)),
                "key": key,
                "name": f"{level.title()} zero-result acknowledgement {index}",
                "body": body,
                "report_key": report_key,
                "channel": channel,
                "scope_key": scope_key,
            }


TEMPLATE_KEYS = tuple(row["key"] for row in _rows())


def upgrade() -> None:
    connection = op.get_bind()
    statement = sa.text(
        """
        INSERT INTO whatsapp_templates
            (id, application_id, report_type_id, recipient_scope_id,
             recipient_channel, key, name, category, body, enabled,
             created_at, updated_at)
        SELECT CAST(:id AS UUID), app.id, report.id, scope.id,
               :channel, :key, :name, 'zero_result_acknowledgement',
               :body, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM whatsapp_applications app
        JOIN whatsapp_report_types report
          ON report.application_id = app.id
         AND report.key = :report_key
        JOIN whatsapp_recipient_scopes scope
          ON scope.application_id = app.id
         AND scope.channel = :channel
         AND scope.key = :scope_key
        WHERE app.key = 'antidengue'
        ON CONFLICT (key) DO NOTHING
        """
    )
    for row in _rows():
        connection.execute(statement, row)


def downgrade() -> None:
    connection = op.get_bind()
    statement = sa.text(
        "DELETE FROM whatsapp_templates WHERE key IN :keys"
    ).bindparams(sa.bindparam("keys", expanding=True))
    connection.execute(statement, {"keys": list(TEMPLATE_KEYS)})
