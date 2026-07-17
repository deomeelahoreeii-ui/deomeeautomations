from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

from automation_core.config import get_settings
from automation_core.models import Artifact, Job, JobLog, SourceFile, SourceFileRun, TaskOutbox
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintFieldObservation,
    ComplaintMatch,
    DocumentExtraction,
    PaperlessPublication,
)
from antidengue_automation.models import (
    AntiDengueSchedule,
    AntiDengueScheduleEvent,
    AntiDengueScheduleExecution,
)
from master_data.models import (
    Department,
    District,
    Markaz,
    MasterDataAudit,
    Officer,
    OfficerJurisdiction,
    School,
    SchoolHead,
    SchoolOfficerOverride,
    Tehsil,
    Wing,
)
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppDelivery,
    WhatsAppContactLink,
    WhatsAppDirectoryContact,
    WhatsAppDirectoryGroup,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchApproval,
    WhatsAppGroup,
    WhatsAppGroupMember,
    WhatsAppIdentityAlias,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
    WhatsAppRecipientScope,
    WhatsAppSettings,
    WhatsAppTemplate,
)

config = context.config
fileConfig(config.config_file_name)

settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.database_url)
target_metadata = SQLModel.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
