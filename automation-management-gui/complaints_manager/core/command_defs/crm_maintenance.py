from __future__ import annotations

from complaints_manager.core.command_model import CommandSpec


COMMANDS: tuple[CommandSpec, ...] = (
    CommandSpec(
        id="crm_pull_paperless_state",
        system_id="maintenance",
        title="Pull CRM Paperless State",
        group="CRM",
        summary="Mirror live Paperless document metadata into DuckDB.",
        script="pull_paperless_state.py",
        cwd="../crm-management-system",
    ),
    CommandSpec(
        id="crm_fix_status",
        system_id="maintenance",
        title="Fix CRM Status",
        group="CRM",
        summary="Backfill CRM statuses and extracted metadata from Paperless PDFs.",
        script="fix_crm_status.py",
        cwd="../crm-management-system",
    ),
    CommandSpec(
        id="crm_delete_old_versions",
        system_id="maintenance",
        title="Delete Old CRM Versions",
        group="CRM",
        summary="Delete older main complaint versions from Paperless.",
        script="delete_old_versions.py",
        cwd="../crm-management-system",
        confirm_label="Deletes documents from Paperless",
    ),
)
