from __future__ import annotations

from complaints_manager.core.command_model import CommandSpec


COMMANDS: tuple[CommandSpec, ...] = (
    CommandSpec(
        id="pmdu_pull_paperless_state",
        system_id="maintenance",
        title="Pull PMDU Paperless State",
        group="PMDU",
        summary="Mirror live Paperless document metadata into DuckDB.",
        script="pull_paperless_state.py",
        cwd="../pmdu-management-system",
    ),
    CommandSpec(
        id="pmdu_restore_status",
        system_id="maintenance",
        title="Restore PMDU Status",
        group="PMDU",
        summary="Restore Paperless status fields from DuckDB status history.",
        script="restore_pmdu_status.py",
        cwd="../pmdu-management-system",
        confirm_label="Patches Paperless document statuses",
    ),
    CommandSpec(
        id="pmdu_delete_old_versions",
        system_id="maintenance",
        title="Delete Old PMDU Versions",
        group="PMDU",
        summary="Delete older main complaint versions from Paperless.",
        script="delete_old_versions.py",
        cwd="../pmdu-management-system",
        confirm_label="Deletes documents from Paperless",
    ),
)
