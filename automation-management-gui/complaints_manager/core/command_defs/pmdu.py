from __future__ import annotations

from complaints_manager.core.command_model import (
    ArgField,
    CommandSpec,
    EnvField,
    Option,
    uv_shell_command,
)


RECIPIENT_OPTIONS = (
    Option("group", "Group"),
    Option("aeo", "AEO"),
    Option("ddeo", "DDEO"),
    Option("deo", "DEO"),
    Option("officers", "Officers"),
    Option("all", "All"),
)

FILE_OPTIONS = (
    Option("none", "Text only"),
    Option("pdf", "PDF"),
    Option("pdf-and-attachments", "PDF + attachments"),
    Option("combined-pdf", "Combined PDF"),
)

SCOPE_OPTIONS = (
    Option("all", "All"),
    Option("current-pmdu-under-investigation", "Current PMDU under investigation"),
    Option("new", "New"),
    Option("new-or-updated", "New or updated"),
    Option("newly-under-investigation", "Status newly changed to under investigation"),
)

MODE_OPTIONS = (
    Option("", "Default"),
    Option("per_complaint", "Per complaint"),
    Option("per_recipient", "Per recipient"),
)

INQUIRY_SOURCE_OPTIONS = (
    Option("paperless", "Paperless"),
    Option("artifacts", "Artifacts"),
)

FOLLOWUP_TYPE_OPTIONS = (
    Option("reminder", "Reminder"),
    Option("warning", "Warning"),
    Option("final_warning", "Final warning"),
)

FOLLOWUP_RECIPIENT_OPTIONS = (
    Option("ddeo", "DDEO"),
    Option("aeo", "AEO"),
    Option("deo_mee", "DEO MEE"),
)

FOLLOWUP_FILE_OPTIONS = (
    Option("inquiry-order", "Inquiry order"),
    Option("combined-pdf", "Combined PDF"),
    Option("none", "Text only"),
)

PREVIEW_CLEANUP_OPTIONS = (
    Option("all", "Notifications + follow-ups"),
    Option("notifications", "Notifications only"),
    Option("followups", "Follow-ups only"),
)

PAPERLESS_ARTIFACT_SCOPE_OPTIONS = (
    Option("current-scrape", "Current scrape only"),
    Option("all-artifacts", "All artifacts"),
)


def notification_fields(
    to_default: str = "group",
    files_default: str = "none",
    scope_default: str = "all",
) -> tuple[ArgField, ...]:
    return (
        ArgField("--to", "Recipient", "--to", to_default, "select", RECIPIENT_OPTIONS),
        ArgField("--files", "Files", "--files", files_default, "select", FILE_OPTIONS),
        ArgField("--scope", "Scope", "--scope", scope_default, "select", SCOPE_OPTIONS),
        ArgField("--tehsil", "Tehsil", "--tehsil", "", "text"),
        ArgField("--mode", "Message mode", "--mode", "", "select", MODE_OPTIONS),
    )


def followup_fields() -> tuple[ArgField, ...]:
    return (
        ArgField("type", "Follow-up type", "--type", "reminder", "select", FOLLOWUP_TYPE_OPTIONS),
        ArgField("recipient", "Recipient", "--recipient", "ddeo", "select", FOLLOWUP_RECIPIENT_OPTIONS),
        ArgField("files", "Files", "--files", "inquiry-order", "select", FOLLOWUP_FILE_OPTIONS),
        ArgField("tehsil", "Tehsil", "--tehsil", "", "text"),
        ArgField("complaint", "Complaint number", "--complaint", "", "text"),
        ArgField("max_cases", "Max cases", "--max-cases", "", "int"),
        ArgField("allow_second_today", "Allow second today", "--allow-second-today", "false", "bool_flag"),
        ArgField("force", "Force send/preview", "--force", "false", "bool_flag"),
    )


COMMANDS: tuple[CommandSpec, ...] = (
    CommandSpec(
        id="pmdu_phase1",
        system_id="pmdu",
        title="Scrape Portal",
        group="Intake",
        summary="Login, queue complaint details, and save rendered HTML into raw_html.",
        script="main.py",
        args=("phase1",),
        aliases=("scrape",),
        env_fields=(
            EnvField("PMDU_MAX_DETAILS", "Max details", "", "int"),
            EnvField("PMDU_HEADLESS", "Headless Chromium", "false", "bool"),
        ),
    ),
    CommandSpec(
        id="pmdu_scraped_data_cleanup_preview",
        system_id="pmdu",
        title="Preview Scraped Data Cleanup",
        group="Intake",
        summary="Show raw HTML files and scrape queue rows that would be discarded.",
        script="cleanup_scraped_data.py",
        args=("--dry-run",),
        arg_fields=(
            ArgField("raw_html_dir", "Raw HTML folder", "--raw-html-dir", "raw_html", "path"),
            ArgField("artifact_dir", "Artifact folder", "--artifact-dir", "artifacts", "path"),
            ArgField("include_artifacts", "Also discard artifacts", "--include-artifacts", "false", "bool_flag"),
        ),
    ),
    CommandSpec(
        id="pmdu_scraped_data_cleanup",
        system_id="pmdu",
        title="Discard Scraped Data",
        group="Intake",
        summary="Delete PMDU raw HTML files and clear scrape queue rows. Artifacts are optional.",
        script="cleanup_scraped_data.py",
        args=("--yes",),
        arg_fields=(
            ArgField("raw_html_dir", "Raw HTML folder", "--raw-html-dir", "raw_html", "path"),
            ArgField("artifact_dir", "Artifact folder", "--artifact-dir", "artifacts", "path"),
            ArgField("include_artifacts", "Also discard artifacts", "--include-artifacts", "false", "bool_flag"),
        ),
        confirm_label="Deletes raw scraped PMDU HTML and clears the scrape queue",
    ),
    CommandSpec(
        id="pmdu_phase2",
        system_id="pmdu",
        title="Build Artifacts",
        group="Artifacts",
        summary="Build artifacts from the scraped queue by default; use all-raw-html only for maintenance rebuilds.",
        script="main.py",
        args=("phase2",),
        aliases=("artifacts", "parse"),
        env_fields=(
            EnvField("PMDU_PHASE2_MAX_FILES", "Max files", "", "int"),
            EnvField(
                "PMDU_PHASE2_INPUT_SCOPE",
                "Input scope (scraped-queue or all-raw-html)",
                "scraped-queue",
                "text",
            ),
        ),
    ),
    CommandSpec(
        id="pmdu_archive_processed_dry_run",
        system_id="pmdu",
        title="Preview Processed Archive",
        group="Archive",
        summary="Create a dry-run archive manifest for processed PMDU raw HTML/artifacts without moving files.",
        script="archive_pmdu_history.py",
        args=("--selection", "processed", "--dry-run"),
        arg_fields=(
            ArgField("archive_root", "Archive root", "--archive-root", "archives/pmdu", "path"),
            ArgField("label", "Batch label", "--label", "history", "text"),
        ),
    ),
    CommandSpec(
        id="pmdu_archive_processed",
        system_id="pmdu",
        title="Archive Processed History",
        group="Archive",
        summary="Move processed PMDU raw HTML/artifacts into a timestamped archive batch, leaving unprocessed raw HTML in place.",
        script="archive_pmdu_history.py",
        args=("--selection", "processed", "--yes"),
        arg_fields=(
            ArgField("archive_root", "Archive root", "--archive-root", "archives/pmdu", "path"),
            ArgField("label", "Batch label", "--label", "history", "text"),
        ),
        confirm_label="Moves processed PMDU raw HTML and artifact folders into an audited archive batch",
    ),
    CommandSpec(
        id="pmdu_diagnostics_preview",
        system_id="pmdu",
        title="Preview Diagnostic Cleanup",
        group="Diagnostics",
        summary="Preview moving browser login/list captures out of artifacts into date-wise diagnostics.",
        script="organize_pmdu_diagnostics.py",
        args=("--dry-run",),
        arg_fields=(
            ArgField("diagnostics_root", "Diagnostics root", "--diagnostics-root", "diagnostics/pmdu", "path"),
        ),
    ),
    CommandSpec(
        id="pmdu_diagnostics_organize",
        system_id="pmdu",
        title="Organize Browser Diagnostics",
        group="Diagnostics",
        summary="Move PMDU browser login/list captures into diagnostics/pmdu/YYYY/MM/DD/<run-id>.",
        script="organize_pmdu_diagnostics.py",
        args=("--yes",),
        arg_fields=(
            ArgField("diagnostics_root", "Diagnostics root", "--diagnostics-root", "diagnostics/pmdu", "path"),
        ),
        confirm_label="Moves root-level PMDU browser diagnostics out of artifacts",
    ),
    CommandSpec(
        id="paperless_check",
        system_id="pmdu",
        title="Check Paperless Setup",
        group="Paperless",
        summary="Validate document types, correspondent, custom fields, and select options.",
        script="main.py",
        args=("paperless-check",),
        arg_fields=(
            ArgField("artifact_dir", "Artifact folder", "--artifact-dir", "artifacts", "path"),
            ArgField(
                "artifact_scope",
                "Artifact scope",
                "--artifact-scope",
                "current-scrape",
                "select",
                PAPERLESS_ARTIFACT_SCOPE_OPTIONS,
            ),
        ),
    ),
    CommandSpec(
        id="paperless_sync",
        system_id="pmdu",
        title="Upload to Paperless",
        group="Paperless",
        summary="Create missing Paperless documents; existing documents are skipped.",
        script="main.py",
        args=("paperless",),
        aliases=("phase3", "paperless-upload"),
        env_fields=(
            EnvField("PAPERLESS_MAX_CASES", "Max cases", "", "int"),
            EnvField("PAPERLESS_DRY_RUN", "Dry run", "false", "bool"),
        ),
        arg_fields=(
            ArgField("artifact_dir", "Artifact folder", "--artifact-dir", "artifacts", "path"),
            ArgField(
                "artifact_scope",
                "Artifact scope",
                "--artifact-scope",
                "current-scrape",
                "select",
                PAPERLESS_ARTIFACT_SCOPE_OPTIONS,
            ),
        ),
    ),
    CommandSpec(
        id="paperless_patch",
        system_id="pmdu",
        title="Patch Paperless Metadata",
        group="Paperless",
        summary="Update metadata and custom fields for documents already in Paperless.",
        script="main.py",
        args=("paperless-patch",),
        aliases=("paperless-refresh",),
        env_fields=(
            EnvField("PAPERLESS_MAX_CASES", "Max cases", "", "int"),
            EnvField("PAPERLESS_DRY_RUN", "Dry run", "false", "bool"),
        ),
        arg_fields=(
            ArgField("artifact_dir", "Artifact folder", "--artifact-dir", "artifacts", "path"),
            ArgField(
                "artifact_scope",
                "Artifact scope",
                "--artifact-scope",
                "current-scrape",
                "select",
                PAPERLESS_ARTIFACT_SCOPE_OPTIONS,
            ),
        ),
        confirm_label="Patches metadata on existing Paperless documents",
    ),
    CommandSpec(
        id="notify_preview",
        system_id="pmdu",
        title="Preview Notifications",
        group="Notifications",
        summary="Preview WhatsApp jobs without publishing or recording deliveries.",
        script="main.py",
        args=("notify-preview",),
        arg_fields=notification_fields("group", "none", "all"),
    ),
    CommandSpec(
        id="notify_open_latest_preview",
        system_id="pmdu",
        title="Open Latest Preview",
        group="Notifications",
        summary="Open the latest frozen WhatsApp notification preview batch in the browser.",
        script="open_latest_notification_preview.py",
    ),
    CommandSpec(
        id="pmdu_cleanup_previews_dry_run",
        system_id="pmdu",
        title="Preview Preview-Batch Cleanup",
        group="Preview Cleanup",
        summary="Show notification/follow-up preview batches that would be discarded.",
        script="cleanup_preview_batches.py",
        args=("--dry-run",),
        arg_fields=(
            ArgField("kind", "Preview type", "--kind", "all", "select", PREVIEW_CLEANUP_OPTIONS),
            ArgField(
                "include_notification_downloads",
                "Also discard staged notification downloads",
                "--include-notification-downloads",
                "false",
                "bool_flag",
            ),
        ),
    ),
    CommandSpec(
        id="pmdu_cleanup_previews",
        system_id="pmdu",
        title="Discard Preview Batches",
        group="Preview Cleanup",
        summary="Delete generated notification/follow-up preview batches and latest-preview pointers.",
        script="cleanup_preview_batches.py",
        args=("--yes",),
        arg_fields=(
            ArgField("kind", "Preview type", "--kind", "all", "select", PREVIEW_CLEANUP_OPTIONS),
            ArgField(
                "include_notification_downloads",
                "Also discard staged notification downloads",
                "--include-notification-downloads",
                "false",
                "bool_flag",
            ),
        ),
        confirm_label="Deletes generated preview batches from reports/pmdu",
    ),
    CommandSpec(
        id="pmdu_generate_inquiry_letters",
        system_id="pmdu",
        title="Generate DDEO Inquiry Letters",
        group="Inquiry Letters",
        summary="Generate editable ODT and PDF inquiry letters for under-investigation PMDU complaints.",
        script="generate_inquiry_letters.py",
        arg_fields=(
            ArgField("source", "Source", "--source", "paperless", "select", INQUIRY_SOURCE_OPTIONS),
            ArgField("status", "Paperless status", "--status", "Under Investigation", "text"),
            ArgField("artifact_dir", "Artifact folder", "--artifact-dir", "artifacts", "path"),
            ArgField("output_root", "Output folder", "--output-root", "inquiry_letters_to_ddeos", "path"),
            ArgField("issue_date", "Issue date", "--issue-date", "today", "date"),
            ArgField("max_cases", "Max cases", "--max-cases", "", "int"),
            ArgField("force", "Force regenerate", "--force", "false", "bool_flag"),
        ),
    ),
    CommandSpec(
        id="pmdu_inquiry_date_preview",
        system_id="pmdu",
        title="Preview Inquiry Date Update",
        group="Inquiry Letters",
        summary="Preview changing inquiry order issue/due dates for an existing complaint.",
        script="update_inquiry_order_date.py",
        args=("--dry-run",),
        arg_fields=(
            ArgField("complaint", "Complaint number", "--complaint", "", "text"),
            ArgField("issue_date", "Issue date", "--issue-date", "today", "date"),
            ArgField("output_root", "Output folder", "--output-root", "inquiry_letters_to_ddeos", "path"),
        ),
    ),
    CommandSpec(
        id="pmdu_inquiry_date_apply",
        system_id="pmdu",
        title="Apply Inquiry Date Update",
        group="Inquiry Letters",
        summary="Update inquiry order issue/due dates and record an audit event.",
        script="update_inquiry_order_date.py",
        args=("--yes",),
        arg_fields=(
            ArgField("complaint", "Complaint number", "--complaint", "", "text"),
            ArgField("issue_date", "Issue date", "--issue-date", "today", "date"),
            ArgField("output_root", "Output folder", "--output-root", "inquiry_letters_to_ddeos", "path"),
        ),
        confirm_label="Updates inquiry order dates and records an audit event",
    ),
    CommandSpec(
        id="pmdu_followup_preview",
        system_id="pmdu",
        title="Preview Inquiry Follow-ups",
        group="Inquiry Follow-ups",
        summary="Preview reminder/warning WhatsApp jobs for pending inquiry orders.",
        script="inquiry_followups.py",
        args=("preview",),
        arg_fields=followup_fields(),
    ),
    CommandSpec(
        id="pmdu_followup_open_latest_preview",
        system_id="pmdu",
        title="Open Latest Follow-up Preview",
        group="Inquiry Follow-ups",
        summary="Open the latest inquiry follow-up preview batch in the browser.",
        script="open_latest_followup_preview.py",
    ),
    CommandSpec(
        id="pmdu_followup_send",
        system_id="pmdu",
        title="Send Inquiry Follow-ups",
        group="Inquiry Follow-ups",
        summary="Publish reminder/warning WhatsApp jobs and record follow-up delivery history.",
        script="inquiry_followups.py",
        args=("send",),
        arg_fields=followup_fields(),
        confirm_label="Sends inquiry follow-up WhatsApp jobs through NATS",
    ),
    CommandSpec(
        id="notify_send",
        system_id="pmdu",
        title="Send Notifications",
        group="Notifications",
        summary="Publish WhatsApp jobs through NATS and record delivery state.",
        script="main.py",
        args=("notify-send",),
        arg_fields=notification_fields("group", "none", "all"),
        confirm_label="Sends WhatsApp jobs through NATS",
    ),
    CommandSpec(
        id="pmdu_end_to_end",
        system_id="pmdu",
        title="End-to-End PMDU Flow",
        group="Runbook",
        summary="Run scrape, artifact generation, Paperless check, and Paperless upload in sequence.",
        raw_command=(
            "zsh",
            "-lc",
            uv_shell_command(
                "python main.py phase1",
                "python main.py phase2",
                "python main.py paperless-check --artifact-dir artifacts",
                "python main.py paperless --artifact-dir artifacts",
            ),
        ),
        confirm_label="Runs multiple PMDU stages",
    ),
)
