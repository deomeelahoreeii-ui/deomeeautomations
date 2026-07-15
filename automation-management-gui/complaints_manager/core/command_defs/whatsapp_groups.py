from __future__ import annotations

from complaints_manager.core.command_model import ArgField, CommandSpec, Option, uv_executable


ACCOUNT_COMMANDS: tuple[CommandSpec, ...] = (
    CommandSpec(
        id="whatsapp_accounts",
        system_id="whatsapp_accounts",
        title="WhatsApp Accounts",
        group="Account Management",
        summary="Manage isolated WhatsApp worker accounts, login state, QR authentication, and per-account service controls.",
        raw_command=(),
        cwd="../whatsappbot",
    ),
    CommandSpec(
        id="whatsapp_account_add",
        system_id="whatsapp_accounts",
        title="Add WhatsApp Account",
        group="Account Management",
        summary="Create a new isolated WhatsApp account profile and start QR login for it.",
        raw_command=(),
        cwd="../whatsappbot",
    ),
)


COMMANDS: tuple[CommandSpec, ...] = (
    CommandSpec(
        id="whatsapp_group_membership_audit",
        system_id="whatsapp_groups",
        title="Audit Group Membership",
        group="Reports",
        summary=(
            "Read current group membership and generate an Excel report containing "
            "complete source records for applicants who are not members."
        ),
        raw_command=(uv_executable(), "run", "audit-whatsapp-group-members"),
        cwd="../add-to-whatsapp-group",
        required_services=("nats", "whatsapp"),
        arg_fields=(
            ArgField(key="file", label="Excel file", flag="--file", default="lahore-ht-test.xlsx", kind="file", omit_if_empty=False),
            ArgField(key="group_jid", label="WhatsApp group JID", flag="--group-jid", omit_if_empty=False),
            ArgField(key="output", label="Report output", flag="--output", default="reports/not-members-report.xlsx", omit_if_empty=False),
            ArgField(key="sheet", label="Sheet (blank = first)", flag="--sheet"),
            ArgField(key="country_code", label="Default country code", flag="--country-code", default="92", omit_if_empty=False),
            ArgField(key="worker_id", label="WhatsApp account", flag="--worker-id", default="default", omit_if_empty=False),
            ArgField(key="health_subject", label="Worker health subject", flag="--health-subject", default="whatsapp.worker.health", omit_if_empty=False),
            ArgField(key="members_subject", label="Membership query subject", flag="--members-subject", default="whatsapp.worker.group-members", omit_if_empty=False),
            ArgField(key="worker_ready_timeout", label="Worker startup timeout (seconds)", flag="--worker-ready-timeout", default="120", omit_if_empty=False),
        ),
    ),
    CommandSpec(
        id="whatsapp_group_import",
        system_id="whatsapp_groups",
        title="Import Excel Contacts",
        group="Group Membership",
        summary=(
            "Preview or queue a configurable range from the contact_no Excel column "
            "for addition to a WhatsApp group."
        ),
        raw_command=(uv_executable(), "run", "add-to-whatsapp-group"),
        cwd="../add-to-whatsapp-group",
        required_services=("nats", "whatsapp"),
        confirm_label=(
            "When Execute is enabled, this changes WhatsApp group membership. "
            "Verify the file, group JID, selection, and preview before continuing."
        ),
        arg_fields=(
            ArgField(
                key="file",
                label="Excel file",
                flag="--file",
                default="lahore-ht-test.xlsx",
                kind="file",
                omit_if_empty=False,
            ),
            ArgField(
                key="group_jid",
                label="WhatsApp group JID",
                flag="--group-jid",
                default="",
            ),
            ArgField(
                key="selection",
                label="Selection",
                flag="--selection",
                default="last",
                kind="select",
                options=(
                    Option("last", "Last applicants"),
                    Option("first", "First applicants"),
                    Option("range", "Position range"),
                    Option("all", "All applicants"),
                ),
                omit_if_empty=False,
            ),
            ArgField(key="count", label="Count", flag="--count", default="50"),
            ArgField(key="start", label="Range start (1-based)", flag="--start"),
            ArgField(key="end", label="Range end (inclusive)", flag="--end"),
            ArgField(key="sheet", label="Sheet (blank = first)", flag="--sheet"),
            ArgField(
                key="country_code",
                label="Default country code",
                flag="--country-code",
                default="92",
                omit_if_empty=False,
            ),
            ArgField(
                key="delay_ms",
                label="Delay per addition (ms)",
                flag="--delay-ms",
                default="15000",
                omit_if_empty=False,
            ),
            ArgField(
                key="wait_timeout",
                label="Completion timeout (seconds)",
                flag="--wait-timeout",
                default="1800",
                omit_if_empty=False,
            ),
            ArgField(
                key="worker_ready_timeout",
                label="Worker startup timeout (seconds)",
                flag="--worker-ready-timeout",
                default="120",
                omit_if_empty=False,
            ),
            ArgField(
                key="consent_confirmed",
                label="Recipients were informed and consented",
                flag="--consent-confirmed",
                default="false",
                kind="bool_flag",
            ),
            ArgField(
                key="execute",
                label="Execute (unchecked = preview only)",
                flag="--execute",
                default="false",
                kind="bool_flag",
            ),
        ),
    ),
)
