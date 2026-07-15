from __future__ import annotations

from complaints_manager.core.command_model import ArgField, CommandSpec, EnvField


COMMANDS: tuple[CommandSpec, ...] = (
    CommandSpec(
        id="antidengue_run",
        system_id="antidengue",
        title="Run AntiDengue",
        group="Run",
        summary="Run the AntiDengue automation and publish WhatsApp jobs.",
        raw_command=(
            "/home/ahmad/code/deomeeautomations/antidengue/.venv/bin/python",
            "main.py",
        ),
        cwd="/home/ahmad/code/deomeeautomations/antidengue",
        required_services=("nats", "whatsapp"),
        env_fields=(
            EnvField("PORTAL_LOGIN_MODE", "Portal login mode", "manual"),
        ),
        confirm_label="Runs AntiDengue automation and may queue WhatsApp jobs",
    ),
    CommandSpec(
        id="antidengue_manual_file",
        system_id="antidengue",
        title="Send Manual AntiDengue Report",
        group="Manual",
        summary="Process a manually downloaded AntiDengue portal report and send the remaining WhatsApp workflow.",
        raw_command=(
            "/home/ahmad/code/deomeeautomations/antidengue/.venv/bin/python",
            "main.py",
        ),
        args=("manual-file",),
        cwd="/home/ahmad/code/deomeeautomations/antidengue",
        required_services=("nats", "whatsapp"),
        arg_fields=(
            ArgField("file", "Downloaded report file", "--file", "", "file", omit_if_empty=False),
            ArgField("dry_run", "Build only; do not send WhatsApp", "--dry-run", "false", "bool_flag"),
        ),
        confirm_label="Processes selected manual report and may queue WhatsApp jobs",
    ),
    CommandSpec(
        id="antidengue_pocketbase_migrate",
        system_id="antidengue",
        title="Apply AntiDengue PocketBase Migrations",
        group="Database",
        summary="Apply versioned PocketBase schema migrations for AntiDengue data.",
        raw_command=("./pocketbase", "migrate", "up"),
        cwd="/home/ahmad/code/deomeeautomations/antidengue-pocketbase",
        confirm_label="Applies AntiDengue PocketBase database migrations",
    ),
    CommandSpec(
        id="antidengue_pocketbase_import_officers",
        system_id="antidengue",
        title="Sync AntiDengue Data to PocketBase",
        group="Database",
        summary="Import officers_list.csv and whatsapp_recipients.csv into PocketBase schools, officers, jurisdictions, and fixed recipients.",
        raw_command=("python3", "scripts/import_officers_csv.py"),
        cwd="/home/ahmad/code/deomeeautomations/antidengue-pocketbase",
        confirm_label="Imports current AntiDengue CSV data into PocketBase",
    ),
)
