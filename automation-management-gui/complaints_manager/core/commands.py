from __future__ import annotations

from complaints_manager.core.command_defs import (
    antidengue,
    crm,
    crm_maintenance,
    pmdu,
    pmdu_maintenance,
    sms_campaigns,
    whatsapp_groups,
)
from complaints_manager.core.command_model import (
    ArgField,
    CommandSpec,
    EnvField,
    Option,
    SystemSpec,
)


SYSTEMS: tuple[SystemSpec, ...] = (
    SystemSpec(
        id="services",
        title="Services",
        subtitle="NATS, WhatsApp, and PocketBase",
        summary="Start, stop, restart, and inspect infrastructure services used by SMS, WhatsApp accounts, group automation, and AntiDengue storage.",
        accent=(0.72, 0.62, 0.95, 1.0),
    ),
    SystemSpec(
        id="crm",
        title="CRM",
        subtitle="CRM complaints and compliances",
        summary="Filter CRM sheets/PDFs, prepare uploads, upload complaints/compliances, generate letters, and dispatch reports.",
        accent=(0.25, 0.70, 0.45, 1.0),
    ),
    SystemSpec(
        id="pmdu",
        title="PMDU",
        subtitle="Portal complaints",
        summary="Scrape portal complaints, build artifacts, upload to Paperless, and send under-investigation notifications.",
        accent=(0.25, 0.55, 0.95, 1.0),
    ),
    SystemSpec(
        id="antidengue",
        title="AntiDengue",
        subtitle="Dengue activity reports",
        summary="Run AntiDengue reporting now or schedule it for selected times.",
        accent=(0.68, 0.85, 0.30, 1.0),
    ),
    SystemSpec(
        id="maintenance",
        title="Maintenance",
        subtitle="Repair and housekeeping",
        summary="Run CRM and PMDU repair, Paperless sync, and housekeeping commands.",
        accent=(0.85, 0.62, 0.28, 1.0),
    ),
    SystemSpec(
        id="sms_campaigns",
        title="SMS Campaigns",
        subtitle="Excel campaigns through SMSGate",
        summary="Preview and send consented, durable, rate-limited SMS campaigns through an Android gateway.",
        accent=(0.28, 0.72, 0.90, 1.0),
    ),
    SystemSpec(
        id="whatsapp_accounts",
        title="WhatsApp Accounts",
        subtitle="Worker profiles and logins",
        summary="Manage multiple isolated WhatsApp accounts, QR login state, and per-account workers.",
        accent=(0.20, 0.68, 0.92, 1.0),
    ),
    SystemSpec(
        id="whatsapp_groups",
        title="WhatsApp Groups",
        subtitle="Excel contact imports",
        summary="Preview and queue configurable Excel contact ranges for WhatsApp group membership.",
        accent=(0.18, 0.78, 0.48, 1.0),
    ),
)

COMMANDS: tuple[CommandSpec, ...] = (
    *crm.COMMANDS,
    *pmdu.COMMANDS,
    *antidengue.COMMANDS,
    *crm_maintenance.COMMANDS,
    *pmdu_maintenance.COMMANDS,
    *sms_campaigns.COMMANDS,
    *whatsapp_groups.ACCOUNT_COMMANDS,
    *whatsapp_groups.COMMANDS,
)


def commands_by_system() -> dict[str, list[CommandSpec]]:
    grouped: dict[str, list[CommandSpec]] = {system.id: [] for system in SYSTEMS}
    for command in COMMANDS:
        grouped.setdefault(command.system_id, []).append(command)
    return grouped


def command_by_id(command_id: str) -> CommandSpec:
    for command in COMMANDS:
        if command.id == command_id:
            return command
    raise KeyError(command_id)
