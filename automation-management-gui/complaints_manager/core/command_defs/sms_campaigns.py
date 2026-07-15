from __future__ import annotations

from complaints_manager.core.command_model import ArgField, CommandSpec, uv_executable


COMMANDS: tuple[CommandSpec, ...] = (
    CommandSpec(
        id="sms_campaign_create",
        system_id="sms_campaigns",
        title="Create SMS Campaign",
        group="Campaign Management",
        summary="Create a new named durable SMS campaign from an Excel file, selected number column, and message template.",
        raw_command=(uv_executable(), "run", "sms-campaign", "campaigns", "create"),
        cwd="../sms-campaign-manager",
        arg_fields=(
            ArgField(key="campaign_id", label="New campaign name / reference", flag="--campaign-id", omit_if_empty=False),
            ArgField(key="description", label="Description", flag="--description"),
            ArgField(key="file", label="Excel file", flag="--file", default="../add-to-whatsapp-group/lahore-ht-test.xlsx", kind="file", omit_if_empty=False),
            ArgField(key="sheet", label="Sheet", flag="--sheet", kind="excel_sheet"),
            ArgField(key="column", label="Number column", flag="--column", kind="excel_column", omit_if_empty=False),
            ArgField(key="message", label="Message", flag="--message", kind="multiline", omit_if_empty=False),
            ArgField(key="country_code", label="Default country code", flag="--country-code", default="92", omit_if_empty=False),
        ),
    ),
    CommandSpec(
        id="sms_campaign_update",
        system_id="sms_campaigns",
        title="Update SMS Campaign",
        group="Campaign Management",
        summary="Select an existing campaign, load its saved settings, edit them, and save the updated campaign definition.",
        raw_command=(uv_executable(), "run", "sms-campaign", "campaigns", "create"),
        cwd="../sms-campaign-manager",
        arg_fields=(
            ArgField(key="campaign_id", label="Existing campaign", flag="--campaign-id", kind="sms_campaign", omit_if_empty=False),
            ArgField(key="description", label="Description", flag="--description"),
            ArgField(key="file", label="Excel file", flag="--file", kind="file", omit_if_empty=False),
            ArgField(key="sheet", label="Sheet", flag="--sheet", kind="excel_sheet"),
            ArgField(key="column", label="Number column", flag="--column", kind="excel_column", omit_if_empty=False),
            ArgField(key="message", label="Message", flag="--message", kind="multiline", omit_if_empty=False),
            ArgField(key="country_code", label="Default country code", flag="--country-code", default="92", omit_if_empty=False),
        ),
    ),
    CommandSpec(
        id="sms_campaign_list",
        system_id="sms_campaigns",
        title="List Campaigns",
        group="Campaign Management",
        summary="List all saved SMS campaigns with campaign-wise status counts.",
        raw_command=(uv_executable(), "run", "sms-campaign", "campaigns", "list"),
        cwd="../sms-campaign-manager",
    ),
    CommandSpec(
        id="sms_campaign_delete",
        system_id="sms_campaigns",
        title="Archive / Delete Campaign",
        group="Campaign Management",
        summary="Archive a campaign so it disappears from normal selectors while preserving audit and delivery history.",
        raw_command=(uv_executable(), "run", "sms-campaign", "campaigns", "delete"),
        cwd="../sms-campaign-manager",
        confirm_label=(
            "This archives the selected campaign. It will be hidden from normal campaign dropdowns, "
            "but delivery history is preserved for audit and recovery."
        ),
        arg_fields=(
            ArgField(key="campaign_id", label="Campaign", flag="--campaign-id", kind="sms_campaign", omit_if_empty=False),
        ),
    ),
    CommandSpec(
        id="sms_campaign_show",
        system_id="sms_campaigns",
        title="Show Campaign Summary",
        group="Campaign Management",
        summary="Show one campaign's saved definition and delivery ledger summary.",
        raw_command=(uv_executable(), "run", "sms-campaign", "campaigns", "show"),
        cwd="../sms-campaign-manager",
        arg_fields=(
            ArgField(key="campaign_id", label="Campaign", flag="--campaign-id", kind="sms_campaign", omit_if_empty=False),
        ),
    ),
    CommandSpec(
        id="sms_campaign_reconcile",
        system_id="sms_campaigns",
        title="Refresh Delivery Statuses",
        group="Delivery Reports",
        summary="Query SMSGate for queued message states and update the durable SQLite ledger.",
        raw_command=(uv_executable(), "run", "sms-campaign", "reconcile"),
        cwd="../sms-campaign-manager",
        arg_fields=(
            ArgField(key="campaign_id", label="Campaign (blank = all campaigns)", flag="--campaign-id", kind="sms_campaign"),
        ),
    ),
    CommandSpec(
        id="sms_campaign_failed",
        system_id="sms_campaigns",
        title="List Failed Recipients",
        group="Delivery Reports",
        summary="List failed recipients and failure reasons for the selected campaign.",
        raw_command=(uv_executable(), "run", "sms-campaign", "campaigns", "failed"),
        cwd="../sms-campaign-manager",
        arg_fields=(
            ArgField(key="campaign_id", label="Campaign", flag="--campaign-id", kind="sms_campaign", omit_if_empty=False),
        ),
    ),
    CommandSpec(
        id="sms_campaign_send",
        system_id="sms_campaigns",
        title="Send Excel SMS Campaign",
        group="Campaigns",
        summary=(
            "Select an Excel number column, preview normalized recipients and SMS segments, "
            "then submit a durable rate-limited campaign through SMSGate for Android."
        ),
        raw_command=(uv_executable(), "run", "sms-campaign", "send"),
        cwd="../sms-campaign-manager",
        confirm_label=(
            "This can send real SMS messages through the selected Android device. Confirm the "
            "recipients consented, are not suppressed/DNCR, and the preview and limits are correct."
        ),
        arg_fields=(
            ArgField(key="campaign_id", label="Campaign", flag="--campaign-id", kind="sms_campaign"),
            ArgField(key="file", label="Excel file (optional when campaign is selected)", flag="--file", default="../add-to-whatsapp-group/lahore-ht-test.xlsx", kind="file"),
            ArgField(key="sheet", label="Sheet", flag="--sheet", kind="excel_sheet"),
            ArgField(key="column", label="Number column (optional when campaign is selected)", flag="--column", kind="excel_column"),
            ArgField(key="message", label="Message (optional when campaign is selected)", flag="--message", kind="multiline"),
            ArgField(key="country_code", label="Default country code", flag="--country-code", default="92", omit_if_empty=False),
            ArgField(key="minute_limit", label="Maximum SMS segments / rolling minute", flag="--minute-limit", default="5", omit_if_empty=False),
            ArgField(key="hour_limit", label="Maximum SMS segments / rolling hour", flag="--hour-limit", default="50", omit_if_empty=False),
            ArgField(key="day_limit", label="Maximum SMS segments / rolling 24 hours", flag="--day-limit", default="200", omit_if_empty=False),
            ArgField(key="delay_seconds", label="Minimum delay between recipients (seconds)", flag="--delay-seconds", default="12", omit_if_empty=False),
            ArgField(key="sim_number", label="SIM slot", flag="--sim-number", default="1", omit_if_empty=False),
            ArgField(key="ttl", label="Gateway queue expiry (seconds)", flag="--ttl", default="3600", omit_if_empty=False),
            ArgField(key="retry_failed", label="Retry confirmed failed recipients only", flag="--retry-failed", default="false", kind="bool_flag"),
            ArgField(key="max_attempts", label="Maximum attempts per recipient", flag="--max-attempts", default="3", omit_if_empty=False),
            ArgField(key="watch_until_complete", label="Watch until gateway confirms sent/delivered/failed", flag="--watch-until-complete", default="true", kind="bool_flag"),
            ArgField(key="watch_timeout_seconds", label="Watch timeout after submission (seconds)", flag="--watch-timeout-seconds", default="7200", omit_if_empty=False),
            ArgField(key="watch_poll_seconds", label="Watch status polling interval (seconds)", flag="--watch-poll-seconds", default="30", omit_if_empty=False),
            ArgField(key="consent_confirmed", label="Recipients consented and suppression/DNCR checks are complete", flag="--consent-confirmed", default="false", kind="bool_flag"),
            ArgField(key="execute", label="Execute (unchecked = preview only)", flag="--execute", default="false", kind="bool_flag"),
        ),
    ),
)
