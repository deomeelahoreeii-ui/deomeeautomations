"""Compatibility exports for the modular GUI implementation.

New code should import from the focused submodules instead of this facade.
"""

from complaints_manager.gui.common.context import (
    disabled_when,
)
from complaints_manager.gui.common.colors import (
    status_color,
    service_status_color,
    sms_campaign_status_color,
    status_badge_color,
)
from complaints_manager.gui.common.formatting import (
    format_elapsed,
    parse_datetime_parts,
    combine_datetime_value,
    draw_clipped_text,
    _option_index,
    _safe_json_loads,
)
from complaints_manager.gui.common.desktop import (
    open_folder_from_gui,
    open_external_file,
    count_path_items,
    open_desktop_path,
    open_url_from_gui,
)
from complaints_manager.gui.navigation import (
    system_by_id,
    draw_top_bar,
    draw_metric_row,
)
from complaints_manager.gui.services.views import (
    draw_services_panel,
    draw_service_detail,
)
from complaints_manager.gui.commands.validation import (
    command_validation_error,
)
from complaints_manager.gui.commands.list_view import (
    draw_command_list,
)
from complaints_manager.gui.commands.fields import (
    draw_field,
    draw_env_field,
)
from complaints_manager.gui.commands.execution import (
    start_command_with_services,
    _start_command_with_services_worker,
)
from complaints_manager.gui.commands.detail import (
    draw_command_detail,
    draw_confirm_modal,
)
from complaints_manager.gui.commands.logs import (
    draw_logs,
)
from complaints_manager.gui.crm.actions import (
    clear_duplicate_filter_results,
    copy_excel_filter_values_to_notice_command,
)
from complaints_manager.gui.sms.campaigns import (
    apply_sms_campaign_to_command,
    sms_campaign_database_path,
    discovered_sms_campaigns,
    draw_sms_campaign_selector,
    draw_sms_campaign_list_dashboard,
)
from complaints_manager.gui.whatsapp.groups import (
    discovered_whatsapp_groups,
    draw_whatsapp_group_selector,
    draw_whatsapp_account_selector,
)
from complaints_manager.gui.whatsapp.accounts import (
    reset_whatsapp_authentication,
    whatsapp_service_id,
    whatsapp_service_for_account,
    whatsapp_qr_path,
    draw_whatsapp_accounts_dashboard,
    draw_whatsapp_account_card,
    draw_whatsapp_account_add_screen,
    draw_whatsapp_reset_modal,
)
from complaints_manager.gui.antidengue.schedule import (
    parse_schedule_times,
    parse_weekdays,
    format_weekdays,
    schedule_matches_now,
    next_schedule_run,
    process_antidengue_schedules,
)
from complaints_manager.gui.antidengue.runtime import (
    antidengue_root,
    antidengue_pocketbase_root,
    whatsappbot_root,
    whatsapp_auth_dir,
    latest_antidengue_summary,
    read_json_file,
    bailey_token_data_length,
    is_baileys_tctoken_expired,
    lid_for_phone,
    antidengue_token_status_for_mobile,
    refresh_antidengue_token_status,
    start_or_check_antidengue_worker,
    start_or_check_control_api,
    is_control_api_reachable,
    draw_antidengue_worker_controls,
)
from complaints_manager.gui.antidengue.health_repository import (
    antidengue_pocketbase_counts,
    antidengue_pocketbase_health_checks,
    open_pocketbase_read_connection,
)
from complaints_manager.gui.antidengue.database import (
    _pocketbase_report_runs_has_lifecycle,
    _now_iso,
    antidengue_record_id,
    open_pocketbase_write_connection,
    _pb_bool,
    _one_or_zero,
    _dispatch_settings_defaults,
    load_antidengue_dispatch_settings,
    save_antidengue_dispatch_settings,
    load_antidengue_dispatch_groups,
    load_antidengue_scope_options,
    _normalized_emis,
    normalize_pk_mobile_for_gui,
    _school_level_from_name_gui,
    _pocketbase_table_columns,
    _master_school_form_defaults,
    antidengue_new_school_form,
    load_antidengue_master_data_options,
    _relation_options,
    _set_single_default,
    _draw_relation_combo,
    resolve_antidengue_school_owners,
    validate_antidengue_new_school_form,
    create_antidengue_school_from_form,
    update_antidengue_dispatch_group,
    add_antidengue_dispatch_group,
    record_antidengue_dispatch_event,
    load_antidengue_latest_dispatch_context,
)
from complaints_manager.gui.antidengue.delivery import (
    failed_officer_rows,
    failed_delivery_statuses,
    delivery_failure_cause,
)
from complaints_manager.gui.antidengue.messages import (
    _group_scope_label,
    _rows_for_dispatch_group,
    _school_count_text,
    _school_line,
    _tehsil_summary_lines,
    _markaz_summary_lines,
    _summary_only_sections,
    _hierarchy_school_sections,
    _markaz_school_sections,
    _grouped_school_sections,
    build_antidengue_group_dispatch_message,
    build_antidengue_manual_message,
)
from complaints_manager.gui.antidengue.history_repository import (
    set_antidengue_run_archived,
    delete_antidengue_run_history,
    clear_archived_antidengue_runs,
    _antidengue_run_key,
    load_antidengue_run_history,
    load_antidengue_run_details,
)
from complaints_manager.gui.antidengue.dispatch_views import (
    draw_delivery_metric,
    _record_dispatch_action_result,
    draw_antidengue_dispatch_settings_controls,
    draw_antidengue_dispatch_overview,
    draw_antidengue_dispatch_individuals,
    _draw_group_scope_selector,
    _tehsil_name_by_id,
    draw_existing_group_scope_selector,
    draw_antidengue_group_add_form,
    draw_antidengue_dispatch_groups,
    draw_antidengue_dispatch_center_section,
)
from complaints_manager.gui.antidengue.sections import (
    draw_antidengue_health_section,
    draw_antidengue_failed_officers_section,
    draw_antidengue_group_delivery_section,
    draw_antidengue_files_section,
    draw_antidengue_manual_report_section,
    _draw_master_owner_preview,
    _draw_master_school_choice_fields,
    draw_antidengue_master_data_section,
    draw_antidengue_database_section,
)
from complaints_manager.gui.antidengue.history_view import (
    _format_history_time,
    _history_summary_output_dir,
    _run_is_dry,
    _run_mode_label,
    draw_history_filter_button,
    _record_history_action_result,
    draw_antidengue_run_history_section,
)
from complaints_manager.gui.antidengue.dashboard import (
    draw_antidengue_view_button,
    draw_antidengue_run_status_screen,
    draw_antidengue_run_controls,
    draw_antidengue_dashboard,
)
from complaints_manager.gui.antidengue.schedule import (
    WEEKDAYS,
    PORTAL_LOGIN_MODES,
)
from complaints_manager.gui.antidengue.database import (
    DISPATCH_SETTING_ID,
    MASTER_SCHOOL_SHIFTS,
    MASTER_SCHOOL_TYPES,
    MASTER_SCHOOL_LEVELS,
)
from complaints_manager.gui.antidengue.dispatch_views import (
    DISPATCH_MESSAGE_MODES,
    DISPATCH_ROUTE_KINDS,
)
from complaints_manager.gui.antidengue.dashboard import (
    ANTIDENGUE_VIEWS,
)

__all__ = [name for name in globals() if not name.startswith("_")]
