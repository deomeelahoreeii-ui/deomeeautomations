from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

from complaints_manager.core.app_settings import load_app_settings, save_app_settings
from complaints_manager.core.commands import COMMANDS, CommandSpec
from complaints_manager.core.runner import CommandRunner
from complaints_manager.core.services import ServiceManager
from complaints_manager.core.stats import metrics_for


@dataclass
class GuiState:
    project_root: Path
    runner: CommandRunner
    services: ServiceManager
    current_system_id: str | None = None
    current_service_id: str | None = None
    selected_command_id: str = COMMANDS[0].id
    field_values: dict[str, dict[str, str]] = field(default_factory=dict)
    auto_scroll: bool = True
    show_confirm_for: str | None = None
    last_metrics_refresh: float = 0.0
    cached_metrics: dict[str, list] = field(default_factory=dict)
    app_settings: dict = field(default_factory=dict)
    folder_dialogs: dict[str, Any] = field(default_factory=dict)
    file_dialogs: dict[str, Any] = field(default_factory=dict)
    service_auto_scroll: bool = True
    antidengue_form: dict[str, str] = field(default_factory=dict)
    show_whatsapp_auth_reset_confirm: bool = False
    whatsapp_auth_reset_service_id: str = ""
    whatsapp_account_setup_service_id: str = ""
    new_whatsapp_account_id: str = ""
    new_whatsapp_account_name: str = ""
    edit_whatsapp_account_id: str = ""
    edit_whatsapp_account_name: str = ""
    sms_campaign_cache: list[dict[str, str]] = field(default_factory=list)
    sms_campaign_cache_path: str = ""
    sms_campaign_cache_at: float = 0.0
    sms_campaign_last_error: str = ""
    antidengue_token_cache: dict[str, dict[str, str]] = field(default_factory=dict)
    antidengue_token_cache_at: float = 0.0
    antidengue_view: str = "dispatch"
    antidengue_dispatch_view: str = "overview"
    antidengue_history_selected_run_key: str = ""
    antidengue_history_filter: str = "active"
    antidengue_history_pending_delete_run_key: str = ""
    last_gui_error: str = ""
    command_start_in_progress: bool = False

    @classmethod
    def create(cls, project_root: Path) -> "GuiState":
        state = cls(
            project_root=project_root,
            runner=CommandRunner(project_root),
            services=ServiceManager(project_root),
            app_settings=load_app_settings(project_root),
        )
        state._load_defaults()
        return state

    def _load_defaults(self) -> None:
        saved_values = self.app_settings.get("command_values", {})
        if not isinstance(saved_values, dict):
            saved_values = {}
        for command in COMMANDS:
            command_root = self.command_root(command)
            env = dotenv_values(command_root / ".env")
            values: dict[str, str] = {}
            for field in command.env_fields:
                values[field.key] = str(env.get(field.key, field.default) or field.default)
            for field in command.arg_fields:
                values[field.key] = field.default
            command_saved = saved_values.get(command.id, {})
            if isinstance(command_saved, dict):
                values.update(
                    {
                        str(key): str(value)
                        for key, value in command_saved.items()
                        if value is not None
                    }
                )
            if command.id == "crm_filter_pdf_duplicates":
                input_dir = self._resolve_path(command_root, values.get("input_dir", ""))
                output_dir = self._resolve_path(command_root, values.get("output_dir", ""))
                if self._path_is_inside(input_dir, output_dir):
                    values["input_dir"] = "phase1-crm/unprocessed-crm/pdf"
            if (
                command.id == "crm_ingest_pending"
                and values.get("input_dir") == "phase1-crm/unprocessed-crm/filtered/fresh/number-missing"
            ):
                values["input_dir"] = "phase1-crm/unprocessed-crm/filtered/fresh/number-found"
            if command.id == "whatsapp_group_import":
                if values.get("delay_ms") == "1500":
                    values["delay_ms"] = "15000"
                if values.get("wait_timeout") == "300":
                    values["wait_timeout"] = "1800"
                values.setdefault("consent_confirmed", "false")
            self.field_values[command.id] = values

        self.antidengue_form = {
            "mode": str(self.app_settings.get("antidengue_mode", "daily")),
            "date": str(self.app_settings.get("antidengue_date", date.today().isoformat())),
            "times": str(self.app_settings.get("antidengue_times", "09:00, 13:00")),
            "weekdays": str(self.app_settings.get("antidengue_weekdays", "0,1,2,3,4,5")),
        }

    def command_values(self, command: CommandSpec) -> dict[str, str]:
        return self.field_values.setdefault(command.id, {})

    def command_root(self, command: CommandSpec) -> Path:
        return command.working_dir(self.project_root)

    def system_root(self, system_id: str) -> Path:
        if system_id == "services":
            return self.project_root.resolve(strict=False)
        if system_id == "crm":
            return (self.project_root.parent / "crm-management-system").resolve(strict=False)
        if system_id == "pmdu":
            return (self.project_root.parent / "pmdu-management-system").resolve(strict=False)
        if system_id == "whatsapp_groups":
            return (self.project_root.parent / "add-to-whatsapp-group").resolve(strict=False)
        if system_id == "whatsapp_accounts":
            return (self.project_root.parent / "whatsappbot").resolve(strict=False)
        if system_id == "sms_campaigns":
            return (self.project_root.parent / "sms-campaign-manager").resolve(strict=False)
        return self.project_root.resolve(strict=False)

    def _resolve_path(self, root: Path, value: str) -> Path:
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = root / path
        return path.resolve(strict=False)

    @staticmethod
    def _path_is_inside(path: Path, parent: Path) -> bool:
        try:
            path.relative_to(parent)
        except ValueError:
            return False
        return True

    def persist_command_values(self, command: CommandSpec) -> None:
        command_values = self.app_settings.setdefault("command_values", {})
        for known_command in COMMANDS:
            persisted = dict(self.command_values(known_command))
            if known_command.id == "sms_campaign_send":
                persisted.pop("message", None)
            command_values[known_command.id] = persisted
        save_app_settings(self.project_root, self.app_settings)

    def propagate_env_value(self, key: str, value: str) -> None:
        for command in COMMANDS:
            if any(field.key == key for field in command.env_fields):
                self.command_values(command)[key] = value

    def antidengue_schedules(self) -> list[dict[str, Any]]:
        schedules = self.app_settings.setdefault("antidengue_schedules", [])
        if not isinstance(schedules, list):
            schedules = []
            self.app_settings["antidengue_schedules"] = schedules
        return schedules

    def persist_antidengue_form(self) -> None:
        self.app_settings["antidengue_mode"] = self.antidengue_form.get("mode", "daily")
        self.app_settings["antidengue_date"] = self.antidengue_form.get("date", "")
        self.app_settings["antidengue_times"] = self.antidengue_form.get("times", "")
        self.app_settings["antidengue_weekdays"] = self.antidengue_form.get("weekdays", "")
        save_app_settings(self.project_root, self.app_settings)

    def add_antidengue_schedule(self) -> None:
        schedule = {
            "id": uuid.uuid4().hex,
            "enabled": True,
            "mode": self.antidengue_form.get("mode", "daily"),
            "date": self.antidengue_form.get("date", ""),
            "times": self.antidengue_form.get("times", ""),
            "weekdays": self.antidengue_form.get("weekdays", ""),
            "last_run_key": "",
        }
        self.antidengue_schedules().append(schedule)
        self.persist_antidengue_form()

    def remove_antidengue_schedule(self, schedule_id: str) -> None:
        schedules = [
            item
            for item in self.antidengue_schedules()
            if str(item.get("id")) != schedule_id
        ]
        self.app_settings["antidengue_schedules"] = schedules
        save_app_settings(self.project_root, self.app_settings)

    def update_antidengue_schedule(self, schedule: dict[str, Any]) -> None:
        schedules = self.antidengue_schedules()
        for index, item in enumerate(schedules):
            if str(item.get("id")) == str(schedule.get("id")):
                schedules[index] = schedule
                break
        save_app_settings(self.project_root, self.app_settings)

    def metrics(self, system_id: str):
        now = time.monotonic()
        if now - self.last_metrics_refresh > 2 or system_id not in self.cached_metrics:
            self.cached_metrics[system_id] = metrics_for(system_id, self.project_root)
            self.last_metrics_refresh = now
        return self.cached_metrics[system_id]
