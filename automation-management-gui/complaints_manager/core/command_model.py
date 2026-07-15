from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Option:
    value: str
    label: str


@dataclass(frozen=True)
class EnvField:
    key: str
    label: str
    default: str = ""
    kind: str = "text"


@dataclass(frozen=True)
class ArgField:
    key: str
    label: str
    flag: str
    default: str = ""
    kind: str = "text"
    options: tuple[Option, ...] = ()
    omit_if_empty: bool = True


@dataclass(frozen=True)
class CommandSpec:
    id: str
    system_id: str
    title: str
    group: str
    summary: str
    script: str | None = None
    args: tuple[str, ...] = ()
    raw_command: tuple[str, ...] = ()
    env_fields: tuple[EnvField, ...] = ()
    arg_fields: tuple[ArgField, ...] = ()
    aliases: tuple[str, ...] = ()
    confirm_label: str = ""
    cwd: str = ""
    required_services: tuple[str, ...] = ()

    def build(self, project_root: Path, values: dict[str, str]) -> list[str]:
        command = self._base_command()
        command.extend(self.args)
        for field in self.arg_fields:
            if field.kind.startswith("hidden_env:"):
                env_key = field.kind.split(":", 1)[1]
                value = values.get(env_key, "").strip()
                if field.omit_if_empty and not value:
                    continue
                command.extend([field.flag, value])
                continue
            value = values.get(field.key, field.default).strip()
            if field.kind == "bool_flag":
                if value.lower() in {"1", "true", "yes", "y", "on"}:
                    command.append(field.flag)
                continue
            if field.omit_if_empty and not value:
                continue
            command.extend([field.flag, value])
        return command

    def working_dir(self, project_root: Path) -> Path:
        if not self.cwd:
            return default_system_root(project_root, self.system_id)
        path = Path(self.cwd).expanduser()
        if not path.is_absolute():
            path = project_root / path
        return path.resolve(strict=False)

    def build_env(self, values: dict[str, str]) -> dict[str, str]:
        env: dict[str, str] = {}
        for field in self.env_fields:
            value = values.get(field.key, field.default).strip()
            if value:
                env[field.key] = value
        return env

    def _base_command(self) -> list[str]:
        if self.raw_command:
            return list(self.raw_command)
        if not self.script:
            raise ValueError(f"Command {self.id} has no script or raw command")
        uv = find_uv()
        if uv:
            return [uv, "run", "python", self.script]
        return [sys.executable, self.script]


@dataclass(frozen=True)
class SystemSpec:
    id: str
    title: str
    subtitle: str
    summary: str
    accent: tuple[float, float, float, float]
    command_ids: tuple[str, ...] = field(default_factory=tuple)


def find_uv() -> str | None:
    uv_path = shutil.which("uv")
    if uv_path:
        return uv_path
    local_uv = Path.home() / ".local" / "bin" / "uv"
    if local_uv.exists():
        return str(local_uv)
    return None


def default_system_root(project_root: Path, system_id: str) -> Path:
    if system_id == "crm":
        return (project_root.parent / "crm-management-system").resolve(strict=False)
    if system_id == "pmdu":
        return (project_root.parent / "pmdu-management-system").resolve(strict=False)
    if system_id == "whatsapp_groups":
        return (project_root.parent / "add-to-whatsapp-group").resolve(strict=False)
    if system_id == "whatsapp_accounts":
        return (project_root.parent / "whatsappbot").resolve(strict=False)
    if system_id == "sms_campaigns":
        return (project_root.parent / "sms-campaign-manager").resolve(strict=False)
    return project_root.resolve(strict=False)


def uv_executable() -> str:
    return find_uv() or "uv"


def uv_shell_command(*commands: str) -> str:
    uv = uv_executable()
    return " && ".join(f"{uv} run {command}" for command in commands)
