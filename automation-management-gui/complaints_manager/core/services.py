from __future__ import annotations

import os
import json
import queue
import re
import shutil
import signal
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse


MAX_SERVICE_LOG_LINES = 2_000
DEFAULT_NATS_URL = "nats://localhost:4222"
DEFAULT_WHATSAPP_HEALTH_SUBJECT = "whatsapp.worker.health"


@dataclass(frozen=True)
class ServiceSpec:
    id: str
    title: str
    summary: str
    command: tuple[str, ...]
    cwd: Path
    env: tuple[tuple[str, str], ...] = ()
    kind: str = "service"
    profile_id: str = ""


@dataclass
class ManagedService:
    spec: ServiceSpec
    process: subprocess.Popen[str] | None = None
    started_at: float = 0.0
    reader: threading.Thread | None = None
    log_lines: list[str] = field(default_factory=list)
    last_exit_code: int | None = None
    status_message: str = "Stopped"

    @property
    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def elapsed_seconds(self) -> int:
        if not self.is_running:
            return 0
        return int(time.monotonic() - self.started_at)

    def append_log(self, line: str) -> None:
        self.log_lines.append(line.rstrip())
        if len(self.log_lines) > MAX_SERVICE_LOG_LINES:
            self.log_lines = self.log_lines[-MAX_SERVICE_LOG_LINES:]


class ServiceManager:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.specs = service_specs(project_root)
        self.services = {
            spec.id: ManagedService(spec=spec) for spec in self.specs
        }
        self.output_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self.pending_restarts: set[str] = set()

    def start(self, service_id: str) -> None:
        service = self.services[service_id]
        if service.is_running:
            service.append_log(f"{service.spec.title} is already running.")
            return

        if not service.spec.cwd.exists():
            service.append_log(f"Working folder missing: {service.spec.cwd}")
            service.status_message = "Missing folder"
            service.last_exit_code = 1
            return

        command = list(service.spec.command)
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["NODE_PATH"] = env.get("NODE_PATH", "")
        env.update(dict(service.spec.env))
        command_dir = Path(command[0]).expanduser().parent
        if Path(command[0]).is_absolute() and command_dir.exists():
            env["PATH"] = f"{command_dir}{os.pathsep}{env.get('PATH', '')}"

        service.append_log("$ " + " ".join(command))
        try:
            process = subprocess.Popen(
                command,
                cwd=service.spec.cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=os.name == "posix",
            )
        except Exception as exc:
            service.append_log(f"Could not start service: {exc}")
            service.status_message = "Failed to start"
            service.last_exit_code = 1
            return

        reader = threading.Thread(
            target=self._read_process_output,
            args=(service.spec.id, process),
            daemon=True,
        )
        reader.start()
        service.process = process
        service.started_at = time.monotonic()
        service.reader = reader
        service.last_exit_code = None
        service.status_message = "Running"
        service.append_log(f"Started {service.spec.title}.")

    def stop(self, service_id: str) -> None:
        service = self.services[service_id]
        if not service.is_running or service.process is None:
            service.append_log(f"{service.spec.title} is not running.")
            return
        service.append_log(f"Stopping {service.spec.title}...")
        self._send_signal(service.process, signal.SIGINT)

    def restart(self, service_id: str) -> None:
        service = self.services[service_id]
        if service.is_running:
            self.pending_restarts.add(service_id)
            service.status_message = "Restarting"
            self.stop(service_id)
            return
        self.start(service_id)

    def clear_log(self, service_id: str) -> None:
        self.services[service_id].log_lines.clear()

    def reload_specs(self) -> None:
        updated = service_specs(self.project_root)
        updated_ids = {spec.id for spec in updated}
        for spec in updated:
            if spec.id in self.services:
                self.services[spec.id].spec = spec
            else:
                self.services[spec.id] = ManagedService(spec=spec)
        for service_id in list(self.services):
            if service_id not in updated_ids and not self.services[service_id].is_running:
                del self.services[service_id]
        self.specs = updated

    def add_whatsapp_account(self, profile_id: str, name: str) -> tuple[bool, str]:
        normalized = normalize_whatsapp_account_id(profile_id)
        if not normalized or normalized == "default":
            return False, "Use a unique account ID such as office-2."
        data = self.whatsapp_accounts()
        if any(str(item.get("id")) == normalized for item in data):
            return False, f"WhatsApp account {normalized!r} already exists."
        data.append(
            {
                "id": normalized,
                "name": name.strip() or normalized,
                "enabled": True,
                "auth_dir": f"auth_info_baileys_{normalized}",
            }
        )
        self.save_whatsapp_accounts(data)
        self.reload_specs()
        return True, f"Added WhatsApp account profile: {normalized}"

    def whatsapp_accounts_path(self) -> Path:
        return self.project_root.parent / "whatsappbot" / "accounts.json"

    def whatsapp_accounts(self) -> list[dict]:
        return load_whatsapp_accounts(self.whatsapp_accounts_path())

    def save_whatsapp_accounts(self, accounts: list[dict]) -> None:
        path = self.whatsapp_accounts_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"accounts": accounts}, indent=2, sort_keys=True), encoding="utf-8")

    def update_whatsapp_account(self, profile_id: str, *, name: str | None = None, enabled: bool | None = None) -> tuple[bool, str]:
        normalized = normalize_whatsapp_account_id(profile_id)
        accounts = self.whatsapp_accounts()
        for account in accounts:
            if str(account.get("id")) != normalized:
                continue
            if name is not None:
                account["name"] = name.strip() or normalized
            if enabled is not None:
                account["enabled"] = enabled
            self.save_whatsapp_accounts(accounts)
            self.reload_specs()
            action = "enabled" if enabled is True else "disabled" if enabled is False else "updated"
            return True, f"WhatsApp account {normalized!r} {action}."
        return False, f"WhatsApp account {normalized!r} was not found."

    def remove_whatsapp_account(self, profile_id: str) -> tuple[bool, str]:
        normalized = normalize_whatsapp_account_id(profile_id)
        if normalized == "default":
            return False, "Primary WhatsApp account cannot be removed; disable or reset login if needed."
        service_id = f"whatsapp_{normalized}"
        service = self.services.get(service_id)
        if service and service.is_running:
            return False, f"Stop WhatsApp account {normalized!r} before removing it."
        accounts = self.whatsapp_accounts()
        kept = [account for account in accounts if str(account.get("id")) != normalized]
        if len(kept) == len(accounts):
            return False, f"WhatsApp account {normalized!r} was not found."
        self.save_whatsapp_accounts(kept)
        self.reload_specs()
        return True, f"Removed WhatsApp account profile: {normalized}. Credentials folder was preserved."

    def drain_output(self) -> None:
        while True:
            try:
                service_id, line = self.output_queue.get_nowait()
            except queue.Empty:
                break
            self.services[service_id].append_log(line)

        for service in self.services.values():
            if service.process is None:
                continue
            exit_code = service.process.poll()
            if exit_code is None:
                continue
            if service.last_exit_code is None:
                service.last_exit_code = exit_code
                service.status_message = (
                    "Stopped" if exit_code == 0 else f"Exited ({exit_code})"
                )
                service.append_log(
                    f"{service.spec.title} exited with code {exit_code}."
                )
            service.process = None
            if service.spec.id in self.pending_restarts:
                self.pending_restarts.remove(service.spec.id)
                self.start(service.spec.id)

    def readiness(self, service_id: str) -> tuple[bool, str]:
        service = self.services.get(service_id)
        if service is None:
            return False, "service is not configured"

        if service_id == "nats":
            if tcp_open("127.0.0.1", 4222, timeout=0.3):
                return True, "reachable on 127.0.0.1:4222"
            return False, "not reachable on 127.0.0.1:4222"

        if service.spec.kind == "whatsapp":
            service_env = dict(service.spec.env)
            nats_url = service_env.get("NATS_URL") or os.environ.get("NATS_URL") or DEFAULT_NATS_URL
            subject = (
                service_env.get("NATS_HEALTH_SUBJECT")
                or os.environ.get("NATS_HEALTH_SUBJECT")
                or DEFAULT_WHATSAPP_HEALTH_SUBJECT
            )
            ready, detail = request_whatsapp_worker_health(nats_url, subject, timeout=1.0)
            if ready or service.is_running:
                return ready, detail
            return False, service.status_message or detail or "worker is not running"

        if service.is_running:
            return True, "process is running"
        return False, service.status_message or "service is not running"

    def wait_until_ready(
        self,
        service_id: str,
        *,
        timeout_seconds: float,
        poll_seconds: float = 0.5,
    ) -> tuple[bool, str]:
        deadline = time.monotonic() + timeout_seconds
        last_detail = ""
        while True:
            self.drain_output()
            ready, detail = self.readiness(service_id)
            last_detail = detail
            if ready:
                return True, detail

            service = self.services.get(service_id)
            if service is not None and service.process is not None:
                exit_code = service.process.poll()
                if exit_code is not None:
                    self.drain_output()
                    return False, f"process exited with code {exit_code}"

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False, last_detail or "service did not become ready"
            time.sleep(min(poll_seconds, remaining))

    def shutdown(self) -> None:
        for service in self.services.values():
            if service.is_running and service.process is not None:
                service.append_log("Console is closing; stopping service...")
                self._send_signal(service.process, signal.SIGINT)

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if not any(service.is_running for service in self.services.values()):
                return
            time.sleep(0.1)

        for service in self.services.values():
            if service.is_running and service.process is not None:
                self._send_signal(service.process, signal.SIGTERM)

    def _read_process_output(
        self, service_id: str, process: subprocess.Popen[str]
    ) -> None:
        assert process.stdout is not None
        for line in process.stdout:
            self.output_queue.put((service_id, line))

    def _send_signal(
        self,
        process: subprocess.Popen[str],
        sig: signal.Signals,
    ) -> None:
        if process.poll() is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, sig)
            else:
                process.send_signal(sig)
        except ProcessLookupError:
            return


def service_specs(project_root: Path) -> tuple[ServiceSpec, ...]:
    base_dir = project_root.parent
    nats_dir = base_dir / "nats-server"
    whatsapp_dir = base_dir / "whatsappbot"
    pocketbase_dir = base_dir / "antidengue-pocketbase"
    control_api_dir = base_dir / "automation-control-api"
    control_api_python = base_dir / "antidengue" / ".venv" / "bin" / "python"
    if not control_api_python.exists():
        control_api_python = Path("python3")
    node = find_node()

    specs: list[ServiceSpec] = [
        ServiceSpec(
            id="nats",
            title="NATS Server",
            summary="JetStream queue used by dispatch jobs and the WhatsApp worker.",
            command=("./nats-server", "-js"),
            cwd=nats_dir,
        ),
        ServiceSpec(
            id="antidengue_pocketbase",
            title="PocketBase: AntiDengue",
            summary="Local PocketBase database and admin dashboard for AntiDengue schools, officers, reports, and delivery audits.",
            command=("./pocketbase", "serve", "--http", "127.0.0.1:8090"),
            cwd=pocketbase_dir,
            kind="database",
        ),
        ServiceSpec(
            id="automation_control_api",
            title="Automation Control API",
            summary="Local FastAPI control agent for Windmill, Tailscale, and phone-based automation controls.",
            command=(
                str(control_api_python),
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                "8787",
            ),
            cwd=control_api_dir,
            kind="api",
        ),
    ]
    accounts = load_whatsapp_accounts(whatsapp_dir / "accounts.json")
    for account in accounts:
        if not bool(account.get("enabled", True)):
            continue
        profile_id = str(account.get("id") or "").strip()
        if not profile_id:
            continue
        is_default = profile_id == "default"
        env = {
            "WA_WORKER_ID": profile_id,
            "WA_AUTH_DIR": str(account.get("auth_dir") or f"auth_info_baileys_{profile_id}"),
            "WA_WORKER_LOCK_FILE": f"data/worker-{profile_id}.lock",
            "WA_QR_IMAGE_PATH": f"data/whatsapp-login-qr-{profile_id}.png",
        }
        if not is_default:
            env.update(
                {
                    "NATS_CONSUMER_NAME": f"whatsapp_worker_{profile_id}",
                    "NATS_STREAM": f"whatsapp_{profile_id}_stream".replace("-", "_"),
                    "NATS_SUBJECT": f"whatsapp.accounts.{profile_id}.pending",
                    "NATS_HEALTH_SUBJECT": f"whatsapp.worker.{profile_id}.health",
                    "NATS_GROUP_MEMBERS_SUBJECT": f"whatsapp.worker.{profile_id}.group-members",
                    "WA_GROUPS_FILE": f"data/discovered-groups-{profile_id}.csv",
                }
            )
        specs.append(
            ServiceSpec(
                id="whatsapp" if is_default else f"whatsapp_{profile_id}",
                title=f"WhatsApp: {account.get('name') or profile_id}",
                summary=f"Isolated WhatsApp worker profile ({profile_id}).",
                command=(node, "worker.js"),
                cwd=whatsapp_dir,
                env=tuple(sorted(env.items())),
                kind="whatsapp",
                profile_id=profile_id,
            )
        )
    return tuple(specs)


def load_whatsapp_accounts(path: Path) -> list[dict]:
    if not path.exists():
        return [{"id": "default", "name": "Primary WhatsApp", "enabled": True, "auth_dir": "auth_info_baileys"}]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        accounts = data.get("accounts", []) if isinstance(data, dict) else []
        return [item for item in accounts if isinstance(item, dict)]
    except (OSError, json.JSONDecodeError):
        return [{"id": "default", "name": "Primary WhatsApp", "enabled": True, "auth_dir": "auth_info_baileys"}]


def normalize_whatsapp_account_id(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", value.strip().lower()).strip("-")


def find_pnpm() -> str:
    pnpm = shutil.which("pnpm")
    if pnpm:
        return pnpm

    nvm_versions = Path.home() / ".nvm" / "versions" / "node"
    if nvm_versions.exists():
        candidates = sorted(nvm_versions.glob("*/bin/pnpm"), reverse=True)
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)

    return "pnpm"


def find_node() -> str:
    node = shutil.which("node")
    if node:
        return node

    nvm_versions = Path.home() / ".nvm" / "versions" / "node"
    if nvm_versions.exists():
        candidates = sorted(nvm_versions.glob("*/bin/node"), reverse=True)
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)

    return "node"


def tcp_open(host: str, port: int, *, timeout: float) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def request_whatsapp_worker_health(
    nats_url: str,
    subject: str,
    *,
    timeout: float,
) -> tuple[bool, str]:
    parsed = urlparse(nats_url or DEFAULT_NATS_URL)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 4222
    inbox = f"_INBOX.automation_gui.{uuid.uuid4().hex}"
    connect_payload = json.dumps(
        {
            "verbose": False,
            "pedantic": False,
            "headers": True,
            "no_responders": True,
            "lang": "automation-management-gui",
            "version": "1",
        },
        separators=(",", ":"),
    )

    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            stream = sock.makefile("rwb")
            stream.readline()
            stream.write(f"CONNECT {connect_payload}\r\nPING\r\n".encode("utf-8"))
            stream.flush()
            if not wait_for_nats_pong(stream):
                return False, "NATS did not respond to PING"

            stream.write(f"SUB {inbox} 1\r\n".encode("utf-8"))
            stream.write(f"PUB {subject} {inbox} 2\r\n{{}}\r\n".encode("utf-8"))
            stream.write(b"PING\r\n")
            stream.flush()

            while True:
                line = stream.readline()
                if not line:
                    return False, "NATS connection closed before health reply"
                if line.startswith(b"MSG "):
                    payload = read_nats_msg_payload(stream, line)
                    data = json.loads(payload.decode("utf-8") if payload else "{}")
                    if data.get("ready"):
                        return True, "worker answered health check"
                    return False, worker_health_detail(data)
                if line.startswith(b"HMSG "):
                    headers, payload = read_nats_hmsg_payload(stream, line)
                    first_header = headers.splitlines()[0] if headers.splitlines() else b""
                    if b" 503" in first_header:
                        return False, f"no responder on {subject}"
                    data = json.loads(payload.decode("utf-8") if payload else "{}")
                    if data.get("ready"):
                        return True, "worker answered health check"
                    return False, worker_health_detail(data)
                if line.startswith(b"-ERR"):
                    return False, line.decode("utf-8", errors="replace").strip()
    except socket.timeout:
        return False, f"timed out waiting for {subject}"
    except OSError as exc:
        return False, f"NATS connection failed: {exc}"
    except (ValueError, json.JSONDecodeError) as exc:
        return False, f"invalid worker health response: {exc}"


def worker_health_detail(data: dict) -> str:
    status = str(data.get("status") or "not_ready")
    parts = [f"worker responded but is not ready: {status}"]
    if "whatsappConnected" in data:
        parts.append(f"whatsappConnected={bool(data.get('whatsappConnected'))}")
    if "natsConsumerReady" in data:
        parts.append(f"natsConsumerReady={bool(data.get('natsConsumerReady'))}")
    if "hasActiveSocket" in data:
        parts.append(f"hasActiveSocket={bool(data.get('hasActiveSocket'))}")
    if "reconnectScheduled" in data:
        parts.append(f"reconnectScheduled={bool(data.get('reconnectScheduled'))}")
    if data.get("lastConnectionStatus"):
        parts.append(f"lastConnectionStatus={data.get('lastConnectionStatus')}")
    last_disconnect = data.get("lastDisconnect")
    if isinstance(last_disconnect, dict) and last_disconnect.get("error"):
        parts.append(f"lastDisconnect={last_disconnect.get('error')}")
    return ", ".join(parts)


def wait_for_nats_pong(stream) -> bool:
    while True:
        line = stream.readline()
        if not line:
            return False
        if line.startswith(b"PONG"):
            return True
        if line.startswith(b"-ERR"):
            return False


def read_nats_msg_payload(stream, header_line: bytes) -> bytes:
    parts = header_line.split()
    payload_size = int(parts[-1])
    payload = stream.read(payload_size)
    stream.read(2)
    return payload


def read_nats_hmsg_payload(stream, header_line: bytes) -> tuple[bytes, bytes]:
    parts = header_line.split()
    header_size = int(parts[-2])
    total_size = int(parts[-1])
    raw = stream.read(total_size)
    stream.read(2)
    return raw[:header_size], raw[header_size:]
