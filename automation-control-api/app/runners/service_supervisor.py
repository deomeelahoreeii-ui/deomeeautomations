from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlparse

from app.config import settings

DEFAULT_NATS_URL = "nats://localhost:4222"
DEFAULT_WHATSAPP_HEALTH_SUBJECT = "whatsapp.worker.health"


@dataclass
class ServiceHealth:
    id: str
    title: str
    healthy: bool
    running: bool
    detail: str
    pid: int | None = None
    log_path: str = ""
    started_by_api: bool = False


class ServiceSupervisor:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.logs_dir = data_dir / "service-logs"
        self.pid_dir = data_dir / "service-pids"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.pid_dir.mkdir(parents=True, exist_ok=True)

    def status(self) -> dict[str, dict[str, object]]:
        nats = self.nats_status()
        whatsapp = self.whatsapp_status()
        return {
            nats.id: asdict(nats),
            whatsapp.id: asdict(whatsapp),
        }

    def ensure_antidengue_dependencies(self, *, require_whatsapp: bool) -> dict[str, dict[str, object]]:
        nats = self.ensure_nats()
        if not nats.healthy:
            raise RuntimeError(f"NATS is not healthy: {nats.detail}")

        whatsapp = self.whatsapp_status()
        if require_whatsapp:
            whatsapp = self.ensure_primary_whatsapp()
            if not whatsapp.healthy:
                raise RuntimeError(f"Primary WhatsApp worker is not healthy: {whatsapp.detail}")

        return {
            nats.id: asdict(nats),
            whatsapp.id: asdict(whatsapp),
        }

    def ensure_nats(self) -> ServiceHealth:
        current = self.nats_status()
        if current.healthy:
            return current

        executable = settings.nats_dir / "nats-server"
        log_path = self.logs_dir / "nats.log"
        if not executable.is_file():
            return ServiceHealth(
                id="nats",
                title="NATS Server",
                healthy=False,
                running=False,
                detail=f"NATS binary not found: {executable}",
                log_path=str(log_path),
            )

        process = self._start_background(
            [str(executable), "-js"],
            cwd=settings.nats_dir,
            log_path=log_path,
            pid_path=self.pid_dir / "nats.pid",
        )

        for _ in range(30):
            time.sleep(0.25)
            current = self.nats_status()
            if current.healthy:
                current.started_by_api = True
                current.pid = current.pid or process.pid
                return current

        return ServiceHealth(
            id="nats",
            title="NATS Server",
            healthy=False,
            running=process.poll() is None,
            detail="NATS did not become reachable on 127.0.0.1:4222.",
            pid=process.pid,
            log_path=str(log_path),
            started_by_api=True,
        )

    def ensure_primary_whatsapp(self) -> ServiceHealth:
        current = self.whatsapp_status()
        if current.healthy:
            return current

        process: subprocess.Popen[str] | None = None
        log_path = self.logs_dir / "whatsapp-primary.log"

        if not current.running:
            worker = settings.whatsappbot_dir / "worker.js"
            if not worker.is_file():
                return ServiceHealth(
                    id="whatsapp_primary",
                    title="Primary WhatsApp Worker",
                    healthy=False,
                    running=False,
                    detail=f"WhatsApp worker script not found: {worker}",
                    log_path=str(log_path),
                )

            node = self._find_node()
            env = os.environ.copy()
            env.update(
                {
                    "WA_WORKER_ID": "default",
                    "WA_AUTH_DIR": "auth_info_baileys",
                    "WA_WORKER_LOCK_FILE": "data/worker-default.lock",
                    "WA_QR_IMAGE_PATH": "data/whatsapp-login-qr-default.png",
                }
            )
            process = self._start_background(
                [node, "worker.js"],
                cwd=settings.whatsappbot_dir,
                log_path=log_path,
                pid_path=self.pid_dir / "whatsapp-primary.pid",
                env=env,
            )

        last_status = current
        for _ in range(240):
            time.sleep(0.5)
            current = self.whatsapp_status()
            last_status = current
            if current.healthy:
                current.started_by_api = True
                if process is not None:
                    current.pid = current.pid or process.pid
                return current
            if process is not None and process.poll() is not None:
                break

        return ServiceHealth(
            id="whatsapp_primary",
            title="Primary WhatsApp Worker",
            healthy=False,
            running=(process.poll() is None) if process is not None else last_status.running,
            detail=f"Primary WhatsApp worker did not become healthy: {last_status.detail}",
            pid=process.pid if process is not None else last_status.pid,
            log_path=str(log_path),
            started_by_api=process is not None or last_status.started_by_api,
        )

    def nats_status(self) -> ServiceHealth:
        pid = self._read_pid(self.pid_dir / "nats.pid")
        reachable = self._tcp_open("127.0.0.1", 4222, timeout=0.25)
        process_running = bool(pid and self._pid_alive(pid))
        return ServiceHealth(
            id="nats",
            title="NATS Server",
            healthy=reachable,
            running=reachable or process_running,
            detail="reachable on 127.0.0.1:4222" if reachable else "not reachable on 127.0.0.1:4222",
            pid=pid,
            log_path=str(self.logs_dir / "nats.log"),
            started_by_api=bool(pid),
        )

    def whatsapp_status(self) -> ServiceHealth:
        pid = self._read_pid(self.pid_dir / "whatsapp-primary.pid")
        managed_running = bool(pid and self._pid_alive(pid))
        existing_pid = self._find_existing_whatsapp_pid()
        health_ready, health_detail = request_whatsapp_worker_health(
            os.environ.get("NATS_URL") or DEFAULT_NATS_URL,
            os.environ.get("NATS_HEALTH_SUBJECT") or DEFAULT_WHATSAPP_HEALTH_SUBJECT,
            timeout=0.5,
        )
        if health_ready:
            return ServiceHealth(
                id="whatsapp_primary",
                title="Primary WhatsApp Worker",
                healthy=True,
                running=True,
                detail=health_detail,
                pid=pid or existing_pid,
                log_path=str(self.logs_dir / "whatsapp-primary.log"),
                started_by_api=bool(pid),
            )

        if managed_running or existing_pid:
            return ServiceHealth(
                id="whatsapp_primary",
                title="Primary WhatsApp Worker",
                healthy=False,
                running=True,
                detail=f"process is running but health check failed: {health_detail}",
                pid=pid or existing_pid,
                log_path=str(self.logs_dir / "whatsapp-primary.log"),
                started_by_api=managed_running,
            )

        return ServiceHealth(
            id="whatsapp_primary",
            title="Primary WhatsApp Worker",
            healthy=False,
            running=False,
            detail=f"primary worker process is not running; {health_detail}",
            log_path=str(self.logs_dir / "whatsapp-primary.log"),
        )

    def _start_background(
        self,
        command: list[str],
        *,
        cwd: Path,
        log_path: Path,
        pid_path: Path,
        env: dict[str, str] | None = None,
    ) -> subprocess.Popen[str]:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        pid_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"\n$ {' '.join(command)}\n")
            handle.flush()
            process = subprocess.Popen(
                command,
                cwd=str(cwd),
                env=env or os.environ.copy(),
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=os.name == "posix",
            )
        pid_path.write_text(str(process.pid), encoding="utf-8")
        return process

    @staticmethod
    def _tcp_open(host: str, port: int, *, timeout: float) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    @staticmethod
    def _read_pid(path: Path) -> int | None:
        try:
            value = int(path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return None
        return value if value > 0 else None

    @staticmethod
    def _find_node() -> str:
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

    def _find_existing_whatsapp_pid(self) -> int | None:
        try:
            output = subprocess.check_output(
                ["pgrep", "-af", "worker.js"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except (OSError, subprocess.CalledProcessError):
            return None
        for line in output.splitlines():
            parts = line.strip().split(maxsplit=1)
            if len(parts) != 2:
                continue
            try:
                pid = int(parts[0])
            except ValueError:
                continue
            if pid != os.getpid():
                command = parts[1]
                if "node" not in command or "worker.js" not in command:
                    continue
                if self._process_cwd_matches(pid, settings.whatsappbot_dir):
                    return pid
                worker_path = str(settings.whatsappbot_dir / "worker.js")
                if worker_path in command:
                    return pid
        return None

    @staticmethod
    def _process_cwd_matches(pid: int, expected_cwd: Path) -> bool:
        proc_cwd = Path("/proc") / str(pid) / "cwd"
        try:
            return proc_cwd.resolve() == expected_cwd.resolve()
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
    inbox = f"_INBOX.automation_control.{uuid.uuid4().hex}"
    connect_payload = json.dumps(
        {
            "verbose": False,
            "pedantic": False,
            "headers": True,
            "no_responders": True,
            "lang": "automation-control-api",
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
