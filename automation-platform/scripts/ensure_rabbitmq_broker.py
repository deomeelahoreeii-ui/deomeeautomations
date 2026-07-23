#!/usr/bin/env python3
"""Validate and reconcile the Celery broker used by local development.

The local RabbitMQ container is durable, so changing Compose defaults does not
change users already stored in its volume.  This preflight aligns the durable
RabbitMQ user with the effective Celery URL and verifies a real AMQP login
before API, scheduler, recovery, or worker processes are started.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit

DEFAULT_BROKER_URL = "amqp://automation:automation-local-only@localhost:5672//"
DEFAULT_RABBITMQ_USER = "automation"
DEFAULT_RABBITMQ_PASSWORD = "automation-local-only"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
AMQP_SCHEMES = {"amqp", "pyamqp"}
_ENV_LINE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")


class BrokerPreflightError(RuntimeError):
    """Raised when the broker cannot be made safe for startup."""


@dataclass(frozen=True)
class BrokerPlan:
    original_url: str
    effective_url: str
    scheme: str
    host: str | None
    port: int | None
    username: str | None
    password: str | None
    local_amqp: bool
    migrated_guest: bool


def _strip_env_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _ENV_LINE.match(raw_line)
        if match:
            values[match.group(1)] = _strip_env_value(match.group(2))
    return values


def _effective_value(name: str, env_file_values: dict[str, str], default: str = "") -> str:
    return os.environ.get(name) or env_file_values.get(name) or default


def _netloc(username: str | None, password: str | None, host: str, port: int | None) -> str:
    credentials = ""
    if username is not None:
        credentials = quote(username, safe="")
        if password is not None:
            credentials += f":{quote(password, safe='')}"
        credentials += "@"
    rendered_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return f"{credentials}{rendered_host}{f':{port}' if port else ''}"


def build_broker_plan(
    broker_url: str,
    *,
    rabbitmq_user: str = DEFAULT_RABBITMQ_USER,
    rabbitmq_password: str = DEFAULT_RABBITMQ_PASSWORD,
) -> BrokerPlan:
    raw_url = broker_url.strip() or DEFAULT_BROKER_URL
    parsed = urlsplit(raw_url)
    scheme = parsed.scheme.lower()
    host = parsed.hostname.lower() if parsed.hostname else None
    username = unquote(parsed.username) if parsed.username is not None else None
    password = unquote(parsed.password) if parsed.password is not None else None
    local_amqp = scheme in AMQP_SCHEMES and host in LOCAL_HOSTS
    migrated_guest = False
    effective_url = raw_url

    if local_amqp and (not username or username == "guest"):
        if not rabbitmq_user or rabbitmq_user == "guest":
            raise BrokerPreflightError(
                "Local Docker RabbitMQ cannot use the guest account through the published host port. "
                "Set RABBITMQ_USER to a non-guest application account."
            )
        if not rabbitmq_password:
            raise BrokerPreflightError("RABBITMQ_PASSWORD cannot be empty for local development.")
        username = rabbitmq_user
        password = rabbitmq_password
        effective_url = urlunsplit(
            (
                parsed.scheme or "amqp",
                _netloc(username, password, parsed.hostname or "localhost", parsed.port or 5672),
                parsed.path or "//",
                parsed.query,
                parsed.fragment,
            )
        )
        migrated_guest = True

    if local_amqp and (not username or password is None or password == ""):
        raise BrokerPreflightError(
            "The local AMQP broker URL must include a non-empty username and password."
        )

    return BrokerPlan(
        original_url=raw_url,
        effective_url=effective_url,
        scheme=scheme,
        host=host,
        port=parsed.port,
        username=username,
        password=password,
        local_amqp=local_amqp,
        migrated_guest=migrated_guest,
    )


def redact_broker_url(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.password is None:
        return url
    username = unquote(parsed.username or "")
    host = parsed.hostname or ""
    return urlunsplit(
        (
            parsed.scheme,
            _netloc(username, "<redacted>", host, parsed.port),
            parsed.path,
            parsed.query,
            parsed.fragment,
        )
    )


def write_env_value(path: Path, key: str, value: str) -> bool:
    """Atomically replace the last active key assignment, preserving all other text."""
    original = path.read_text(encoding="utf-8") if path.exists() else ""
    lines = original.splitlines(keepends=True)
    matching: list[int] = []
    for index, line in enumerate(lines):
        match = _ENV_LINE.match(line.rstrip("\r\n"))
        if match and match.group(1) == key:
            matching.append(index)
    rendered = f"{key}={value}\n"
    if matching:
        newline = "\r\n" if lines[matching[-1]].endswith("\r\n") else "\n"
        lines[matching[-1]] = rendered.rstrip("\n") + newline
    else:
        if lines and not lines[-1].endswith(("\n", "\r")):
            lines[-1] += "\n"
        lines.append(rendered)
    updated = "".join(lines)
    if updated == original:
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        backup = path.with_name(f"{path.name}.rabbitmq-preflight.bak")
        if not backup.exists():
            shutil.copy2(path, backup)
            backup.chmod(0o600)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(updated)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_name, path.stat().st_mode & 0o777 if path.exists() else 0o600)
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
    return True


def _compose_command(compose_file: Path, *args: str) -> list[str]:
    return ["docker", "compose", "-f", str(compose_file), "exec", "-T", "rabbitmq", *args]


def _run(command: list[str], *, allow_failure: bool = False) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode and not allow_failure:
        detail = (completed.stderr or completed.stdout or "unknown error").strip()
        raise BrokerPreflightError(f"Command failed: {' '.join(command[:6])}: {detail}")
    return completed


def ensure_local_rabbitmq_user(compose_file: Path, username: str, password: str) -> None:
    listed = _run(_compose_command(compose_file, "rabbitmqctl", "list_users", "-q"))
    users = {line.split("\t", 1)[0].strip() for line in listed.stdout.splitlines() if line.strip()}
    action = "change_password" if username in users else "add_user"
    _run(_compose_command(compose_file, "rabbitmqctl", action, username, password))
    _run(
        _compose_command(
            compose_file,
            "rabbitmqctl",
            "set_permissions",
            "-p",
            "/",
            username,
            ".*",
            ".*",
            ".*",
        )
    )


def verify_broker_login(url: str) -> None:
    try:
        from kombu import Connection

        connection = Connection(url, connect_timeout=5)
        try:
            connection.connect()
        finally:
            connection.release()
    except Exception as exc:  # pragma: no cover - concrete transport errors vary by Kombu version
        raise BrokerPreflightError(
            f"Celery broker authentication failed for {redact_broker_url(url)}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def run_preflight(
    *,
    compose_file: Path,
    env_file: Path,
    emit_url: Path | None = None,
    rewrite_only: bool = False,
) -> BrokerPlan:
    env_values = read_env_file(env_file)
    plan = build_broker_plan(
        _effective_value("CELERY_BROKER_URL", env_values, DEFAULT_BROKER_URL),
        rabbitmq_user=_effective_value("RABBITMQ_USER", env_values, DEFAULT_RABBITMQ_USER),
        rabbitmq_password=_effective_value(
            "RABBITMQ_PASSWORD", env_values, DEFAULT_RABBITMQ_PASSWORD
        ),
    )

    if plan.migrated_guest:
        write_env_value(env_file, "CELERY_BROKER_URL", plan.effective_url)

    if not rewrite_only and plan.scheme in AMQP_SCHEMES:
        if plan.local_amqp:
            assert plan.username is not None and plan.password is not None
            ensure_local_rabbitmq_user(compose_file, plan.username, plan.password)
        verify_broker_login(plan.effective_url)

    if emit_url is not None:
        emit_url.parent.mkdir(parents=True, exist_ok=True)
        emit_url.write_text(plan.effective_url, encoding="utf-8")
        emit_url.chmod(0o600)
    return plan


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile and verify the Celery RabbitMQ broker")
    parser.add_argument("--compose-file", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--emit-url", type=Path)
    parser.add_argument(
        "--rewrite-only",
        action="store_true",
        help="Only migrate an unsafe local guest URL; do not contact Docker or the broker.",
    )
    args = parser.parse_args(argv)
    try:
        plan = run_preflight(
            compose_file=args.compose_file.resolve(),
            env_file=args.env_file.resolve(),
            emit_url=args.emit_url.resolve() if args.emit_url else None,
            rewrite_only=args.rewrite_only,
        )
    except BrokerPreflightError as exc:
        print(f"RabbitMQ/Celery preflight failed: {exc}", file=sys.stderr)
        return 1

    if plan.migrated_guest:
        print(
            "Migrated the unsafe local guest Celery URL to the durable RabbitMQ application account."
        )
    if args.rewrite_only or plan.scheme not in AMQP_SCHEMES:
        print(f"Celery broker configuration ready: {redact_broker_url(plan.effective_url)}")
    else:
        print(f"Celery broker login verified: {redact_broker_url(plan.effective_url)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
