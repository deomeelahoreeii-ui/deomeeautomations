from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path

import pytest

from automation_core.nats_health import check_nats, parse_args, wait_for_nats


ROOT = Path(__file__).resolve().parents[1]
DEV_SCRIPT = ROOT / "scripts" / "dev.sh"
COMPOSE_FILE = ROOT / "compose.yaml"


class FakeJetStream:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def account_info(self) -> None:
        self.calls.append("account_info")


class FakeClient:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def jetstream(self) -> FakeJetStream:
        self.calls.append("jetstream")
        return FakeJetStream(self.calls)

    async def close(self) -> None:
        self.calls.append("close")


def test_nats_probe_requires_jetstream_and_closes_client(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_connect(url: str, **kwargs) -> FakeClient:
        assert url == "nats://127.0.0.1:4222"
        assert kwargs == {"connect_timeout": 1, "max_reconnect_attempts": 0}
        calls.append("connect")
        return FakeClient(calls)

    monkeypatch.setattr("automation_core.nats_health.nats.connect", fake_connect)

    asyncio.run(check_nats("nats://127.0.0.1:4222"))

    assert calls == ["connect", "jetstream", "account_info", "close"]


def test_nats_probe_retries_until_jetstream_is_ready(monkeypatch) -> None:
    attempts = 0

    async def flaky_check(_url: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("not ready")

    monkeypatch.setattr("automation_core.nats_health.check_nats", flaky_check)

    error = asyncio.run(wait_for_nats("nats://example:4222", 3, 0))

    assert error is None
    assert attempts == 3


def test_nats_probe_returns_last_readiness_error(monkeypatch) -> None:
    errors = iter([RuntimeError("first"), RuntimeError("last")])

    async def failed_check(_url: str) -> None:
        raise next(errors)

    monkeypatch.setattr("automation_core.nats_health.check_nats", failed_check)

    error = asyncio.run(wait_for_nats("nats://example:4222", 2, 0))

    assert isinstance(error, RuntimeError)
    assert str(error) == "last"


@pytest.mark.parametrize("arguments", [["--attempts", "0"], ["--delay", "-1"]])
def test_nats_probe_rejects_invalid_retry_configuration(arguments) -> None:
    with pytest.raises(SystemExit, match="2"):
        parse_args(arguments)


def test_compose_defines_persistent_healthy_jetstream_service() -> None:
    result = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "config", "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    config = json.loads(result.stdout)
    nats_service = config["services"]["nats"]

    assert nats_service["image"] == "nats:2.12.11-alpine"
    assert "-js" in nats_service["command"]
    assert nats_service["ports"][0]["published"] == "4222"
    assert nats_service["ports"][0]["host_ip"] == "127.0.0.1"
    assert any(mount["target"] == "/data" for mount in nats_service["volumes"])
    assert "js-enabled-only=true" in nats_service["healthcheck"]["test"][-1]
    assert "nats-data" in config["volumes"]


def test_dev_script_starts_nats_and_uses_jetstream_readiness_probe() -> None:
    source = DEV_SCRIPT.read_text(encoding="utf-8")

    start = source.index('info "Checking NATS with JetStream"')
    stale_workers = source.index(
        'info "Stopping stale local platform workers before durable-task recovery"'
    )
    nats_block = source[start:stale_workers]

    assert 'scripts/check_nats.py --quiet' in nats_block
    assert 'docker compose -f "$COMPOSE_FILE" up -d nats' in nats_block
    assert 'scripts/check_nats.py --attempts 30 --delay 1' in nats_block
    assert 'docker compose -f "$COMPOSE_FILE" logs --tail=80 nats' in nats_block
    assert "wait_for_port 4222" not in source
