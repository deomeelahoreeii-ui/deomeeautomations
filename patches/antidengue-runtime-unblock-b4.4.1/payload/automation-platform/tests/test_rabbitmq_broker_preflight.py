from __future__ import annotations

from pathlib import Path

import pytest

import scripts.ensure_rabbitmq_broker as broker_preflight
from scripts.ensure_rabbitmq_broker import (
    BrokerPreflightError,
    build_broker_plan,
    read_env_file,
    redact_broker_url,
    run_preflight,
    write_env_value,
)


def test_local_guest_url_is_migrated_to_application_credentials() -> None:
    plan = build_broker_plan(
        "amqp://guest:guest@localhost:5672//",
        rabbitmq_user="automation",
        rabbitmq_password="local secret",
    )

    assert plan.local_amqp is True
    assert plan.migrated_guest is True
    assert plan.username == "automation"
    assert plan.password == "local secret"
    assert plan.effective_url == "amqp://automation:local%20secret@localhost:5672//"


def test_remote_guest_url_is_not_silently_rewritten() -> None:
    plan = build_broker_plan("amqp://guest:guest@broker.example.test:5672/vhost")

    assert plan.local_amqp is False
    assert plan.migrated_guest is False
    assert plan.effective_url == plan.original_url


def test_local_guest_application_account_is_rejected() -> None:
    with pytest.raises(BrokerPreflightError, match="non-guest application account"):
        build_broker_plan(
            "amqp://guest:guest@127.0.0.1:5672//",
            rabbitmq_user="guest",
            rabbitmq_password="guest",
        )


def test_write_env_value_preserves_other_lines_and_updates_last_assignment(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comment\nCELERY_BROKER_URL=old\nOTHER=value\nCELERY_BROKER_URL=effective\n",
        encoding="utf-8",
    )

    changed = write_env_value(env_file, "CELERY_BROKER_URL", "amqp://new:new@localhost:5672//")

    assert changed is True
    assert env_file.read_text(encoding="utf-8") == (
        "# comment\nCELERY_BROKER_URL=old\nOTHER=value\n"
        "CELERY_BROKER_URL=amqp://new:new@localhost:5672//\n"
    )
    assert (tmp_path / ".env.rabbitmq-preflight.bak").is_file()
    assert read_env_file(env_file)["CELERY_BROKER_URL"].startswith("amqp://new")


def test_rewrite_only_migrates_env_and_emits_effective_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "RABBITMQ_USER=automation\n"
        "RABBITMQ_PASSWORD=automation-local-only\n"
        "CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//\n",
        encoding="utf-8",
    )
    emitted = tmp_path / "broker-url"
    monkeypatch.delenv("CELERY_BROKER_URL", raising=False)
    monkeypatch.delenv("RABBITMQ_USER", raising=False)
    monkeypatch.delenv("RABBITMQ_PASSWORD", raising=False)

    plan = run_preflight(
        compose_file=tmp_path / "compose.yaml",
        env_file=env_file,
        emit_url=emitted,
        rewrite_only=True,
    )

    assert plan.migrated_guest is True
    assert emitted.read_text(encoding="utf-8") == (
        "amqp://automation:automation-local-only@localhost:5672//"
    )
    assert read_env_file(env_file)["CELERY_BROKER_URL"] == emitted.read_text(encoding="utf-8")


def test_environment_override_is_used_and_emitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("CELERY_BROKER_URL=memory://\n", encoding="utf-8")
    emitted = tmp_path / "broker-url"
    monkeypatch.setenv("CELERY_BROKER_URL", "filesystem://")

    plan = run_preflight(
        compose_file=tmp_path / "compose.yaml",
        env_file=env_file,
        emit_url=emitted,
        rewrite_only=True,
    )

    assert plan.effective_url == "filesystem://"
    assert emitted.read_text(encoding="utf-8") == "filesystem://"


def test_redaction_never_exposes_password() -> None:
    redacted = redact_broker_url("amqp://user:p%40ss@localhost:5672//")

    assert "p%40ss" not in redacted
    assert "<redacted>" in redacted or "%3Credacted%3E" in redacted



def test_local_amqp_preflight_reconciles_user_then_verifies_login(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "CELERY_BROKER_URL=amqp://automation:secret@localhost:5672//\n",
        encoding="utf-8",
    )
    calls: list[tuple] = []
    monkeypatch.delenv("CELERY_BROKER_URL", raising=False)
    monkeypatch.setattr(
        broker_preflight,
        "ensure_local_rabbitmq_user",
        lambda compose, user, password: calls.append(("ensure", compose, user, password)),
    )
    monkeypatch.setattr(
        broker_preflight,
        "verify_broker_login",
        lambda url: calls.append(("verify", url)),
    )

    run_preflight(
        compose_file=tmp_path / "compose.yaml",
        env_file=env_file,
        rewrite_only=False,
    )

    assert calls == [
        ("ensure", tmp_path / "compose.yaml", "automation", "secret"),
        ("verify", "amqp://automation:secret@localhost:5672//"),
    ]


def test_defaults_are_consistent_across_settings_and_env_example() -> None:
    root = Path(__file__).resolve().parents[1]
    config_text = (
        root / "packages" / "automation_core" / "automation_core" / "config.py"
    ).read_text(encoding="utf-8")
    env_text = (root / ".env.example").read_text(encoding="utf-8")

    expected = "amqp://automation:automation-local-only@localhost:5672//"
    assert f'celery_broker_url: str = "{expected}"' in config_text
    assert f"CELERY_BROKER_URL={expected}" in env_text
    assert "amqp://guest:guest@localhost:5672//" not in config_text

def test_dev_script_runs_broker_preflight_before_recovery() -> None:
    root = Path(__file__).resolve().parents[1]
    text = (root / "scripts" / "dev.sh").read_text(encoding="utf-8")

    preflight = text.index("ensure_rabbitmq_broker.py")
    recovery = text.index('info "Recovering jobs left active by an earlier development process"')
    worker = text.index('info "Starting auto-reloading Celery worker')
    assert preflight < recovery < worker
