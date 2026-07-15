from __future__ import annotations

import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from sqlmodel import Session

from automation_core.database import engine
from automation_core.job_service import append_log, record_artifact


@dataclass(frozen=True)
class CommandRunResult:
    return_code: int
    artifact_count: int


def artifact_kind(path: Path, *, output_dir: Path) -> str:
    """Classify pipeline output by purpose rather than only by extension."""
    relative_path = path.relative_to(output_dir)
    normalized_path = relative_path.as_posix().lower()
    filename = path.name.lower()

    if "group-route-excels/" in normalized_path:
        return "dispatch_draft"
    if filename == "run_summary.json":
        return "manifest"
    if "audit" in filename:
        return "audit"
    if "activity evidence" in filename:
        return "evidence"
    if path.suffix.lower() in {".xls", ".xlsx", ".csv"}:
        return "report"
    return path.suffix.lower().removeprefix(".") or "file"


def append_job_log(job_id: uuid.UUID | str, message: str, *, level: str = "info") -> None:
    with Session(engine) as session:
        append_log(session, job_id, message, level=level)


def discover_artifacts(
    job_id: uuid.UUID | str,
    output_dir: Path,
    *,
    modified_after: float,
) -> int:
    if not output_dir.exists():
        return 0
    count = 0
    with Session(engine) as session:
        for path in sorted(output_dir.rglob("*")):
            if not path.is_file():
                continue
            if path.stat().st_mtime < modified_after:
                continue
            record_artifact(
                session,
                job_id,
                path.resolve(),
                kind=artifact_kind(path, output_dir=output_dir),
                name=str(path.relative_to(output_dir)),
            )
            count += 1
    return count


def run_streamed_command(
    job_id: uuid.UUID | str,
    command: list[str],
    *,
    cwd: Path,
    output_dir: Path,
    additional_output_dirs: tuple[Path, ...] = (),
    env: dict[str, str] | None = None,
) -> CommandRunResult:
    started_mtime = time.time()
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    append_job_log(job_id, f"$ {' '.join(command)}")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=merged_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None
    for line in process.stdout:
        append_job_log(job_id, line.rstrip())

    return_code = process.wait()
    artifact_count = discover_artifacts(
        job_id,
        output_dir,
        modified_after=started_mtime,
    )
    for extra_output_dir in additional_output_dirs:
        artifact_count += discover_artifacts(
            job_id,
            extra_output_dir,
            modified_after=started_mtime,
        )
    return CommandRunResult(return_code=return_code, artifact_count=artifact_count)
