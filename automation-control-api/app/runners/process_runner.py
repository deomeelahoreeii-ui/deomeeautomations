from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class JobRecord:
    id: str
    project: str
    title: str
    status: str
    pid: int | None
    command: list[str]
    command_line: str
    cwd: str
    log_path: str
    state_path: str
    started_at: str
    finished_at: str = ""
    exit_code: int | None = None
    stop_requested: bool = False


@dataclass
class RunningJob:
    record: JobRecord
    process: subprocess.Popen[str]
    reader: threading.Thread


class ProcessRegistry:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.logs_dir = data_dir / "logs"
        self.state_dir = data_dir / "run-state"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._running: dict[str, RunningJob] = {}

    def start(
        self,
        *,
        project: str,
        title: str,
        command: list[str],
        cwd: Path,
        env: dict[str, str] | None = None,
    ) -> JobRecord:
        with self._lock:
            existing = self.active(project)
            if existing is not None:
                raise RuntimeError(f"{project} already has a running job: {existing.id}")

            job_id = f"{project}-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
            log_path = self.logs_dir / f"{job_id}.log"
            state_path = self.state_dir / f"{job_id}.json"
            process_env = os.environ.copy()
            process_env["PYTHONUNBUFFERED"] = "1"
            if env:
                process_env.update(env)

            record = JobRecord(
                id=job_id,
                project=project,
                title=title,
                status="running",
                pid=None,
                command=command,
                command_line=shlex.join(command),
                cwd=str(cwd),
                log_path=str(log_path),
                state_path=str(state_path),
                started_at=datetime.now().isoformat(timespec="seconds"),
            )
            self._write_log(log_path, "$ " + record.command_line + "\n")
            process = subprocess.Popen(
                command,
                cwd=str(cwd),
                env=process_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=os.name == "posix",
            )
            record.pid = process.pid
            self._save(record)
            reader = threading.Thread(
                target=self._read_output,
                args=(project, job_id, process, log_path),
                daemon=True,
            )
            running = RunningJob(record=record, process=process, reader=reader)
            self._running[project] = running
            reader.start()
            return record

    def active(self, project: str) -> JobRecord | None:
        running = self._running.get(project)
        if running is None:
            return None
        self._refresh(project, running)
        if running.record.status == "running":
            return running.record
        return None

    def latest(self, project: str) -> JobRecord | None:
        active = self.active(project)
        if active is not None:
            return active
        paths = sorted(
            self.state_dir.glob(f"{project}-*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for path in paths:
            try:
                return JobRecord(**json.loads(path.read_text(encoding="utf-8")))
            except (OSError, TypeError, json.JSONDecodeError):
                continue
        return None

    def stop(self, project: str) -> JobRecord:
        running = self._running.get(project)
        if running is None:
            latest = self.latest(project)
            if latest is None:
                raise RuntimeError(f"No {project} job has been started.")
            if latest.status != "running":
                raise RuntimeError(f"No running {project} job to stop.")
            return latest

        running.record.stop_requested = True
        self._save(running.record)
        process = running.process
        if process.poll() is None:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGINT)
            else:
                process.send_signal(signal.SIGINT)
        self._refresh(project, running)
        return running.record

    def tail(self, project: str, lines: int = 200) -> dict[str, object]:
        record = self.latest(project)
        if record is None:
            return {"project": project, "job": None, "lines": []}
        log_path = Path(record.log_path)
        return {
            "project": project,
            "job": asdict(record),
            "lines": self._tail_file(log_path, max(1, min(lines, 2000))),
        }

    def _refresh(self, project: str, running: RunningJob) -> None:
        exit_code = running.process.poll()
        if exit_code is None:
            return
        if running.record.status == "running":
            running.record.exit_code = exit_code
            running.record.status = (
                "stopped"
                if running.record.stop_requested
                else "completed"
                if exit_code == 0
                else "failed"
            )
            running.record.finished_at = datetime.now().isoformat(timespec="seconds")
            self._save(running.record)
        self._running.pop(project, None)

    def _read_output(
        self,
        project: str,
        job_id: str,
        process: subprocess.Popen[str],
        log_path: Path,
    ) -> None:
        assert process.stdout is not None
        with log_path.open("a", encoding="utf-8") as handle:
            for line in process.stdout:
                handle.write(line)
                handle.flush()
        time.sleep(0.1)
        running = self._running.get(project)
        if running and running.record.id == job_id:
            self._refresh(project, running)

    def _save(self, record: JobRecord) -> None:
        Path(record.state_path).write_text(
            json.dumps(asdict(record), indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _write_log(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(text)

    @staticmethod
    def _tail_file(path: Path, lines: int) -> list[str]:
        if not path.is_file():
            return []
        data = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return data[-lines:]
