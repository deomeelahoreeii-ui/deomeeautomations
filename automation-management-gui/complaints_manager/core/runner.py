from __future__ import annotations

import os
import queue
import shlex
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from .commands import CommandSpec


MAX_LOG_LINES = 3_000


@dataclass
class RunningCommand:
    spec: CommandSpec
    process: subprocess.Popen[str]
    started_at: float
    reader: threading.Thread
    command_line: str


@dataclass
class CommandRunner:
    project_root: Path
    log_lines: list[str] = field(default_factory=list)
    output_queue: queue.Queue[str] = field(default_factory=queue.Queue)
    running: RunningCommand | None = None
    last_exit_code: int | None = None
    last_command_title: str = ""
    status_message: str = "Ready"

    @property
    def is_running(self) -> bool:
        return self.running is not None and self.running.process.poll() is None

    def start(self, spec: CommandSpec, values: dict[str, str]) -> None:
        if self.is_running:
            return

        command = spec.build(self.project_root, values)
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env.update(spec.build_env(values))
        if command and Path(command[0]).name == "uv":
            env.pop("VIRTUAL_ENV", None)

        working_dir = spec.working_dir(self.project_root)
        command_line = shlex.join(command)
        self.append_log("$ " + command_line)
        process = subprocess.Popen(
            command,
            cwd=working_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=os.name == "posix",
        )
        reader = threading.Thread(
            target=self._read_process_output,
            args=(process,),
            daemon=True,
        )
        reader.start()
        self.running = RunningCommand(
            spec=spec,
            process=process,
            started_at=time.monotonic(),
            reader=reader,
            command_line=command_line,
        )
        self.last_exit_code = None
        self.last_command_title = spec.title
        self.status_message = f"Running: {spec.title}"

    def stop(self) -> None:
        if not self.is_running or self.running is None:
            return
        self.append_log(f"Stopping: {self.running.spec.title}")
        self._send_signal(self.running.process, signal.SIGINT)

    def shutdown(self) -> None:
        if not self.is_running or self.running is None:
            return
        self.append_log("Console is closing; stopping active command...")
        process = self.running.process
        self._send_signal(process, signal.SIGINT)
        try:
            process.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            pass

        self._send_signal(process, signal.SIGTERM)
        try:
            process.wait(timeout=3)
            return
        except subprocess.TimeoutExpired:
            pass

        self._send_signal(process, signal.SIGKILL)
        process.wait(timeout=3)

    def drain_output(self) -> None:
        while True:
            try:
                self.append_log(self.output_queue.get_nowait())
            except queue.Empty:
                break

        if self.running is not None:
            exit_code = self.running.process.poll()
            if exit_code is not None:
                self.last_exit_code = exit_code
                self.status_message = (
                    f"Finished: {self.running.spec.title}"
                    if exit_code == 0
                    else f"Failed: {self.running.spec.title} ({exit_code})"
                )
                self.running = None

    def append_log(self, line: str) -> None:
        self.log_lines.append(line.rstrip())
        if len(self.log_lines) > MAX_LOG_LINES:
            self.log_lines = self.log_lines[-MAX_LOG_LINES:]

    def clear_log(self) -> None:
        self.log_lines.clear()

    def elapsed_seconds(self) -> int:
        if self.running is None:
            return 0
        return int(time.monotonic() - self.running.started_at)

    def _read_process_output(self, process: subprocess.Popen[str]) -> None:
        assert process.stdout is not None
        for line in process.stdout:
            self.output_queue.put(line)

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
