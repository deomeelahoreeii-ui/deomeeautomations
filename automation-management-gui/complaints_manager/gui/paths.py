from __future__ import annotations

from pathlib import Path
from typing import Any

from complaints_manager.core.commands import CommandSpec
from complaints_manager.gui.state import GuiState


def resolve_gui_path(project_root: Path, value: str) -> Path:
    path = Path((value or ".").strip()).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def existing_initial_dir(project_root: Path, value: str) -> Path:
    path = resolve_gui_path(project_root, value)
    if path.is_file():
        path = path.parent
    if path.exists() and path.is_dir():
        return path

    parent = path.parent
    while parent != parent.parent and not parent.exists():
        parent = parent.parent
    if parent.exists() and parent.is_dir():
        return parent
    return project_root.resolve(strict=False)


def display_path_value(project_root: Path, selected_path: Path) -> str:
    resolved_path = selected_path.expanduser().resolve(strict=False)
    resolved_root = project_root.resolve(strict=False)
    try:
        return str(resolved_path.relative_to(resolved_root))
    except ValueError:
        return str(resolved_path)


def apply_selected_folder(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    key: str,
    label: str,
    selected: str | Path,
) -> None:
    selected_path = Path(selected).expanduser()
    values[key] = display_path_value(state.command_root(command), selected_path)
    state.propagate_env_value(key, values[key])
    state.persist_command_values(command)
    state.runner.append_log(
        f"Selected {label}: {selected_path.resolve(strict=False)}"
    )


def apply_selected_file(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    key: str,
    label: str,
    selected: str | Path,
) -> None:
    selected_path = Path(selected).expanduser()
    values[key] = display_path_value(state.command_root(command), selected_path)
    state.persist_command_values(command)
    state.runner.append_log(
        f"Selected {label}: {selected_path.resolve(strict=False)}"
    )


def start_folder_dialog(
    state: GuiState,
    key: str,
    title: str,
    initial_dir: Path,
) -> Path | None:
    try:
        from imgui_bundle import portable_file_dialogs as pfd

        state.folder_dialogs[key] = pfd.select_folder(title, str(initial_dir))
        return None
    except Exception as exc:
        state.runner.append_log(
            f"Native folder picker unavailable ({exc}); trying Tkinter."
        )

    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        root.update()
        try:
            selected = filedialog.askdirectory(
                title=title,
                initialdir=str(initial_dir),
                mustexist=False,
            )
        finally:
            root.destroy()
    except Exception as exc:
        state.runner.append_log(f"Folder picker unavailable: {exc}")
        return None

    return Path(selected) if selected else None


def start_file_dialog(
    state: GuiState,
    key: str,
    title: str,
    initial_dir: Path,
) -> Path | None:
    try:
        from imgui_bundle import portable_file_dialogs as pfd

        state.file_dialogs[key] = pfd.open_file(
            title,
            str(initial_dir),
            [
                "Sheet files",
                "*.csv *.xlsx *.xlsm *.xltx *.xltm",
                "CSV files",
                "*.csv",
                "Excel files",
                "*.xlsx *.xlsm *.xltx *.xltm",
                "All files",
                "*",
            ],
        )
        return None
    except Exception as exc:
        state.runner.append_log(
            f"Native file picker unavailable ({exc}); trying Tkinter."
        )

    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        root.update()
        try:
            selected = filedialog.askopenfilename(
                title=title,
                initialdir=str(initial_dir),
                filetypes=(
                    (
                        "Sheet files",
                        "*.csv *.xlsx *.xlsm *.xltx *.xltm",
                    ),
                    ("CSV files", "*.csv"),
                    ("Excel files", "*.xlsx *.xlsm *.xltx *.xltm"),
                    ("All files", "*"),
                ),
            )
        finally:
            root.destroy()
    except Exception as exc:
        state.runner.append_log(f"File picker unavailable: {exc}")
        return None

    return Path(selected) if selected else None


def consume_folder_dialog_result(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    key: str,
    label: str,
) -> None:
    dialog: Any = state.folder_dialogs.get(key)
    if dialog is None:
        return

    try:
        if not dialog.ready():
            return
        result = dialog.result()
    except Exception as exc:
        state.runner.append_log(f"Folder picker failed: {exc}")
        state.folder_dialogs.pop(key, None)
        return

    state.folder_dialogs.pop(key, None)
    if not result:
        return

    if isinstance(result, (list, tuple)):
        selected = result[0] if result else ""
    else:
        selected = result
    if selected:
        apply_selected_folder(state, command, values, key, label, selected)


def consume_file_dialog_result(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    key: str,
    label: str,
) -> None:
    dialog: Any = state.file_dialogs.get(key)
    if dialog is None:
        return

    try:
        if not dialog.ready():
            return
        result = dialog.result()
    except Exception as exc:
        state.runner.append_log(f"File picker failed: {exc}")
        state.file_dialogs.pop(key, None)
        return

    state.file_dialogs.pop(key, None)
    if not result:
        return

    if isinstance(result, (list, tuple)):
        selected = result[0] if result else ""
    else:
        selected = result
    if selected:
        apply_selected_file(state, command, values, key, label, selected)
