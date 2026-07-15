from __future__ import annotations

from pathlib import Path

from imgui_bundle import immapp

from complaints_manager.gui.screens import draw_gui
from complaints_manager.gui.state import GuiState


def run_gui(project_root: Path | None = None) -> None:
    root = project_root or Path(__file__).resolve().parents[2]
    state = GuiState.create(root)
    try:
        immapp.run(
            lambda: draw_gui(state),
            window_title="Automation Management",
            window_size=(1180, 760),
            ini_disable=True,
        )
    finally:
        state.services.shutdown()
        state.runner.shutdown()
