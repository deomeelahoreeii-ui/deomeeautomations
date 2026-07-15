from __future__ import annotations

import traceback

from imgui_bundle import imgui

from complaints_manager.core.commands import SYSTEMS, command_by_id, commands_by_system
from complaints_manager.gui.antidengue.dashboard import draw_antidengue_dashboard
from complaints_manager.gui.antidengue.schedule import process_antidengue_schedules
from complaints_manager.gui.commands.detail import draw_command_detail, draw_confirm_modal
from complaints_manager.gui.commands.list_view import draw_command_list
from complaints_manager.gui.commands.logs import draw_logs
from complaints_manager.gui.navigation import draw_metric_row, draw_top_bar, system_by_id
from complaints_manager.gui.services.views import draw_service_detail, draw_services_panel
from complaints_manager.gui.state import GuiState


def draw_home(state: GuiState) -> None:
    draw_top_bar(state)
    imgui.text("Select an automation area")
    imgui.spacing()
    for system in SYSTEMS:
        imgui.push_id(system.id)
        imgui.text_colored(system.accent, system.title)
        imgui.same_line()
        imgui.text_disabled(system.subtitle)
        imgui.text_wrapped(system.summary)
        if imgui.button(f"Open {system.title}", (180, 0)):
            state.current_system_id = system.id
            commands = commands_by_system()[system.id]
            if commands:
                state.selected_command_id = commands[0].id
        imgui.separator()
        imgui.pop_id()


def draw_service(state: GuiState, service_id: str) -> None:
    draw_top_bar(state)
    if imgui.button("Back", (90, 0)):
        state.current_service_id = None
        return
    draw_service_detail(state, service_id)


def draw_system(state: GuiState, system_id: str) -> None:
    system = system_by_id(system_id)
    draw_top_bar(state)
    if imgui.button("Back", (90, 0)):
        state.current_system_id = None
        return
    imgui.same_line()
    imgui.text_colored(system.accent, system.title)
    imgui.same_line()
    imgui.text_disabled(system.subtitle)
    imgui.text_wrapped(system.summary)
    draw_metric_row(state, system_id)
    imgui.separator()

    if system_id == "services":
        draw_services_panel(state)
        return

    if system_id == "antidengue":
        draw_antidengue_dashboard(state)
        draw_logs(state)
        return

    commands = commands_by_system().get(system_id, [])
    if not commands:
        imgui.text_disabled(f"No commands are registered for system: {system_id}")
        return

    left_width = 270
    imgui.begin_child(f"{system_id}_commands", (left_width, 0), True)
    try:
        draw_command_list(state, system_id)
    finally:
        imgui.end_child()

    imgui.same_line()
    imgui.begin_child(f"{system_id}_detail", (0, 0), False)
    try:
        try:
            command = command_by_id(state.selected_command_id)
        except KeyError:
            command = commands[0]
            state.selected_command_id = command.id
        if command.system_id != system_id:
            command = commands[0]
            state.selected_command_id = command.id
        draw_command_detail(state, command)
        draw_logs(state)
    finally:
        imgui.end_child()

    draw_confirm_modal(state)


def draw_gui(state: GuiState) -> None:
    try:
        state.runner.drain_output()
        state.services.drain_output()
        process_antidengue_schedules(state)
        if state.current_service_id is not None:
            draw_service(state, state.current_service_id)
            return
        if state.current_system_id is None:
            draw_home(state)
        else:
            draw_system(state, state.current_system_id)
        state.last_gui_error = ""
    except Exception as exc:
        state.last_gui_error = traceback.format_exc()
        traceback.print_exc()
        state.runner.append_log(f"GUI render error: {exc}")
        draw_top_bar(state)
        imgui.text_colored((0.95, 0.35, 0.28, 1.0), "GUI render error")
        imgui.text_wrapped(str(exc))
        if imgui.button("Back", (90, 0)):
            state.current_service_id = None
            state.current_system_id = None
        imgui.separator()
        imgui.text_disabled("Details")
        imgui.text_wrapped(state.last_gui_error[-4000:])
