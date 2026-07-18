"""Compatibility facade for the modular AntiDengue renderer."""

from whatsapp_gateway.rendering.antidengue.excel import _ensure_excel_attachment
from whatsapp_gateway.rendering.antidengue.fonts import _font, _wrap_text
from whatsapp_gateway.rendering.antidengue.formatting import (
    _report_time, _school_count, _wing_display_name, _wing_label,
)
from whatsapp_gateway.rendering.antidengue.images import _ensure_image_attachment
from whatsapp_gateway.rendering.antidengue.issues import _issue
from whatsapp_gateway.rendering.antidengue.messages import _build_message, _build_wing_message
from whatsapp_gateway.rendering.antidengue.models import (
    PAKISTAN_TIME, RENDERER_KEY, REQUIRED_COLUMNS, WING_RENDERER_KEY,
    DormantSourceRow, RenderedTehsilDormantReport, RenderedWingDormantReport,
    ScopedDormantSchool,
)
from whatsapp_gateway.rendering.antidengue.policy import (
    _has_attachment, normalize_presentation_policy,
)
from whatsapp_gateway.rendering.antidengue.source_data import (
    _canonical_report_artifact, _normalize_emis, _report_rows,
)
from whatsapp_gateway.rendering.antidengue.tehsil_report import render_tehsil_dormant_report
from whatsapp_gateway.rendering.antidengue.wing_report import render_wing_dormant_report
from whatsapp_gateway.rendering.antidengue.hotspot_report import (
    HOTSPOT_RENDERER_KEY,
    HOTSPOT_REPORT_KEY,
    RenderedHotspotReport,
    render_hotspot_distance_report,
)
from whatsapp_gateway.rendering.antidengue.simple_activity_report import (
    SIMPLE_ACTIVITY_RENDERER_KEY, SIMPLE_ACTIVITY_REPORT_KEY,
    RenderedSimpleActivityReport, render_simple_activity_report,
)

__all__ = [
    "PAKISTAN_TIME", "RENDERER_KEY", "REQUIRED_COLUMNS", "WING_RENDERER_KEY",
    "DormantSourceRow", "RenderedTehsilDormantReport", "RenderedWingDormantReport",
    "ScopedDormantSchool", "_issue", "_normalize_emis", "_report_rows",
    "_canonical_report_artifact", "_school_count", "normalize_presentation_policy",
    "_has_attachment", "_wing_label", "_wing_display_name", "_report_time",
    "_build_message", "_build_wing_message", "_ensure_excel_attachment",
    "_font", "_wrap_text", "_ensure_image_attachment",
    "render_wing_dormant_report", "render_tehsil_dormant_report",
    "HOTSPOT_RENDERER_KEY", "HOTSPOT_REPORT_KEY", "RenderedHotspotReport",
    "render_hotspot_distance_report",
    "SIMPLE_ACTIVITY_RENDERER_KEY", "SIMPLE_ACTIVITY_REPORT_KEY",
    "RenderedSimpleActivityReport", "render_simple_activity_report",
]
