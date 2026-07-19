from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


CONSOLIDATED_DIGEST_REPORT_KEY = "consolidated_action_digest"
CONSOLIDATED_DIGEST_RENDERER_KEY = "antidengue.consolidated_action_digest.v1"
CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE = """🚨 *ANTI-DENGUE ACTION DIGEST*

📅 *Generated:* {{generated_at}}
👥 *Audience:* {{group_name}}
🏢 *Wing:* {{wing_name}}
📍 *Scope:* {{scope_name}}

*{{school_count}} UNIQUE SCHOOLS REQUIRE ATTENTION*

💤 Dormant activity: {{dormant_summary}}
📏 Hotspot distance: {{hotspot_summary}}
⏱️ Activity timing: {{timing_summary}}

*ISSUE LEGEND*
💤 No qualifying activity submitted
📏 Activity outside the permitted distance
⏱️ Before/after evidence submitted too quickly

*{{detail_heading}}*

{{school_details}}

⏰ *Action required before {{deadline}}*
Submit missing activities and review flagged distance or timing evidence.

📎 *Attached workbook:* Overview · Dormant Schools · Hotspot Distance · Activity Timing

⚠️ Distance and timing findings are rule-based review candidates, not confirmed violations."""

# Recognized immutable v1 default. Configuration bootstrap upgrades only this
# exact body (or {{report_body}}), never operator-customized templates.
CONSOLIDATED_DIGEST_LEGACY_TEMPLATE_V1 = CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE.replace(
    "*{{detail_heading}}*", "*DETAILS BY MARKAZ*"
)


@dataclass(frozen=True)
class DigestSchool:
    emis: str
    school_name: str
    wing: str
    tehsil: str
    markaz: str
    dormant: bool = False
    hotspot_distance: bool = False
    activity_timing: bool = False

    @property
    def issue_codes(self) -> list[str]:
        codes: list[str] = []
        if self.dormant:
            codes.append("DORMANT")
        if self.hotspot_distance:
            codes.append("DISTANCE")
        if self.activity_timing:
            codes.append("TIMING")
        return codes

    @property
    def issue_icons(self) -> str:
        icons: list[str] = []
        if self.dormant:
            icons.append("💤")
        if self.hotspot_distance:
            icons.append("📏")
        if self.activity_timing:
            icons.append("⏱️")
        return " ".join(icons)


@dataclass
class RenderedConsolidatedDigest:
    message: str
    context: dict[str, str]
    schools: list[DigestSchool]
    dormant_rows: list[Any]
    hotspot_rows: list[Any]
    timing_rows: list[Any]
    issues: list[dict[str, Any]] = field(default_factory=list)
    attachment_paths: list[Path] = field(default_factory=list)
    source_artifact_id: int | None = None
    source_artifact_sha256: str = ""


__all__ = [
    "CONSOLIDATED_DIGEST_RENDERER_KEY",
    "CONSOLIDATED_DIGEST_REPORT_KEY",
    "CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE",
    "CONSOLIDATED_DIGEST_LEGACY_TEMPLATE_V1",
    "DigestSchool",
    "RenderedConsolidatedDigest",
]
