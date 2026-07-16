from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import Counter
from pathlib import Path

from PIL import Image, ImageDraw
from sqlalchemy import select
from sqlmodel import Session

from automation_core.config import get_settings
from automation_core.models import Artifact, Job
from master_data.models import Wing
from whatsapp_gateway.rendering.antidengue.fonts import _font, _wrap_text
from whatsapp_gateway.rendering.antidengue.formatting import (
    _school_count, _wing_display_name,
)
from whatsapp_gateway.rendering.antidengue.models import ScopedDormantSchool


def _ensure_image_attachment(
    session: Session,
    *,
    source_job: Job,
    wing: Wing,
    schools: list[ScopedDormantSchool],
    source_digest: str,
    scope_key: str,
    scope_label: str,
    image_content: str,
) -> tuple[Artifact, Path, str]:
    content_key = hashlib.sha256(
        json.dumps(
            {
                "source": source_digest,
                "wing": str(wing.id),
                "scope": [scope_key, scope_label],
                "content": image_content,
                "layout": 2,
                "schools": [[s.emis, s.school_name, s.tehsil, s.markaz] for s in schools],
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    safe_wing = re.sub(r"[^A-Za-z0-9_-]+", "-", wing.code or wing.name).strip("-") or "wing"
    safe_scope = re.sub(r"[^A-Za-z0-9_-]+", "-", scope_label).strip("-") or scope_key
    filename = f"Anti-Dengue Dormant Users - {safe_wing} - {safe_scope}.png"
    path = get_settings().artifact_root.expanduser().resolve() / "antidengue" / "native-reports" / str(source_job.id) / f"{safe_wing}-{safe_scope}-{content_key[:16]}.png"
    artifact = session.scalar(select(Artifact).where(Artifact.job_id == source_job.id, Artifact.path == str(path)))
    if artifact is not None and path.is_file():
        return artifact, path, hashlib.sha256(path.read_bytes()).hexdigest()

    width = 1080
    margin = 64
    title_font, heading_font, body_font, small_font = _font(34, bold=True), _font(24, bold=True), _font(20), _font(17)
    scratch = Image.new("RGB", (width, 100), "white")
    draw = ImageDraw.Draw(scratch)
    rows: list[tuple[str, str, str]] = []
    by_tehsil = Counter(s.tehsil for s in schools)
    for tehsil, count in sorted(by_tehsil.items(), key=lambda item: item[0].casefold()):
        rows.append((tehsil, _school_count(count), "summary"))
    if image_content == "details":
        rows.append(("SCHOOL DETAILS", "", "heading"))
        for school in schools:
            rows.append((f"{school.emis}  {school.school_name}", f"{school.tehsil} · {school.markaz}", "detail"))
    calculated = 300
    for left, right, kind in rows:
        if kind == "heading":
            calculated += 64
        else:
            lines = _wrap_text(draw, left, body_font if kind == "summary" else small_font, 700 if kind == "summary" else 900)
            calculated += max(58, len(lines) * (27 if kind == "summary" else 23) + (28 if kind == "summary" else 50))
    height = min(max(calculated + 80, 720), 14000)
    image = Image.new("RGB", (width, height), "#F4F7FA")
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((32, 32, width - 32, height - 32), radius=22, fill="white", outline="#D9E2EC", width=2)
    draw.rounded_rectangle((32, 32, width - 32, 210), radius=22, fill="#087487")
    draw.text((margin, 62), "ANTI-DENGUE DORMANCY REPORT", font=title_font, fill="white")
    draw.text((margin, 115), f"{scope_label}  ·  {_wing_display_name(wing)}", font=heading_font, fill="#D8F5F5")
    draw.text((margin, 158), f"Total dormant: {_school_count(len(schools))}", font=body_font, fill="white")
    y = 246
    draw.text((margin, y), "TEHSIL SUMMARY", font=heading_font, fill="#26445D")
    y += 48
    for left, right, kind in rows:
        if y > height - 80:
            break
        if kind == "heading":
            y += 18
            draw.text((margin, y), left, font=heading_font, fill="#087487")
            y += 48
            continue
        font = body_font if kind == "summary" else small_font
        lines = _wrap_text(draw, left, font, 700 if kind == "summary" else 900)
        row_height = max(58, len(lines) * (27 if kind == "summary" else 23) + (28 if kind == "summary" else 50))
        draw.rounded_rectangle((margin, y, width - margin, y + row_height - 8), radius=10, fill="#F7FAFC")
        line_y = y + 12
        for line in lines:
            draw.text((margin + 18, line_y), line, font=font, fill="#1F2937")
            line_y += 27 if kind == "summary" else 23
        if right and kind == "summary":
            draw.text((width - margin - 18, y + 14), right, font=small_font, fill="#60758A", anchor="ra")
        elif right:
            draw.text((margin + 18, line_y + 4), right, font=small_font, fill="#60758A")
        y += row_height
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    image.save(temporary, format="PNG", optimize=True)
    temporary.replace(path)
    if artifact is None:
        artifact = Artifact(job_id=source_job.id, kind="delivery", name=filename, path=str(path), size_bytes=path.stat().st_size)
    else:
        artifact.name = filename
        artifact.size_bytes = path.stat().st_size
    session.add(artifact)
    session.flush()
    return artifact, path, hashlib.sha256(path.read_bytes()).hexdigest()
