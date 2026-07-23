from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_MAIN = ROOT / "apps/api/automation_api/main.py"
WEB_MIDDLEWARE = ROOT / "apps/web/src/middleware.ts"
PREVIEW_ROUTE = ROOT / "apps/web/src/pages/crm/pdf-preview/[id].pdf.ts"
PREPARE_PAGE = ROOT / "apps/web/src/pages/crm/replies/[id]/official-letter.astro"


def test_pdf_preview_uses_a_same_origin_range_preserving_proxy() -> None:
    middleware = WEB_MIDDLEWARE.read_text(encoding="utf-8")
    route = PREVIEW_ROUTE.read_text(encoding="utf-8")
    page = PREPARE_PAGE.read_text(encoding="utf-8")
    api_main = API_MAIN.read_text(encoding="utf-8")

    assert 'x-frame-options", "DENY"' in api_main
    assert 'pathname.startsWith("/crm/pdf-preview/")' in middleware
    assert 'x-frame-options", "SAMEORIGIN"' in middleware
    assert "frame-ancestors 'self'" in middleware
    assert 'request.headers.get("range")' in route
    assert 'upstream.headers.get("content-range")' in route
    assert 'Content-Type": "application/pdf"' in route
    assert "previewProxyUrl" in page
    assert "letter-preview-frame" in page
    assert "preview-load-error" in page
