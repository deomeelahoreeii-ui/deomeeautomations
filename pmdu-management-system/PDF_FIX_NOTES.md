# Phase 2 PDF Fix Notes

## What Changed

- Replaced `page.goto(file://...)` with `page.set_content(...)` so Playwright renders the saved portal HTML directly.
- Added a browser context `base_url` plus an injected `<base href="https://admin.pmdu.gov.pk/">` tag so CSS, JavaScript, image, and font asset URLs resolve as they do on the live portal.
- Render only the portal `.printable` block, matching the page's `printThis` button behavior instead of printing the normal screen complaint view.
- Preserve original stylesheet links and load `https://admin.pmdu.gov.pk/assets/css/print.css`, matching `$('.printable').printThis({ importCSS: true, loadCSS: ... })`.
- Added `page.emulate_media(media="print")` before PDF generation to activate the portal's `@media print` CSS.
- Added render waits for `networkidle`, portal selectors, and `document.fonts.ready`.
- Added Urdu-capable font fallbacks: `Noto Nastaliq Urdu`, `Noto Naskh Arabic`, `Noto Sans Arabic`, then common browser fonts.
- Added Chrome-style header and footer templates so output includes title, date, source file path, and page numbers like browser print output.
- Updated PDF options to match browser print behavior more closely:
  - `format="Letter"` by default because the supplied manual reference PDF is Letter sized (`612 x 792 pts`). This can be changed with `PMDU_PDF_FORMAT`.
  - `print_background=True`
  - `margin={"top":"10mm","right":"10mm","bottom":"10mm","left":"10mm"}`
  - `prefer_css_page_size=True`
- Added `pdf_generation_success` to `snapshot.json` and the DuckDB `versions` table. If PDF generation fails, attachments and snapshot generation still continue.

## Remaining Limitations

- Pixel-to-pixel comparison can be run against `sample/Complaint Details - PMDU.pdf`; exact equality may still vary by Chromium print metadata, installed fonts, and live remote asset availability.
- Exact PDF output can still vary by installed system fonts, Chromium version, and whether remote portal assets are available at render time.
- The tested environment has `Noto Nastaliq Urdu` installed, which should prevent Urdu glyph boxes. If another machine lacks Urdu fonts, install `fonts-noto-core` / `fonts-noto-extra` or equivalent.
