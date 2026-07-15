from __future__ import annotations

import subprocess
from pathlib import Path


def main() -> None:
    project_root = Path(__file__).resolve().parent
    latest_path = project_root / "reports" / "pmdu" / "followup_previews" / "LATEST"
    if not latest_path.exists():
        raise SystemExit("No inquiry follow-up preview has been generated yet.")
    preview_dir = Path(latest_path.read_text(encoding="utf-8").strip())
    preview_html = preview_dir / "preview.html"
    if not preview_html.exists():
        raise SystemExit(f"Latest follow-up preview is missing: {preview_html}")
    subprocess.Popen(["xdg-open", str(preview_html)])
    print(f"Opened {preview_html}")


if __name__ == "__main__":
    main()
