from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def main() -> int:
    project_root = Path(__file__).resolve().parent
    latest_path = (
        project_root
        / "reports"
        / "crm"
        / "compliance_dispatch_previews"
        / "LATEST"
    )
    if not latest_path.exists():
        raise SystemExit("No CRM compliance dispatch preview has been generated yet.")

    preview_dir = Path(latest_path.read_text(encoding="utf-8").strip())
    preview_html = preview_dir / "preview.html"
    if not preview_html.exists():
        raise SystemExit(f"Latest CRM compliance dispatch preview is missing: {preview_html}")

    opener = shutil.which("xdg-open")
    if not opener:
        raise SystemExit("Cannot open preview; xdg-open was not found.")
    subprocess.Popen(
        [opener, str(preview_html)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    print(f"Opened {preview_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
