from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    project_root = Path(__file__).resolve().parent
    latest_path = project_root / "reports" / "pmdu" / "notification_previews" / "LATEST"
    if not latest_path.exists():
        raise SystemExit("No notification preview has been created yet.")

    batch_dir = Path(latest_path.read_text(encoding="utf-8").strip())
    preview_path = batch_dir / "preview.html"
    if not preview_path.exists():
        raise SystemExit(f"Latest preview HTML not found: {preview_path}")

    opener = "xdg-open" if sys.platform.startswith("linux") else "open"
    subprocess.Popen([opener, str(preview_path)])
    print(f"Opened {preview_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
