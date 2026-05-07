#!/home/ahmad/automations/dengue/.venv/bin/python
"""
Quick test script to verify Playwright + System Chromium on CachyOS
Usage: ./test_playwright.py
"""
import sys
import shutil
from pathlib import Path

# Ensure the venv's site-packages are in path (in case shebang isn't enough)
venv_lib = Path("/home/ahmad/automations/dengue/.venv/lib/python3.14/site-packages")
if str(venv_lib) not in sys.path:
    sys.path.insert(0, str(venv_lib))

from playwright.sync_api import sync_playwright

def main():
    # Find system Chromium
    chromium_path = shutil.which("chromium") or "/usr/bin/chromium"

    if not Path(chromium_path).exists():
        print(f"❌ Error: Chromium not found at {chromium_path}")
        print("💡 Install with: sudo pacman -S chromium")
        return 1

    print(f"🎯 Using Chromium: {chromium_path}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                executable_path=chromium_path,
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            page = browser.new_page()
            page.goto("https://example.com", wait_until="domcontentloaded")

            title = page.title()
            ua = page.evaluate("navigator.userAgent")

            print(f"✅ Page Title: {title}")
            print(f"✅ User-Agent: {ua[:100]}...")  # Truncate long UA

            # Bonus: Check if Arch is in UA
            if "Arch" in ua or "Linux" in ua:
                print("🐧 Confirmed: Running on system Chromium (Arch/CachyOS)")

            browser.close()
            return 0

    except Exception as e:
        print(f"❌ Error: {type(e).__name__}: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
