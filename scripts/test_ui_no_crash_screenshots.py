#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs" / "reports" / "ui_smoke"

QUERIES = [
    "Miten menot kehittyivät 2020-2024?",
    "Top 10 eniten kasvaneet momentit 2010-2024",
    "Mitkä alamomentit kasvoivat eniten 2020-2024?",
    "Näytä trendi hallinnonaloittain 2008-2024",
]


def _pick_free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_http(url: str, timeout_sec: int = 60) -> None:
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            with urlopen(url, timeout=2) as resp:
                if resp.status < 500:
                    return
        except Exception:
            time.sleep(0.5)
    raise TimeoutError(f"Streamlit did not become ready: {url}")


def main() -> None:
    parser = argparse.ArgumentParser(description="No-crash screenshot smoke test (Playwright).")
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        print("SKIP: playwright is not installed (install with: pip install playwright && playwright install chromium)")
        raise SystemExit(0)

    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"

    env = os.environ.copy()
    env.setdefault("BUDJETTIHAUKKA_DATA_SOURCE", "google_sheets")
    env.setdefault("BUDJETTIHAUKKA_ENABLE_LLM_QUERY_PLAN", "0")
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{ROOT}:{existing_pythonpath}" if existing_pythonpath else str(ROOT)

    streamlit_bin = ROOT / ".venv" / "bin" / "streamlit"
    cmd = [
        str(streamlit_bin),
        "run",
        str(ROOT / "streamlit_app.py"),
        "--server.headless",
        "true",
        "--server.port",
        str(port),
    ]

    process = subprocess.Popen(cmd, cwd=str(ROOT), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_http(base_url, timeout_sec=min(60, args.timeout))
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1600, "height": 1200})
            page.goto(base_url, wait_until="domcontentloaded", timeout=args.timeout * 1000)

            for idx, query in enumerate(QUERIES, start=1):
                page.locator("textarea").first.fill(query)
                page.get_by_role("button", name="Hae tulokset").click()
                page.wait_for_timeout(2500)

                # Fail if the app crashes visibly.
                if page.get_by_text("Virhe sovelluksessa").count() > 0:
                    raise AssertionError(f"UI crash detected after query: {query}")

                shot = OUT_DIR / f"query_{idx:02d}.png"
                page.screenshot(path=str(shot), full_page=True)
                print(f"OK screenshot: {shot}")

            browser.close()

        print("No-crash screenshot smoke test PASSED")
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    main()
