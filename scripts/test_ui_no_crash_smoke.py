#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "streamlit_app.py"

QUERIES = [
    "Miten menot kehittyivät 2020-2024?",
    "Top 10 eniten kasvaneet momentit 2010-2024",
    "Mitkä alamomentit kasvoivat eniten 2020-2024?",
    "Näytä trendi hallinnonaloittain 2008-2024",
    "Onko kuukausissa kausivaihtelua 2022-2024?",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="No-crash UI smoke test for key visualization flows.")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()

    os.environ.setdefault("BUDJETTIHAUKKA_DATA_SOURCE", "google_sheets")
    os.environ.setdefault("BUDJETTIHAUKKA_ENABLE_LLM_QUERY_PLAN", "0")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    os.chdir(ROOT)

    at = AppTest.from_file(str(APP))
    at.run(timeout=args.timeout)

    if len(at.text_area) == 0 or len(at.button) == 0:
        raise AssertionError("UI shape changed: expected at least one text_area and button")

    for idx, query in enumerate(QUERIES, start=1):
        at.text_area[0].set_value(query)
        at.button[0].click()
        at.run(timeout=args.timeout)

        if len(at.exception) > 0:
            raise AssertionError(f"UI exception after query {idx}: {query}")

        print(
            f"OK query {idx}/{len(QUERIES)}: exceptions=0, "
            f"errors={len(at.error)}, warnings={len(at.warning)}, dataframes={len(at.dataframe)}"
        )

    print("No-crash UI smoke test PASSED")


if __name__ == "__main__":
    main()
