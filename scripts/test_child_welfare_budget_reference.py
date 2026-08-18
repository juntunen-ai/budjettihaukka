#!/usr/bin/env python3
"""Regression checks for the child-welfare and HVA-budget snapshot."""

from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
JSON_PATH = ROOT / "data/reference/child_welfare_budget_panel_v1.json"
CSV_PATH = ROOT / "data/reference/child_welfare_budget_panel_v1.csv"
SOURCES_PATH = ROOT / "data/reference/child_welfare_budget_sources_v1.json"
HTML_PATH = ROOT / "lastensuojelu-budjetti-hva.html"


def close(actual: float, expected: float, tolerance: float = 0.01) -> None:
    assert abs(float(actual) - expected) <= tolerance, (actual, expected)


def main() -> None:
    snapshot = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    sources = json.loads(SOURCES_PATH.read_text(encoding="utf-8"))
    html = HTML_PATH.read_text(encoding="utf-8")
    rows = snapshot["regions"]

    assert len(rows) == 22
    assert len(snapshot["yta"]) == 5
    assert {row["region_code"] for row in rows} == {
        *(f"{number:02d}" for number in range(1, 22)),
        "90",
    }
    assert all(
        row[f"budget_nominal_meur_{year}"] > 0
        for row in rows
        for year in range(2023, 2027)
    )
    assert all(
        row[f"placed_children_{year}"] > 0
        for row in rows
        for year in range(2021, 2025)
    )

    vk = next(row for row in rows if row["region_code"] == "04")
    close(vk["budget_nominal_meur_2023"], 1136.561)
    close(vk["budget_nominal_meur_2026"], 1329.288)
    close(vk["budget_change_pct_2023_2024"], 12.59)
    close(vk["budget_change_pct_2024_2025"], 1.32)
    close(vk["budget_change_pct_2025_2026"], 2.53)
    assert [vk[f"placed_children_{year}"] for year in range(2021, 2025)] == [
        1232,
        1244,
        1179,
        1155,
    ]
    close(vk["placed_children_pct_2024"], 1.4)

    assert sum(row["placed_children_2024"] for row in rows) == 17608
    close(sum(row["budget_nominal_meur_2023"] for row in rows), 26598.184)
    close(sum(row["budget_nominal_meur_2026"] for row in rows), 29863.348)
    east = next(row for row in snapshot["yta"] if row["yta_name"].startswith("Itä"))
    close(east["placed_children_pct_2024"], 1.6)

    with CSV_PATH.open(encoding="utf-8", newline="") as handle:
        csv_rows = list(csv.DictReader(handle))
    assert len(csv_rows) == 22
    assert len(sources["sources"]) == 2
    assert "3563" in json.dumps(sources)

    assert JSON_PATH.name in html
    assert "koko talousarvion toimintamenot" in html
    assert "ei ole automaattisesti onnistumisen merkki" in html
    assert "ei voi päätellä sijoituksen kustannusta" in html
    assert all(label in html for label in ("Δ 23–24", "Δ 24–25", "Δ 25–26"))
    assert html.count('class="finding"') == 4
    assert all(tag not in html for tag in ("<button", "<select", "<input"))

    print("Child-welfare budget reference checks OK")


if __name__ == "__main__":
    main()
