#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    csv_path = ROOT / "data" / "reference" / "official_sote_demo_v1.csv"
    json_path = ROOT / "data" / "reference" / "official_sote_demo_v1.json"
    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    json_rows = json.loads(json_path.read_text(encoding="utf-8"))
    assert len(rows) == len(json_rows) == 155
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["metric_id"]].append(row)
        assert 2008 <= int(row["year"]) <= 2025
        assert row["source_url"].startswith("https://ec.europa.eu/eurostat/")
    assert len(grouped) == 10
    assert grouped["unmet_medical_needs_pct"][-1]["year"] == "2025"
    assert float(grouped["unmet_medical_needs_pct"][-1]["value"]) == 7.8
    assert float(grouped["life_expectancy_years"][0]["value"]) == 79.9
    assert float(grouped["life_expectancy_years"][-1]["value"]) == 82.2
    assert float(grouped["tax_burden_pct_gdp"][0]["value"]) == 41.1
    assert float(grouped["tax_burden_pct_gdp"][-1]["value"]) == 42.2

    html = (ROOT / "sote-demo.html").read_text(encoding="utf-8")
    assert html.count("<section>") == 6
    assert html.count("Päähavainto.") == 5
    assert html.count("Mitä kuva ei kerro.") == 5
    assert "<input" not in html and "<select" not in html and "<button" not in html
    assert "official_sote_demo_v1.json" in html
    assert "lievästi huonompaan suuntaan" in html
    print("SOTE demo OK")


if __name__ == "__main__":
    main()
