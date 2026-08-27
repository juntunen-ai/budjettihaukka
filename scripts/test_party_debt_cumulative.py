#!/usr/bin/env python3
"""Regressiotestit puoluekohtaiselle nettovelanotolle.

Testi ei ota verkkoyhteyttä eikä BigQueryyn. Se ajaa saman
sopimusvalidoinnin kuin lataaja ja tarkistaa, että sivu kertoo
kohdennuksen rajoitteet näkyvästi.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_party_debt_cumulative import validate

PAGE = ROOT / "puoluevelka.html"
REFERENCE = ROOT / "data" / "reference" / "party_debt_cumulative_v1.json"

# Velanhallintamomentit, joista mittari muodostuu. Jos lähde nimeää ne
# uudelleen, mittarin sisältö muuttuu eikä sarja ole enää vertailukelpoinen.
EXPECTED_MOMENTS = {"15.03.01.", "37.01.94."}

# Aineisto ei ala 1998 vaan 2001, koska sitä ennen velanhallintamomenttia
# ei ole lainkaan. Tämä rajaus on kerrottava sivulla.
EXPECTED_FIRST_MONTH = "2001-01-01"


def main() -> None:
    payload = json.loads(REFERENCE.read_text(encoding="utf-8"))
    meta = payload["meta"]
    parties = payload["parties"]
    series = payload["monthly"]

    validate(payload)

    assert {row["momentti_tunnusp"] for row in meta["moments"]} == EXPECTED_MOMENTS
    assert meta["first_month"] == EXPECTED_FIRST_MONTH
    assert meta["observed_months"] <= meta["span_months"]
    assert meta["missing_months_in_span"] == meta["span_months"] - meta["observed_months"]

    # Kolme pääministeripuoluetta koko tarkastelujaksolla.
    assert len(parties) == 3
    names = {row["pm_party_fi"] for row in parties}
    assert names == {
        "Kansallinen Kokoomus",
        "Suomen Sosialidemokraattinen Puolue",
        "Suomen Keskusta",
    }, names

    # Lista on suuruusjärjestyksessä ja kuukausivauhti on johdettu oikein.
    totals = [row["debt_change_eur"] for row in parties]
    assert totals == sorted(totals, reverse=True)
    for row in parties:
        expected = row["debt_change_eur"] / row["months"]
        assert abs(row["debt_change_per_month_eur"] - expected) < 1.0
        assert row["cabinets"], row["pm_party_fi"]
        assert row["months"] > 0

    # Mittari kulkee molempiin suuntiin: velkaa on myös lyhennetty.
    assert any(row["debt_change_eur"] < 0 for row in series)
    assert sum(row["repayment_months"] for row in parties) == sum(
        1 for row in series if row["debt_change_eur"] < 0
    )

    # Kumulatiivinen sarja päättyy kokonaismuutokseen.
    assert abs(series[-1]["cumulative_eur"] - meta["total_debt_change_eur"]) < 1.0

    html = PAGE.read_text(encoding="utf-8")
    assert 'lang="fi"' in html
    assert "<input" not in html and "<select" not in html
    assert html.count("<section>") == 1, "yksi kuva, yksi osio"
    assert html.count("Päähavainto.") == 1
    assert html.count("Mitä kuva ei kerro.") == 1
    # Kolme rajoitetta, joita ilman kuva johtaisi harhaan.
    assert "Pääministerin puolue ei ole hallitus" in html
    assert "kausien pituudet eroavat" in html.lower()
    assert "1998–2000" in html
    assert "ei valtionvelan kokonaismäärää" in html or "ei valtionvelan kokonaism" in html

    embedded = re.search(r'<script type="application/json" id="debt-data">(.*?)</script>', html, re.S)
    assert embedded, "upotettu snapshot puuttuu"
    parsed = json.loads(embedded.group(1))
    assert parsed["meta"]["total_debt_change_eur"] == meta["total_debt_change_eur"]
    assert parsed["meta"]["observed_months"] == meta["observed_months"]
    assert [row["pm_party_fi"] for row in parsed["parties"]] == [
        row["pm_party_fi"] for row in parties
    ]

    print(f"Party debt cumulative OK ({len(parties)} puoluetta, "
          f"{meta['observed_months']} kuukautta, "
          f"{meta['total_debt_change_eur'] / 1e9:.1f} mrd €)")


if __name__ == "__main__":
    main()
