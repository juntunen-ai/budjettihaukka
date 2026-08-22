#!/usr/bin/env python3
"""Regressiotestit talouskasvun ja valtionvelan suhteen visualisoinnille.

Testi ei ota verkkoyhteyttä eikä BigQueryyn. Se ajaa saman
sopimusvalidoinnin kuin lataaja ja varmistaa, että sivu erottaa yhteyden
syy-seuraussuhteesta.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_growth_debt_relation import OUT, validate

PAGE = ROOT / "kasvu-ja-velka.html"

# Kaksi kriisivuotta, joiden on erotuttava aineistosta. Jos nama muuttuvat,
# joko velka- tai BKT-sarja on vaihtunut toiseen mittariin.
CRISIS_YEARS = {2009, 2020}


def main() -> None:
    payload = json.loads(OUT.read_text(encoding="utf-8"))
    meta = payload["meta"]
    years = payload["years"]

    validate(payload)

    assert meta["year_from"] == 2001 and meta["year_to"] == 2025, meta
    assert len(years) == 25, len(years)

    by_year = {row["year"]: row for row in years}
    assert CRISIS_YEARS <= set(by_year)

    # Hajotelma summautuu muutokseen tarkalleen jokaisena vuotena. Tama on
    # koko sivun selityksen perusta.
    for row in years:
        total = row["borrowing_effect_pp"] + row["denominator_effect_pp"]
        assert abs(total - row["debt_pct_gdp_change_pp"]) < 1e-6, row["year"]

    # Kriisivuosina talous kutistui ja velkasuhde nousi.
    for year in CRISIS_YEARS:
        row = by_year[year]
        assert row["gdp_volume_change_pct"] < 0, row
        assert row["debt_pct_gdp_change_pp"] > 0, row

    # Nelikentan tyhja lohko on sivun paavaite: yhtenakaan vuonna talous ei
    # kutistunut velkasuhteen laskiessa.
    shrink_and_down = [
        row for row in years
        if row["gdp_volume_change_pct"] < 0 and row["debt_pct_gdp_change_pp"] < 0
    ]
    assert not shrink_and_down, shrink_and_down

    # Kaksi viimeisinta vuotta: velkasuhde nousi vaikka talous kasvoi, ja
    # nousu tuli velanotosta eika kutistuvasta taloudesta.
    for row in years[-2:]:
        assert row["gdp_volume_change_pct"] > 0, row
        assert row["debt_pct_gdp_change_pp"] > 0, row
        assert row["borrowing_effect_pp"] > row["debt_pct_gdp_change_pp"], row
        assert row["denominator_effect_pp"] < 0, row

    # Velkasuhde on noussut selvasti jakson aikana.
    assert meta["debt_pct_gdp_last"] > meta["debt_pct_gdp_first"] + 20

    html = PAGE.read_text(encoding="utf-8")
    # Tekstivaitteet tarkistetaan valilyonnit normalisoituna, koska HTML
    # rivittaa kappaleet eika rivinvaihto saa kaataa testia.
    flat = re.sub(r"\s+", " ", html)
    assert 'lang="fi"' in html
    assert "<input" not in html and "<select" not in html
    assert html.count("<section>") == 1, "yksi kuva, yksi osio"
    assert html.count("Päähavainto.") == 1
    assert html.count("Mitä kuva ei kerro.") == 1
    # Korrelaatiota ei saa esittaa syy-seuraussuhteena.
    assert "Kuva näyttää yhteyden, ei syytä" in flat
    assert "ei kerro syy-seuraussuhdetta" in flat
    # Nimellisen ja volyymin ero on kerrottava, koska akselit ovat eri asiaa.
    assert "nimellinen</em> BKT" in flat
    assert "volyymin muutos ilman inflaatiota" in flat
    # Velan rajaus.
    assert "EDP-velkakanta" in flat
    assert "ei koko julkisen talouden velka" in flat

    embedded = re.search(r'<script type="application/json" id="relation-data">(.*?)</script>', html, re.S)
    assert embedded, "upotettu snapshot puuttuu"
    parsed = json.loads(embedded.group(1))
    assert len(parsed["years"]) == len(years)
    assert [row["year"] for row in parsed["years"]] == [row["year"] for row in years]

    print(f"Growth and debt relation OK ({len(years)} vuotta, velkasuhde "
          f"{meta['debt_pct_gdp_first']:.1f} % -> {meta['debt_pct_gdp_last']:.1f} %)")


if __name__ == "__main__":
    main()
