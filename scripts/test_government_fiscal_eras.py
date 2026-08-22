#!/usr/bin/env python3
"""Regressiotestit hallituskausien visualisoinnille.

Testi ei ota verkkoyhteyttä eikä BigQueryyn. Se tarkistaa committoidun
snapshotin sisäisen johdonmukaisuuden, sivun rakenteen ja sen, että sivulle
upotettu kopio vastaa referenssiaineistoa.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_government_fiscal_eras import validate

PAGE = ROOT / "hallituskaudet.html"
REFERENCE = ROOT / "data" / "reference" / "government_fiscal_eras_v1.json"

# Kolme kuukautta, joilta lähdeaineistossa ei ole yhtään nettokertymän arvoa.
# Nämä eivät ole nollia, eikä niitä saa hiljaa laskea mukaan summiin.
KNOWN_MISSING = ["2017-03-01", "2019-03-01", "2023-03-01"]


def main() -> None:
    payload = json.loads(REFERENCE.read_text(encoding="utf-8"))
    meta = payload["meta"]
    eras = payload["eras"]

    validate(payload)

    assert meta["missing_months"] == KNOWN_MISSING, meta["missing_months"]
    assert meta["real_deflator_id"] == "cpi_general_purchasing_power"
    assert meta["first_observed_month"] == "1998-01-01"
    assert len(eras) >= 12

    # Kaudet, joilta puuttuu alku tai jotka ovat kesken, on merkittävä, koska
    # niiden kokonaissummat eivät vertaudu täysiin kausiin.
    assert sum(1 for era in eras if era["is_ongoing"]) == 1
    assert eras[0]["is_truncated_start"], "1998 alkava kausi pitää merkitä katkaistuksi"
    assert eras[-1]["is_ongoing"]
    for era in eras:
        assert era["is_complete_term"] == (
            not era["is_truncated_start"] and not era["is_ongoing"]
        )

    # Reaalisumma ja sen rinnalla esitettävä nimellinen kattavat samat
    # kuukaudet, muuten inflaatiokorjaus näyttäisi vääriä eroja.
    for era in eras:
        assert era["deflated_months"] <= era["observed_months"]
        nominal = era["net_nominal_deflated_scope_eur"]
        real = era["net_real_eur"]
        if era["deflated_months"] and nominal:
            # Perusvuotta vanhempi raha on reaalisesti suurempi, joten
            # itseisarvon on kasvettava eikä merkki saa kääntyä.
            assert (real >= 0) == (nominal >= 0), era["cabinet_name"]
            assert abs(real) >= abs(nominal) * 0.999, era["cabinet_name"]

    # Meno-, tulo-, rahoitus- ja tekniset erät summautuvat nettokertymään.
    for era in eras:
        parts = (
            era["expense_nominal_eur"]
            + era["revenue_nominal_eur"]
            + era["financing_nominal_eur"]
            + era["technical_nominal_eur"]
        )
        assert abs(parts - era["net_nominal_eur"]) < 1.0, era["cabinet_name"]

    monthly = payload["monthly"]
    assert len(monthly) == meta["observed_months"]
    assert not set(row["month"] for row in monthly) & set(KNOWN_MISSING)

    html = PAGE.read_text(encoding="utf-8")
    assert html.count("<section>") == 5
    assert html.count("Päähavainto.") == 5
    assert html.count("Mitä kuva ei kerro.") == 5
    assert 'lang="fi"' in html
    assert "<input" not in html and "<select" not in html
    # Sivun on kerrottava kohdennussääntö ja päätösvallan rajoite näkyvästi.
    assert "enemmistön ajan" in html
    assert "kuka oli vallassa, ei kuka päätti" in html
    assert "Positiivinen = alijäämä" in html

    embedded = re.search(r'<script type="application/json" id="era-data">(.*?)</script>', html, re.S)
    assert embedded, "upotettu snapshot puuttuu"
    parsed = json.loads(embedded.group(1))
    assert parsed["meta"]["observed_months"] == meta["observed_months"]
    assert parsed["meta"]["missing_months"] == meta["missing_months"]
    assert len(parsed["eras"]) == len(eras)
    assert [era["cabinet_name"] for era in parsed["eras"]] == [era["cabinet_name"] for era in eras]

    print(f"Government fiscal era visualization OK ({len(eras)} kautta, "
          f"{meta['observed_months']} kuukautta, {len(meta['missing_months'])} puuttuvaa)")


if __name__ == "__main__":
    main()
