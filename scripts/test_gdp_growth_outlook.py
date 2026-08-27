#!/usr/bin/env python3
"""Regressiotestit Suomen talouskasvun monilähdeaineistolle.

Testi ei ota verkkoyhteyttä eikä BigQueryyn. Se lukee committoidun
snapshotin ja ajaa saman sopimusvalidoinnin kuin lataaja.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_gdp_growth_outlook import CSV_FIELDS, OUT, validate

# Vuodet, joista kaikki neljä lähdettä ovat käytännössä samaa mieltä.
# Jos yhteisymmärrys katoaa, jokin sarja on vaihtunut toiseen mittariin.
CONSENSUS_YEARS = {2022: 0.8, 2023: -1.3}
CONSENSUS_TOLERANCE = 0.15

EXPECTED_INSTITUTIONS = {"Tilastokeskus", "Suomen Pankki", "OECD", "IMF"}


def read_rows() -> list[dict]:
    with OUT.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == CSV_FIELDS, reader.fieldnames
        rows = []
        for row in reader:
            rows.append(
                {
                    **row,
                    "year": int(row["year"]),
                    "gdp_volume_change_pct": float(row["gdp_volume_change_pct"]),
                    "is_national_official": row["is_national_official"] == "True",
                    "is_beyond_national_actual": row["is_beyond_national_actual"] == "True",
                }
            )
    return rows


def main() -> None:
    rows = read_rows()
    validate(rows)

    by_institution: dict[str, list[dict]] = {}
    for row in rows:
        by_institution.setdefault(row["institution"], []).append(row)
    assert set(by_institution) == EXPECTED_INSTITUTIONS, set(by_institution)

    # Jokaisella lähteellä on oltava riittävä kattavuus, jotta vertailu on
    # mahdollinen. Tilastokeskus kattaa historian, muut ulottuvat eteenpäin.
    statfin = sorted(by_institution["Tilastokeskus"], key=lambda row: row["year"])
    assert len(statfin) >= 40, len(statfin)
    assert statfin[-1]["year"] >= 2025
    assert all(row["is_national_official"] for row in statfin)

    latest_actual = statfin[-1]["year"]
    for row in rows:
        assert row["is_beyond_national_actual"] == (row["year"] > latest_actual), row

    # Ennustelähteet ulottuvat toteutuneen tilinpidon yli, muuten ne eivät
    # tuo mitään lisää Tilastokeskukseen nähden.
    for name in ("Suomen Pankki", "OECD", "IMF"):
        forward = [row for row in by_institution[name] if row["year"] > latest_actual]
        assert forward, f"{name}: ei yhtään ennustevuotta"

    # Suomen Pankin luvut on kirjattu käsin, joten jokaisella on oltava
    # julkaisupäivä ja osoite, ja vintageja on oltava vähintään kaksi jotta
    # ennusteen tarkistuminen näkyy.
    bof = by_institution["Suomen Pankki"]
    vintages = {row["vintage_date"] for row in bof}
    assert len(vintages) >= 2, vintages
    for row in bof:
        assert row["source_url"].startswith("https://www.suomenpankki.fi/"), row
        assert row["vintage_date"] < "2027-01-01"

    # Yhteisymmärrysvuodet: kaikki neljä lähdettä lähellä toisiaan.
    for year, expected in CONSENSUS_YEARS.items():
        values = [row["gdp_volume_change_pct"] for row in rows if row["year"] == year]
        assert len(values) >= 3, f"{year}: liian harva lähde"
        for value in values:
            assert abs(value - expected) <= CONSENSUS_TOLERANCE, (year, value)

    # Mittari on volyymin muutos prosentteina, ei indeksi eikä taso.
    for row in rows:
        assert -20 < row["gdp_volume_change_pct"] < 20, row

    # Sama lähde ja vintage ei anna samalle vuodelle kahta lukua.
    keys = {(row["source_id"], row["vintage_date"], row["year"]) for row in rows}
    assert len(keys) == len(rows)

    print(f"GDP growth outlook OK ({len(rows)} riviä, "
          f"{len(EXPECTED_INSTITUTIONS)} lähdettä, "
          f"kansallinen toteuma {statfin[0]['year']}-{latest_actual})")


if __name__ == "__main__":
    main()
