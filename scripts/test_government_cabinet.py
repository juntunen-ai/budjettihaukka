#!/usr/bin/env python3
"""Regressiotestit Suomen hallitusten referenssiaineistolle.

Testi ei ota verkkoyhteyttä eikä BigQueryyn: se lukee committoidun
snapshotin ja ajaa saman validoinnin kuin lataaja, jotta aineiston
sopimus ei rikkoudu huomaamatta.
"""

from __future__ import annotations

import csv
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.load_government_cabinet import CSV_PATH, FIELDS, validate

# Kiintopisteet, jotka eivät muutu ilman että lähdeaineisto on muuttunut.
FIRST_CABINET = "Svinhufvud"
FIRST_START = dt.date(1917, 11, 27)
KNOWN_CABINETS = {
    "Lipponen": (dt.date(1995, 4, 13), dt.date(1999, 4, 15)),
    "Sipilä": (dt.date(2015, 5, 29), dt.date(2019, 6, 6)),
    "Rinne": (dt.date(2019, 6, 6), dt.date(2019, 12, 10)),
    "Marin": (dt.date(2019, 12, 10), dt.date(2023, 6, 20)),
}


def read_rows() -> list[dict]:
    with CSV_PATH.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    parsed = []
    for row in rows:
        parsed.append(
            {
                "cabinet_ordinal": int(row["cabinet_ordinal"]),
                "cabinet_id": row["cabinet_id"],
                "cabinet_name": row["cabinet_name"],
                "prime_minister_surname": row["prime_minister_surname"],
                "start_date": dt.date.fromisoformat(row["start_date"]),
                "end_date": dt.date.fromisoformat(row["end_date"]) if row["end_date"] else None,
                "is_incumbent": row["is_incumbent"] == "True",
                "days_in_office_source": int(row["days_in_office_source"]),
                "days_in_office_computed": (
                    int(row["days_in_office_computed"]) if row["days_in_office_computed"] else None
                ),
                "pm_party_fi": row["pm_party_fi"],
                "cabinet_type_fi": row["cabinet_type_fi"],
                "source_id": row["source_id"],
                "source_url": row["source_url"],
                "vintage_date": dt.date.fromisoformat(row["vintage_date"]),
            }
        )
    return parsed


def main() -> None:
    with CSV_PATH.open(encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle))
    assert header == FIELDS, f"sarakejärjestys muuttunut: {header}"

    cabinets = read_rows()
    assert len(cabinets) >= 77, f"hallituksia odotettua vähemmän: {len(cabinets)}"

    # Sama sopimusvalidointi kuin lataajassa: yksilölliset tunnisteet,
    # tasan yksi istuva hallitus, katkeamaton ketju ja täsmäävät päivät.
    validate(cabinets)

    by_name = {row["cabinet_name"]: row for row in cabinets}
    assert cabinets[0]["cabinet_name"] == FIRST_CABINET
    assert cabinets[0]["start_date"] == FIRST_START
    for name, (start, end) in KNOWN_CABINETS.items():
        row = by_name[name]
        assert row["start_date"] == start, f"{name} alkupäivä {row['start_date']} != {start}"
        assert row["end_date"] == end, f"{name} loppupäivä {row['end_date']} != {end}"

    # Ordinaalit ovat aukoton 1..N ja seuraavat aikajärjestystä.
    assert [row["cabinet_ordinal"] for row in cabinets] == list(range(1, len(cabinets) + 1))
    assert cabinets == sorted(cabinets, key=lambda row: row["start_date"])

    # Istuvan hallituksen hallituspäivät on lähteen elävä laskuri, joten se
    # vastaa vintage_datea. Sallitaan vuorokauden heitto aikavyöhykkeille.
    incumbent = cabinets[-1]
    expected_days = (incumbent["vintage_date"] - incumbent["start_date"]).days + 1
    assert abs(incumbent["days_in_office_source"] - expected_days) <= 1, (
        f"istuvan hallituksen päivät {incumbent['days_in_office_source']} "
        f"eivät vastaa vintage_datea (odotettu ~{expected_days})"
    )

    # Arvojoukot pysyvät lähteen sanastossa.
    assert {row["cabinet_type_fi"] for row in cabinets} <= {"Enemmistö", "Vähemmistö", "Virkamies"}
    assert all(row["pm_party_fi"] for row in cabinets)
    assert all(row["source_id"] == "valtioneuvosto_hallitukset" for row in cabinets)
    assert all(row["source_url"].startswith("https://valtioneuvosto.fi/") for row in cabinets)

    # Aineisto kattaa koko budjettidatan aikavälin 1998-.
    assert cabinets[0]["start_date"].year <= 1998
    assert incumbent["is_incumbent"] and incumbent["end_date"] is None

    print(f"Government cabinet reference OK ({len(cabinets)} hallitusta, "
          f"{cabinets[0]['start_date']}-)")


if __name__ == "__main__":
    main()
