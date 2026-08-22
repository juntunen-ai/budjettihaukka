#!/usr/bin/env python3
"""Kokoaa Suomen talouskasvun ja valtionvelan suhteen 2001-2025 ja kirjoittaa
snapshotin tiedostoon data/reference/growth_debt_relation_v1.json.

Velka on valtionhallinnon EDP-velkakanta vuoden lopussa, ei budjetin kautta
kulkeva nettovelanotto. Velkasuhde on tämä kanta suhteessa saman vuoden
bruttokansantuotteeseen käyvin hinnoin.

Velkasuhteen vuosimuutos hajotetaan kahteen osaan, jotka summautuvat siihen
tarkalleen:

    D_t/Y_t - D_(t-1)/Y_(t-1)
        = (D_t - D_(t-1)) / Y_t          <- velanoton vaikutus
        + D_(t-1) * (1/Y_t - 1/Y_(t-1))  <- nimellisen BKT:n vaikutus

Nimittäjän liike seuraa nimellistä BKT:tä, joten siinä on mukana myös
inflaatio. Volyymin muutos raportoidaan rinnalla omana sarakkeenaan, koska
se on se luku, jota talouskasvulla tarkoitetaan, mutta se ei ole sama asia
kuin nimittäjävaikutus.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

OUT = ROOT / "data" / "reference" / "growth_debt_relation_v1.json"
YEAR_FROM, YEAR_TO = 2001, 2025
MAX_BYTES = 500_000_000


def macro_sql(project: str, dataset: str) -> str:
    return f"""
SELECT
  year,
  MAX(IF(series_id = 'central_government_edp_debt_q4_meur', value, NULL)) AS debt_meur,
  MAX(IF(series_id = 'gdp_current_prices_meur', value, NULL)) AS gdp_meur,
  MAX(IF(series_id = 'gdp_volume_change_pct', value, NULL)) AS gdp_volume_change_pct
FROM `{project}.{dataset}.official_macro_reference_v1`
WHERE year BETWEEN {YEAR_FROM - 1} AND {YEAR_TO}
GROUP BY year
ORDER BY year
""".strip()


def _run(client: Any, sql: str) -> list[dict[str, Any]]:
    from google.cloud import bigquery

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=True, maximum_bytes_billed=MAX_BYTES),
    )
    return [dict(row) for row in job.result()]


def build_payload(macro: list[dict[str, Any]]) -> dict[str, Any]:
    usable = [
        row for row in macro
        if row["debt_meur"] is not None and row["gdp_meur"] is not None
    ]
    if len(usable) < 3:
        raise ValueError("Velka- tai BKT-sarja ei kata tarkastelujaksoa")

    years: list[dict[str, Any]] = []
    for previous, current in zip(usable, usable[1:]):
        if current["year"] < YEAR_FROM or current["year"] > YEAR_TO:
            continue
        debt, gdp = current["debt_meur"], current["gdp_meur"]
        debt0, gdp0 = previous["debt_meur"], previous["gdp_meur"]
        ratio = 100 * debt / gdp
        ratio0 = 100 * debt0 / gdp0
        borrowing = 100 * (debt - debt0) / gdp
        denominator = 100 * debt0 * (1 / gdp - 1 / gdp0)
        years.append(
            {
                "year": current["year"],
                "gdp_volume_change_pct": current["gdp_volume_change_pct"],
                "gdp_nominal_change_pct": 100 * (gdp / gdp0 - 1),
                "debt_meur": debt,
                "gdp_meur": gdp,
                "debt_pct_gdp": ratio,
                "debt_pct_gdp_change_pp": ratio - ratio0,
                "borrowing_effect_pp": borrowing,
                "denominator_effect_pp": denominator,
                # Nelikentta: kumpaan suuntaan kasvu ja velkasuhde liikkuivat.
                "quadrant": (
                    ("growth_up" if (current["gdp_volume_change_pct"] or 0) >= 0 else "growth_down")
                    + "_"
                    + ("ratio_up" if ratio - ratio0 >= 0 else "ratio_down")
                ),
            }
        )

    if not years:
        raise ValueError("Yhtään vuotta ei muodostunut")

    first, last = years[0], years[-1]
    return {
        "meta": {
            "dataset_id": "growth_debt_relation_v1",
            "year_from": first["year"],
            "year_to": last["year"],
            "debt_measure": "valtionhallinnon EDP-velkakanta vuoden lopussa, S1311",
            "growth_measure": "bruttokansantuotteen volyymin muutos, %",
            "decomposition_note": "velanoton ja nimellisen BKT:n vaikutus summautuvat "
                                  "velkasuhteen muutokseen tarkalleen",
            "debt_pct_gdp_first": first["debt_pct_gdp"],
            "debt_pct_gdp_last": last["debt_pct_gdp"],
            "sources": [
                {
                    "source_id": "statfin_central_government_edp_debt_11yv",
                    "label": "Tilastokeskus, julkisyhteisöjen EDP-velka",
                },
                {
                    "source_id": "statfin_national_accounts_15a9",
                    "label": "Tilastokeskus, kansantalouden tilinpito",
                },
            ],
        },
        "years": years,
    }


def validate(payload: dict[str, Any]) -> None:
    years = payload["years"]
    meta = payload["meta"]

    if not years:
        raise ValueError("Vuosia ei ole")
    if [row["year"] for row in years] != sorted(row["year"] for row in years):
        raise ValueError("Vuodet eivät ole järjestyksessä")
    if len({row["year"] for row in years}) != len(years):
        raise ValueError("Sama vuosi esiintyy useasti")

    for row in years:
        # Hajotelman on summauduttava muutokseen tarkalleen. Jos tämä pettää,
        # kuvan selitys velkasuhteen noususta on vaarassa olla vaarin.
        total = row["borrowing_effect_pp"] + row["denominator_effect_pp"]
        if abs(total - row["debt_pct_gdp_change_pp"]) > 1e-6:
            raise ValueError(f"Hajotelma ei täsmää vuonna {row['year']}")
        if not 0 < row["debt_pct_gdp"] < 200:
            raise ValueError(f"Epäuskottava velkasuhde vuonna {row['year']}")
        if row["gdp_volume_change_pct"] is None:
            raise ValueError(f"Volyymin muutos puuttuu vuodelta {row['year']}")

    if years[0]["year"] != meta["year_from"] or years[-1]["year"] != meta["year_to"]:
        raise ValueError("Metatiedon vuosirajat eivät vastaa aineistoa")


def main() -> int:
    parser = argparse.ArgumentParser(description="Kokoa talouskasvun ja valtionvelan suhde.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    args = parser.parse_args()

    from google.cloud import bigquery

    client = bigquery.Client(project=args.project)
    macro = _run(client, macro_sql(args.project, args.dataset))

    payload = build_payload(macro)
    validate(payload)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    meta = payload["meta"]
    print(f"{OUT.relative_to(ROOT)}: {len(payload['years'])} vuotta "
          f"({meta['year_from']}-{meta['year_to']}), velkasuhde "
          f"{meta['debt_pct_gdp_first']:.1f} % -> {meta['debt_pct_gdp_last']:.1f} %")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
