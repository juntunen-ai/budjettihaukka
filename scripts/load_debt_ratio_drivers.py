#!/usr/bin/env python3
"""Kokoaa velkasuhteen nousun ajurit ja kirjoittaa snapshotin tiedostoon
data/reference/debt_ratio_drivers_v1.json.

Kysymys: miksi velkasuhde nousi vuosina 2024-2025, vaikka talous kasvoi?
Aineisto vastaa kahdella tasolla.

1. Velkasuhteen muutoksen hajotelma vuosittain. Velanoton vaikutus ja
   nimellisen BKT:n vaikutus summautuvat muutokseen tarkalleen. Nimittajan
   apu riippuu nimellisen BKT:n kasvusta, jossa on mukana inflaatio.

2. Valtiontalouden erien muutos vuodesta 2023 vuoteen 2025. Molemmat vuodet
   ovat sote-uudistuksen jalkeisia, joten ne ovat vertailukelpoisia
   keskenaan; vuoteen 2022 ei voi verrata, koska uudistus siirsi noin 18
   miljardin menot ja tulot valtiolle kerralla. Rakennemuutos on kirjattu
   tauluun structural_events_v1 tunnuksella sote_uudistus_2023.

Meno- ja tuloerottelu kayttaa repon jaettua fiscal_side-saantoa. Korkomenot
erotellaan omaksi eraksi paaluokasta 36, koska ne ovat velan hinta eivatka
harkinnanvarainen meno.
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
from utils.budget_semantics import fiscal_side_case_sql

OUT = ROOT / "data" / "reference" / "debt_ratio_drivers_v1.json"
MAX_BYTES = 4_000_000_000
COMPARE_FROM, COMPARE_TO = 2023, 2025
INTEREST_PREFIX = "36."


def interest_sql(project: str, dataset: str, view: str) -> str:
    return f"""
SELECT
  EXTRACT(YEAR FROM period_date) AS year,
  SUM(nettokertyma) AS interest_eur
FROM `{project}.{dataset}.{view}`
WHERE has_valid_nettokertyma AND nettokertyma IS NOT NULL
  AND CAST(momentti_tunnusp AS STRING) LIKE '{INTEREST_PREFIX}%'
  AND LOWER(momentti_snimi) LIKE '%korko%'
GROUP BY year
ORDER BY year
""".strip()


def annual_sides_sql(project: str, dataset: str, view: str) -> str:
    side = fiscal_side_case_sql(
        code_expr="momentti_tunnusp", name_expr="momentti_snimi", hallinnonala_expr="hallinnonala"
    )
    return f"""
WITH v AS (
  SELECT EXTRACT(YEAR FROM period_date) AS year, {side} AS side, nettokertyma AS e
  FROM `{project}.{dataset}.{view}`
  WHERE has_valid_nettokertyma AND nettokertyma IS NOT NULL
)
SELECT year,
  SUM(IF(side = 'expense', e, 0)) AS expense_eur,
  SUM(IF(side = 'revenue', -e, 0)) AS revenue_eur
FROM v
GROUP BY year
ORDER BY year
""".strip()


def movers_sql(project: str, dataset: str, view: str) -> str:
    side = fiscal_side_case_sql(
        code_expr="momentti_tunnusp", name_expr="momentti_snimi", hallinnonala_expr="hallinnonala"
    )
    return f"""
WITH v AS (
  SELECT
    EXTRACT(YEAR FROM period_date) AS year,
    {side} AS side,
    CAST(momentti_tunnusp AS STRING) LIKE '{INTEREST_PREFIX}%' AS is_interest,
    hallinnonala_display AS ha,
    nettokertyma AS e
  FROM `{project}.{dataset}.{view}`
  WHERE has_valid_nettokertyma AND nettokertyma IS NOT NULL
    AND EXTRACT(YEAR FROM period_date) IN ({COMPARE_FROM}, {COMPARE_TO})
)
SELECT
  IF(is_interest, 'interest', side) AS bucket,
  IF(is_interest, 'Valtionvelan korot', ha) AS label,
  SUM(IF(year = {COMPARE_TO}, e, 0)) - SUM(IF(year = {COMPARE_FROM}, e, 0)) AS change_eur
FROM v
WHERE side IN ('expense', 'revenue')
GROUP BY bucket, label
ORDER BY ABS(change_eur) DESC
""".strip()


def _run(client: Any, sql: str) -> list[dict[str, Any]]:
    from google.cloud import bigquery

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=True, maximum_bytes_billed=MAX_BYTES),
    )
    return [dict(row) for row in job.result()]


def build_payload(
    *,
    relation: dict[str, Any],
    interest: list[dict[str, Any]],
    sides: list[dict[str, Any]],
    movers: list[dict[str, Any]],
) -> dict[str, Any]:
    years = [row for row in relation["years"] if row["year"] >= 2019]
    if not years:
        raise ValueError("Hajotelmasta ei loytynyt vuosia")

    interest_by_year = {row["year"]: float(row["interest_eur"]) for row in interest}
    sides_by_year = {
        row["year"]: (float(row["expense_eur"]), float(row["revenue_eur"])) for row in sides
    }

    mechanism = []
    for row in years:
        expense, revenue = sides_by_year.get(row["year"], (None, None))
        mechanism.append(
            {
                "year": row["year"],
                "gdp_volume_change_pct": row["gdp_volume_change_pct"],
                "gdp_nominal_change_pct": row["gdp_nominal_change_pct"],
                "debt_pct_gdp": row["debt_pct_gdp"],
                "debt_pct_gdp_change_pp": row["debt_pct_gdp_change_pp"],
                "borrowing_effect_pp": row["borrowing_effect_pp"],
                "denominator_effect_pp": row["denominator_effect_pp"],
                "interest_eur": interest_by_year.get(row["year"]),
                "expense_eur": expense,
                "revenue_eur": revenue,
                "budget_gap_eur": None if expense is None else expense - revenue,
            }
        )

    # Erien muutos: menoissa kasvu on positiivinen, tuloissa kaannetaan
    # merkki niin etta positiivinen tarkoittaa eron levenemista.
    drivers = []
    for row in movers:
        change = float(row["change_eur"])
        effect = change if row["bucket"] in ("expense", "interest") else change
        drivers.append(
            {
                "bucket": row["bucket"],
                "label": row["label"],
                "change_eur": change,
                # Positiivinen kasvattaa menojen ja tulojen eroa.
                "gap_effect_eur": effect,
            }
        )
    drivers.sort(key=lambda item: item["gap_effect_eur"], reverse=True)

    first, last = mechanism[0], mechanism[-1]
    gap_from = next(r for r in mechanism if r["year"] == COMPARE_FROM)["budget_gap_eur"]
    gap_to = next(r for r in mechanism if r["year"] == COMPARE_TO)["budget_gap_eur"]

    return {
        "meta": {
            "dataset_id": "debt_ratio_drivers_v1",
            "year_from": first["year"],
            "year_to": last["year"],
            "compare_from": COMPARE_FROM,
            "compare_to": COMPARE_TO,
            "budget_gap_from_eur": gap_from,
            "budget_gap_to_eur": gap_to,
            "structural_event": "sote_uudistus_2023",
            "structural_note": "vuoteen 2022 ei voi verrata, koska sote-uudistus siirsi "
                               "menot ja tulot valtiolle kerralla vuonna 2023",
            "sources": [
                {"source_id": "valtiokonttori_kuukausidata",
                 "label": "Valtiokonttori, valtion taloushallinnon kuukausidata"},
                {"source_id": "statfin_central_government_edp_debt_11yv",
                 "label": "Tilastokeskus, julkisyhteisöjen EDP-velka"},
                {"source_id": "statfin_national_accounts_15a9",
                 "label": "Tilastokeskus, kansantalouden tilinpito"},
            ],
        },
        "mechanism": mechanism,
        "drivers": drivers,
    }


def validate(payload: dict[str, Any]) -> None:
    mechanism = payload["mechanism"]
    drivers = payload["drivers"]
    meta = payload["meta"]

    if not mechanism or not drivers:
        raise ValueError("Aineisto on tyhja")

    for row in mechanism:
        total = row["borrowing_effect_pp"] + row["denominator_effect_pp"]
        if abs(total - row["debt_pct_gdp_change_pp"]) > 1e-6:
            raise ValueError(f"Hajotelma ei tasmaa vuonna {row['year']}")
        if row["interest_eur"] is None:
            raise ValueError(f"Korkomenot puuttuvat vuodelta {row['year']}")

    # Erien muutosten summan on vastattava budjettieron muutosta.
    total_effect = sum(item["gap_effect_eur"] for item in drivers)
    expected = meta["budget_gap_to_eur"] - meta["budget_gap_from_eur"]
    if abs(total_effect - expected) > 1_000_000:
        raise ValueError(
            f"Ajureiden summa {total_effect:.0f} ei vastaa eron muutosta {expected:.0f}"
        )

    if not any(item["label"] == "Valtionvelan korot" for item in drivers):
        raise ValueError("Korkoeraa ei eroteltu")


def main() -> int:
    parser = argparse.ArgumentParser(description="Kokoa velkasuhteen nousun ajurit.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--semantic-view", default=settings.table)
    args = parser.parse_args()

    relation_path = ROOT / "data" / "reference" / "growth_debt_relation_v1.json"
    relation = json.loads(relation_path.read_text(encoding="utf-8"))

    from google.cloud import bigquery

    client = bigquery.Client(project=args.project)
    interest = _run(client, interest_sql(args.project, args.dataset, args.semantic_view))
    sides = _run(client, annual_sides_sql(args.project, args.dataset, args.semantic_view))
    movers = _run(client, movers_sql(args.project, args.dataset, args.semantic_view))

    payload = build_payload(relation=relation, interest=interest, sides=sides, movers=movers)
    validate(payload)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    meta = payload["meta"]
    print(f"{OUT.relative_to(ROOT)}: {len(payload['mechanism'])} vuotta, "
          f"{len(payload['drivers'])} ajuria, budjettiero "
          f"{meta['budget_gap_from_eur'] / 1e9:.1f} -> {meta['budget_gap_to_eur'] / 1e9:.1f} mrd")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
