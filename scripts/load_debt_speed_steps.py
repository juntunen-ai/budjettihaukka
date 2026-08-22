#!/usr/bin/env python3
"""Kokoaa valtion velkaantumisen nopeuden askelmina ja kirjoittaa snapshotin
tiedostoon data/reference/debt_speed_steps_v1.json.

Nopeutta mitataan sillä, kuinka monta kuukautta kului kunkin 10 miljardin
euron lisäyksen kertymiseen. Lyhyt askelma tarkoittaa nopeaa velkaantumista.

Askelmat lasketaan kumulatiivisen sarjan pohjalta eikä tarkastelujakson
alusta. Sarja alkaa 1/2001, mutta valtio lyhensi velkaa vuoteen 2008 asti ja
kumulatiivinen kertymä painui 17,8 miljardia miinukselle. Jos askelmat
laskettaisiin vuodesta 2001, ensimmäinen niistä olisi 127 kuukautta ja
näyttäisi hitaalta velkaantumiselta, vaikka valtio tosiasiassa maksoi velkaa
pois suurimman osan siitä ajasta. Lyhennysvaihe raportoidaan siksi omana
jaksonaan.

Velan määritelmä tulee sellaisenaan tiedostosta load_party_debt_cumulative,
jotta samasta asiasta on vain yksi sääntö.
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
from scripts.load_party_debt_cumulative import MAX_BYTES, monthly_sql

OUT = ROOT / "data" / "reference" / "debt_speed_steps_v1.json"
STEP_EUR = 10_000_000_000


def _run(client: Any, sql: str) -> list[dict[str, Any]]:
    from google.cloud import bigquery

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=True, maximum_bytes_billed=MAX_BYTES),
    )
    return [dict(row) for row in job.result()]


def build_payload(monthly: list[dict]) -> dict[str, Any]:
    if not monthly:
        raise ValueError("Velkakuukausia ei saatu BigQuerystä")

    series = []
    running = 0.0
    for row in monthly:
        running += float(row["debt_change_eur"])
        series.append(
            {
                "month": row["month_start"].isoformat(),
                "cabinet_name": row["cabinet_name"],
                "pm_party_fi": row["pm_party_fi"],
                "cumulative_eur": running,
            }
        )

    trough_index = min(range(len(series)), key=lambda i: series[i]["cumulative_eur"])
    trough = series[trough_index]
    base = trough["cumulative_eur"]

    steps: list[dict[str, Any]] = []
    threshold = STEP_EUR
    previous_index = trough_index
    for index in range(trough_index, len(series)):
        while series[index]["cumulative_eur"] - base >= threshold:
            steps.append(
                {
                    "step_index": len(steps) + 1,
                    "from_eur": threshold - STEP_EUR,
                    "to_eur": threshold,
                    "reached_month": series[index]["month"],
                    "months": index - previous_index,
                    "cabinet_name": series[index]["cabinet_name"],
                    "pm_party_fi": series[index]["pm_party_fi"],
                    "is_complete": True,
                }
            )
            previous_index = index
            threshold += STEP_EUR

    # Kesken oleva askelma: kuinka pitkälle seuraavaa kymppimiljardia on
    # ehditty. Tätä ei saa verrata valmiisiin askelmiin.
    last = series[-1]
    pending_eur = (last["cumulative_eur"] - base) - (threshold - STEP_EUR)
    pending = {
        "from_eur": threshold - STEP_EUR,
        "to_eur": threshold,
        "accumulated_eur": pending_eur,
        "months_so_far": len(series) - 1 - previous_index,
        "cabinet_name": last["cabinet_name"],
        "pm_party_fi": last["pm_party_fi"],
        "is_complete": False,
    }

    return {
        "meta": {
            "dataset_id": "debt_speed_steps_v1",
            "measure": "kuukausia kutakin 10 miljardin euron velkalisäystä kohti",
            "step_eur": STEP_EUR,
            "first_month": series[0]["month"],
            "last_month": series[-1]["month"],
            "observed_months": len(series),
            "trough_month": trough["month"],
            "trough_cumulative_eur": base,
            "trough_cabinet_name": trough["cabinet_name"],
            "total_cumulative_eur": last["cumulative_eur"],
            "accumulated_from_trough_eur": last["cumulative_eur"] - base,
            "note": "askelmat lasketaan kumulatiivisen sarjan pohjalta, ei jakson alusta",
            "sources": [
                {
                    "source_id": "valtiokonttori_kuukausidata",
                    "label": "Valtiokonttori, valtion taloushallinnon kuukausidata",
                },
                {
                    "source_id": "valtioneuvosto_hallitukset",
                    "label": "Valtioneuvosto, hallitukset ja ministerit",
                },
            ],
        },
        # Jakso, jolloin velkaa lyhennettiin. Ilman tätä ensimmäinen askelma
        # näyttäisi hitaalta velkaantumiselta.
        "repayment_phase": {
            "from_month": series[0]["month"],
            "to_month": trough["month"],
            "months": trough_index + 1,
            "repaid_eur": -base,
            "cabinets": sorted({row["cabinet_name"] for row in series[: trough_index + 1]}),
        },
        "steps": steps,
        "pending_step": pending,
    }


def validate(payload: dict[str, Any]) -> None:
    meta = payload["meta"]
    steps = payload["steps"]
    phase = payload["repayment_phase"]

    if not steps:
        raise ValueError("Askelmia ei muodostunut")

    if [row["step_index"] for row in steps] != list(range(1, len(steps) + 1)):
        raise ValueError("Askelmien numerointi ei ole aukoton")

    months = [row["reached_month"] for row in steps]
    if months != sorted(months):
        raise ValueError("Askelmat eivät ole aikajärjestyksessä")

    for index, row in enumerate(steps):
        if row["to_eur"] - row["from_eur"] != meta["step_eur"]:
            raise ValueError(f"Askelma {row['step_index']} ei ole {meta['step_eur']} euroa")
        if row["from_eur"] != index * meta["step_eur"]:
            raise ValueError("Askelmien rajat eivät ole peräkkäiset")
        if row["months"] <= 0:
            raise ValueError(f"Askelmalle {row['step_index']} ei kertynyt kuukausia")

    # Askelmien ja lyhennysvaiheen kuukaudet eivät saa mennä päällekkäin
    # eivätkä ylittää havaittua sarjaa.
    total_months = phase["months"] + sum(row["months"] for row in steps)
    total_months += payload["pending_step"]["months_so_far"]
    if total_months != meta["observed_months"]:
        raise ValueError(
            f"Kuukausien summa {total_months} ei vastaa havaittua {meta['observed_months']}"
        )

    if meta["trough_cumulative_eur"] >= 0:
        raise ValueError("Kumulatiivisen pohjan pitäisi olla negatiivinen")
    if abs(phase["repaid_eur"] + meta["trough_cumulative_eur"]) > 1.0:
        raise ValueError("Lyhennetty määrä ei vastaa pohjaa")

    covered = len(steps) * meta["step_eur"] + payload["pending_step"]["accumulated_eur"]
    if abs(covered - meta["accumulated_from_trough_eur"]) > 1.0:
        raise ValueError("Askelmat eivät kata pohjalta kertynyttä määrää")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kokoa velkaantumisen nopeus askelmina.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--semantic-view", default=settings.table)
    args = parser.parse_args()

    from google.cloud import bigquery

    client = bigquery.Client(project=args.project)
    monthly = _run(client, monthly_sql(args.project, args.dataset, args.semantic_view))

    payload = build_payload(monthly)
    validate(payload)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    meta = payload["meta"]
    fastest = min(payload["steps"], key=lambda row: row["months"])
    slowest = max(payload["steps"], key=lambda row: row["months"])
    print(f"{OUT.relative_to(ROOT)}: {len(payload['steps'])} askelmaa, "
          f"pohja {meta['trough_month'][:7]} ({meta['trough_cumulative_eur'] / 1e9:.1f} mrd), "
          f"nopein {fastest['months']} kk, hitain {slowest['months']} kk")


if __name__ == "__main__":
    main()
