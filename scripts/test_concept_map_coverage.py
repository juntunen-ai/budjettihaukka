#!/usr/bin/env python3
"""Tarkistaa, ettei käsitekarttojen kausirajauksiin jää aukkoja.

Jokaiselle säännölle verrataan year_from/year_to siihen, miltä vuosilta
kyseisellä koodilla on oikeasti dataa. Jos rajauksen ULKOPUOLELLA on
merkittävästi rahaa (yli --threshold M€ vuodessa), se on todennäköinen
aukko: kausiraja on asetettu väärin ja rahaa jää käsitteen ulkopuolelle
huomaamatta.

Taustaa: sosiaaliturva-kartan 33.10 sai year_from 2010, koska rakenne-
kysely ryhmitteli nimikausittain ja suodatti näytevuosilla — 2008-2009
kausi jäi näkymättä ja 4,0 mrd € putosi pois. Tämä testi estää saman
virheen jatkossa.

Vaatii BigQuery-yhteyden. Aja karttamuutosten jälkeen.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

MAP_DIR = ROOT / "data" / "ontology" / "concept_code_map"


def main() -> int:
    parser = argparse.ArgumentParser(description="Tarkista käsitekarttojen kausiaukot.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--threshold", type=float, default=50.0, help="M€/vuosi jonka yli aukko raportoidaan")
    args = parser.parse_args()

    rules = []
    for path in sorted(MAP_DIR.glob("*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        for rule in doc.get("rules", []):
            # intentional_gap: koodi tarkoitti eri asiaa toisella kaudella
            # (esim. 29.90 = Taide ja kulttuuri →2008, Liikuntatoimi 2007→).
            if rule["role"] in ("include", "component") and not rule.get("intentional_gap"):
                rules.append((doc["concept"], rule))
    if not rules:
        print("Ei sääntöjä.")
        return 0

    client = bigquery.Client(project=args.project)
    findings: list[str] = []

    for concept, rule in rules:
        code, level = rule["code"], rule["level"]
        year_from = rule.get("year_from") or 0
        year_to = rule.get("year_to") or 9999
        match = (
            f"momentti_tunnusp = '{code}'"
            if level == "momentti"
            else f"STARTS_WITH(momentti_tunnusp, '{code}')"
        )
        sql = f"""
        SELECT vuosi, ROUND(SUM(nettokertyma)/1e6, 1) meur
        FROM `{args.project}.{args.dataset}.valtiontalous_curated_dq_v`
        WHERE {match} AND (vuosi < {year_from} OR vuosi > {year_to})
        GROUP BY vuosi HAVING ABS(meur) > {args.threshold} ORDER BY vuosi
        """
        outside = list(client.query(sql).result())
        if outside:
            years = ", ".join(f"{r.vuosi}: {r.meur:.0f} M€" for r in outside[:6])
            total = sum(abs(r.meur) for r in outside)
            findings.append(
                f"  {concept} / {code} (rajaus {year_from}-{year_to if year_to != 9999 else '→'}): "
                f"rajauksen ulkopuolella {total:.0f} M€ — {years}"
            )

    print(f"Tarkistettu {len(rules)} include/component-sääntöä, kynnys {args.threshold} M€/v")
    if findings:
        print(f"\nMAHDOLLISIA KAUSIAUKKOJA ({len(findings)}):")
        for finding in findings:
            print(finding)
        print(
            "\nHuom: osumat voivat olla tarkoituksellisia (koodi tarkoitti eri asiaa toisella "
            "kaudella). Tarkista jokainen ja korjaa kausiraja tai kirjaa syy sääntöön."
        )
        return 1
    print("Ei kausiaukkoja.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
