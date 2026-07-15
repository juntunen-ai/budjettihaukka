#!/usr/bin/env python3
"""Lataa valtion talousarvioesitysten avoimen datan (budjetti.vm.fi)
momenttitason määrärahat BigQueryyn.

Lähde: https://budjetti.vm.fi/indox/opendata/{vuosi}/tae/hallituksenEsitys/
       {vuosi}-tae-hallituksenEsitys-{NN}.csv
(CC-BY 4.0, ISO-8859-1, puolipiste-eroteltu; NN = osasto 11-15 tai
pääluokka 21-36; momenttitaso, kate 2014→.)

Tämä tuo puuttuvan metriikan: BUDJETOITU (talousarvioesityksen määräraha /
tuloarvio) toteuman rinnalle. Tuottaa:
- BQ-taulu talousarvio_v1 (vuosi, puoli, koodit, nimet, maararaha_eur)
- näkymä budget_vs_actual_v1 (TAE vs toteuma momenttitasolla)
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import time
from pathlib import Path

import requests
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

BASE = "https://budjetti.vm.fi/indox/opendata"
SECTIONS = [str(n) for n in range(11, 16)] + [str(n) for n in range(21, 37)]


def _get_with_retry(session: requests.Session, url: str) -> requests.Response | None:
    for attempt in range(4):
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None  # sektio ei ole olemassa tälle vuodelle
        except requests.RequestException:
            pass
        time.sleep(1.5 * (attempt + 1))
    print(f"  VAROITUS: {url.rsplit('/', 1)[-1]} epäonnistui uudelleenyritystenkin jälkeen")
    return None


def fetch_year(session: requests.Session, year: int, doc: str = "tae") -> list[dict]:
    rows: list[dict] = []
    for section in SECTIONS:
        url = f"{BASE}/{year}/{doc}/hallituksenEsitys/{year}-{doc}-hallituksenEsitys-{section}.csv"
        resp = _get_with_retry(session, url)
        if resp is None:
            continue
        text = resp.content.decode("iso-8859-1")
        reader = csv.reader(io.StringIO(text), delimiter=";")
        header = next(reader, None)
        if not header:
            continue
        is_revenue = "Osaston numero" in header[0]
        for record in reader:
            if len(record) < 8:
                continue
            top_code, top_name, luku_code, luku_name, mom_code, mom_name = (
                record[0].strip(), record[1].strip(), record[2].strip(),
                record[3].strip(), record[4].strip(), record[5].strip(),
            )
            amount_raw = (record[7] or "").strip()
            if not (top_code.isdigit() and luku_code.isdigit() and mom_code.isdigit()):
                continue
            try:
                amount = float(amount_raw.replace(" ", "").replace("\xa0", "").replace(",", "."))
            except ValueError:
                continue
            rows.append(
                {
                    "vuosi": year,
                    "dokumentti": doc,
                    "puoli": "tulo" if is_revenue else "meno",
                    "paaluokka_osasto": top_code,
                    "paaluokka_osasto_nimi": top_name,
                    "luku": luku_code,
                    "momentti_koodi": f"{top_code}.{luku_code.zfill(2)}.{mom_code.zfill(2)}.",
                    "momentti_nimi": mom_name,
                    "maararaha_eur": amount,
                }
            )
        time.sleep(0.5)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Lataa TAE-avoin data BigQueryyn.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--year-from", type=int, default=2014)
    parser.add_argument("--year-to", type=int, default=2026)
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": "budjettihaukka-ingest/1.0"})

    docs = ["tae"] + [f"ltae{n}" for n in range(1, 8)]
    all_rows: list[dict] = []
    for year in range(args.year_from, args.year_to + 1):
        for doc in docs:
            year_rows = fetch_year(session, year, doc)
            if not year_rows and doc != "tae":
                break  # lisätalousarviot ovat juoksevasti numeroituja
            all_rows.extend(year_rows)
            if year_rows:
                print(f"{doc.upper()} {year}: {len(year_rows)} momenttiriviä")

    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.talousarvio_v1"
    client.load_table_from_json(
        all_rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("vuosi", "INT64"),
                bigquery.SchemaField("dokumentti", "STRING"),
                bigquery.SchemaField("puoli", "STRING"),
                bigquery.SchemaField("paaluokka_osasto", "STRING"),
                bigquery.SchemaField("paaluokka_osasto_nimi", "STRING"),
                bigquery.SchemaField("luku", "STRING"),
                bigquery.SchemaField("momentti_koodi", "STRING"),
                bigquery.SchemaField("momentti_nimi", "STRING"),
                bigquery.SchemaField("maararaha_eur", "FLOAT64"),
            ],
        ),
    ).result()
    print(f"BQ-taulu -> {table_id} ({len(all_rows)} riviä)")

    view_sql = f"""
CREATE OR REPLACE VIEW `{args.project}.{args.dataset}.budget_vs_actual_v1` AS
WITH budjetoitu AS (
  SELECT vuosi, momentti_koodi, ANY_VALUE(puoli) AS puoli,
         ANY_VALUE(IF(dokumentti = 'tae', momentti_nimi, NULL)) AS tae_nimi,
         SUM(IF(dokumentti = 'tae', maararaha_eur, 0)) AS tae_eur,
         SUM(IF(dokumentti != 'tae', maararaha_eur, 0)) AS ltae_eur,
         SUM(maararaha_eur) AS budjetoitu_eur
  FROM `{table_id}`
  GROUP BY vuosi, momentti_koodi
),
toteuma AS (
  SELECT vuosi, momentti_tunnusp AS momentti_koodi,
         ANY_VALUE(momentti_snimi) AS momentti_nimi,
         SUM(nettokertyma_sum) AS toteuma_eur
  FROM `{args.project}.{args.dataset}.valtiontalous_yearly_agg_v1`
  GROUP BY vuosi, momentti_tunnusp
)
SELECT
  COALESCE(b.vuosi, t.vuosi) AS vuosi,
  COALESCE(b.momentti_koodi, t.momentti_koodi) AS momentti_koodi,
  COALESCE(b.tae_nimi, t.momentti_nimi) AS momentti_nimi,
  b.puoli,
  b.tae_eur,
  b.ltae_eur,
  b.budjetoitu_eur,
  t.toteuma_eur,
  SAFE_DIVIDE(t.toteuma_eur, NULLIF(b.budjetoitu_eur, 0)) AS toteuma_aste
FROM budjetoitu b
FULL OUTER JOIN toteuma t USING (vuosi, momentti_koodi)
WHERE COALESCE(b.vuosi, t.vuosi) >= 2014
"""
    client.query(view_sql).result()
    print(f"Näkymä -> {args.project}.{args.dataset}.budget_vs_actual_v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
