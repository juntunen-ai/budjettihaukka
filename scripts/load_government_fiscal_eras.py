#!/usr/bin/env python3
"""Kokoaa valtion kuukausitason toteuman hallituskausittain ja kirjoittaa
snapshotin tiedostoon data/reference/government_fiscal_eras_v1.json.

Kohdennussääntö: jokainen kuukausi kuuluu sille hallitukselle, joka piti
valtaa suurimman osan kyseisen kuukauden päivistä. Vaihtokuukausi menee siis
kokonaan enemmistön haltijalle eikä sitä pilkota. Vaihtoehto olisi jakaa
vuosisumma päiväosuuksilla, mutta se olettaisi menojen jakautuvan tasaisesti
vuoden sisällä, mikä ei pidä paikkaansa.

Reaalisarja käyttää elinkustannusindeksiä (cpi_general_purchasing_power)
viimeisimpään täyteen vuoteen normalisoituna. Nimellinen ja reaalinen ovat
datasopimuksen mukaan eri mittareita, joten molemmat kirjoitetaan erikseen.
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

OUT = ROOT / "data" / "reference" / "government_fiscal_eras_v1.json"
MAX_BYTES = 4_000_000_000


def _attribution_cte(project: str, dataset: str, semantic_view: str) -> str:
    """Kuukausi -> hallitus enemmistösäännöllä, puoliavoin väli [start, end)."""
    return f"""
months AS (
  -- Kolmelta kuukaudelta (2017-03, 2019-03, 2023-03) nettokertyma puuttuu jo
  -- raakakerroksesta, vaikka has_valid_nettokertyma on TRUE. Datasopimuksen
  -- saannon 5 mukaan NULL sailyy puuttuvana eika muutu nollaksi, joten nama
  -- kuukaudet eivat ole havaittuja lainkaan.
  SELECT period_date AS month_start
  FROM `{project}.{dataset}.{semantic_view}`
  WHERE has_valid_nettokertyma AND nettokertyma IS NOT NULL
  GROUP BY period_date
),
overlap AS (
  SELECT
    m.month_start,
    g.cabinet_ordinal,
    g.cabinet_name,
    g.pm_party_fi,
    g.cabinet_type_fi,
    g.start_date,
    g.end_date,
    DATE_DIFF(
      LEAST(COALESCE(g.end_date, CURRENT_DATE()), DATE_ADD(m.month_start, INTERVAL 1 MONTH)),
      GREATEST(g.start_date, m.month_start),
      DAY
    ) AS overlap_days
  FROM months m
  CROSS JOIN `{project}.{dataset}.government_cabinet_v1` g
),
attribution AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      overlap.*,
      ROW_NUMBER() OVER (
        PARTITION BY month_start ORDER BY overlap_days DESC, cabinet_ordinal
      ) AS rn
    FROM overlap
    WHERE overlap_days > 0
  )
  WHERE rn = 1
)"""


def era_sql(project: str, dataset: str, semantic_view: str) -> str:
    fiscal_side = fiscal_side_case_sql(
        code_expr="s.momentti_tunnusp",
        name_expr="s.momentti_snimi",
        hallinnonala_expr="s.hallinnonala",
    )
    return f"""
WITH {_attribution_cte(project, dataset, semantic_view)},
deflator AS (
  SELECT year, index_value
  FROM `{project}.{dataset}.official_deflator_reference_v1`
  WHERE deflator_id = 'cpi_general_purchasing_power'
),
base_year AS (
  SELECT MAX(year) AS year FROM deflator
),
monthly AS (
  SELECT
    s.period_date AS month_start,
    {fiscal_side} AS fiscal_side,
    SUM(s.nettokertyma) AS net_eur
  FROM `{project}.{dataset}.{semantic_view}` s
  WHERE s.has_valid_nettokertyma AND s.nettokertyma IS NOT NULL
  GROUP BY month_start, fiscal_side
)
SELECT
  a.cabinet_ordinal,
  a.cabinet_name,
  a.pm_party_fi,
  a.cabinet_type_fi,
  a.start_date,
  a.end_date,
  COUNT(DISTINCT a.month_start) AS observed_months,
  MIN(a.month_start) AS first_observed_month,
  MAX(a.month_start) AS last_observed_month,
  SUM(m.net_eur) AS net_nominal_eur,
  SUM(IF(m.fiscal_side = 'expense', m.net_eur, 0)) AS expense_nominal_eur,
  SUM(IF(m.fiscal_side = 'revenue', m.net_eur, 0)) AS revenue_nominal_eur,
  SUM(IF(m.fiscal_side = 'financing', m.net_eur, 0)) AS financing_nominal_eur,
  SUM(IF(m.fiscal_side = 'technical', m.net_eur, 0)) AS technical_nominal_eur,
  -- Deflaattori kattaa vain vuodet, joille indeksi on julkaistu. Reaalisumma
  -- ja sen rinnalla raportoitava nimellinen lasketaan tasan samoista
  -- kuukausista, jotta vertailu ei vuoda kattamattomien vuosien yli.
  SUM(
    IF(dm.index_value IS NULL, NULL,
       m.net_eur * SAFE_DIVIDE(
         (SELECT d.index_value FROM deflator d WHERE d.year = (SELECT year FROM base_year)),
         dm.index_value))
  ) AS net_real_eur,
  SUM(IF(dm.index_value IS NULL, NULL, m.net_eur)) AS net_nominal_deflated_scope_eur,
  COUNT(DISTINCT IF(dm.index_value IS NULL, NULL, a.month_start)) AS deflated_months
FROM attribution a
JOIN monthly m USING (month_start)
LEFT JOIN deflator dm ON dm.year = EXTRACT(YEAR FROM a.month_start)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY a.cabinet_ordinal
""".strip()


def monthly_sql(project: str, dataset: str, semantic_view: str) -> str:
    return f"""
WITH {_attribution_cte(project, dataset, semantic_view)},
monthly AS (
  SELECT period_date AS month_start, SUM(nettokertyma) AS net_eur
  FROM `{project}.{dataset}.{semantic_view}`
  WHERE has_valid_nettokertyma AND nettokertyma IS NOT NULL
  GROUP BY month_start
)
SELECT
  m.month_start,
  a.cabinet_ordinal,
  a.cabinet_name,
  m.net_eur
FROM monthly m
JOIN attribution a USING (month_start)
ORDER BY m.month_start
""".strip()


def missing_months_sql(project: str, dataset: str, semantic_view: str) -> str:
    """Kuukaudet, joilla on rivejä mutta ei yhtään nettokertymän arvoa."""
    return f"""
SELECT period_date AS month_start, COUNT(*) AS row_count
FROM `{project}.{dataset}.{semantic_view}`
WHERE has_valid_nettokertyma
GROUP BY month_start
HAVING COUNTIF(nettokertyma IS NOT NULL) = 0
ORDER BY month_start
""".strip()


def base_year_sql(project: str, dataset: str) -> str:
    return f"""
SELECT MAX(year) AS base_year
FROM `{project}.{dataset}.official_deflator_reference_v1`
WHERE deflator_id = 'cpi_general_purchasing_power'
""".strip()


def _run(client: Any, sql: str) -> list[dict[str, Any]]:
    from google.cloud import bigquery

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            use_query_cache=True, maximum_bytes_billed=MAX_BYTES
        ),
    )
    return [dict(row) for row in job.result()]


def build_payload(eras: list[dict], monthly: list[dict], missing: list[dict],
                  base_year: int) -> dict[str, Any]:
    if not eras:
        raise ValueError("Hallituskausia ei saatu BigQuerystä")

    first_month = min(row["month_start"] for row in monthly)
    last_month = max(row["month_start"] for row in monthly)

    era_rows = []
    for row in eras:
        starts_before_data = row["start_date"] < first_month
        ongoing = row["end_date"] is None
        era_rows.append(
            {
                "cabinet_ordinal": row["cabinet_ordinal"],
                "cabinet_name": row["cabinet_name"],
                "pm_party_fi": row["pm_party_fi"],
                "cabinet_type_fi": row["cabinet_type_fi"],
                "start_date": row["start_date"].isoformat(),
                "end_date": row["end_date"].isoformat() if row["end_date"] else None,
                "observed_months": row["observed_months"],
                "first_observed_month": row["first_observed_month"].isoformat(),
                "last_observed_month": row["last_observed_month"].isoformat(),
                "net_nominal_eur": float(row["net_nominal_eur"]),
                "net_real_eur": float(row["net_real_eur"]),
                "net_nominal_deflated_scope_eur": float(row["net_nominal_deflated_scope_eur"]),
                "deflated_months": row["deflated_months"],
                "expense_nominal_eur": float(row["expense_nominal_eur"]),
                "revenue_nominal_eur": float(row["revenue_nominal_eur"]),
                "financing_nominal_eur": float(row["financing_nominal_eur"]),
                "technical_nominal_eur": float(row["technical_nominal_eur"]),
                # Kausi on katkaistu, jos hallitus aloitti ennen kuukausidatan
                # alkua tai on yhä vallassa. Naitä ei saa verrata täysiin kausiin.
                "is_truncated_start": starts_before_data,
                "is_ongoing": ongoing,
                "is_complete_term": not starts_before_data and not ongoing,
            }
        )

    return {
        "meta": {
            "dataset_id": "government_fiscal_eras_v1",
            "attribution_rule": "kuukausi kuuluu hallitukselle, joka piti valtaa "
                                "suurimman osan kuukauden päivistä",
            "first_observed_month": first_month.isoformat(),
            "last_observed_month": last_month.isoformat(),
            "observed_months": len(monthly),
            # Kuukaudet, joilla on rivejä mutta ei yhtään nettokertymän arvoa
            # jo raakakerroksessa. Nämä eivät ole nollia vaan puuttuvia.
            "missing_months": [row["month_start"].isoformat() for row in missing],
            "real_base_year": base_year,
            "real_deflator_id": "cpi_general_purchasing_power",
            "sources": [
                {
                    "source_id": "valtiokonttori_kuukausidata",
                    "label": "Valtiokonttori, valtion taloushallinnon kuukausidata",
                },
                {
                    "source_id": "valtioneuvosto_hallitukset",
                    "label": "Valtioneuvosto, hallitukset ja ministerit",
                },
                {
                    "source_id": "statfin_cost_of_living_11xm",
                    "label": "Tilastokeskus, elinkustannusindeksi",
                },
            ],
        },
        "eras": era_rows,
        "monthly": [
            {
                "month": row["month_start"].isoformat(),
                "cabinet_ordinal": row["cabinet_ordinal"],
                "cabinet_name": row["cabinet_name"],
                "net_eur": float(row["net_eur"]),
            }
            for row in monthly
        ],
    }


def validate(payload: dict[str, Any]) -> None:
    eras = payload["eras"]
    monthly = payload["monthly"]

    months_from_eras = sum(row["observed_months"] for row in eras)
    if months_from_eras != len(monthly):
        raise ValueError(
            f"Kuukausien summa kausittain ({months_from_eras}) ei vastaa "
            f"kuukausirivien määrää ({len(monthly)})"
        )

    seen = [row["month"] for row in monthly]
    if len(set(seen)) != len(seen):
        raise ValueError("Sama kuukausi on kohdennettu useammin kuin kerran")

    ordinals = [row["cabinet_ordinal"] for row in eras]
    if ordinals != sorted(ordinals):
        raise ValueError("Kaudet eivät ole aikajärjestyksessä")

    if sum(1 for row in eras if row["is_ongoing"]) != 1:
        raise ValueError("Istuvia hallituksia pitäisi olla tasan yksi")

    for row in eras:
        if row["observed_months"] <= 0:
            raise ValueError(f"Kaudella {row['cabinet_name']} ei ole yhtäkään kuukautta")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kokoa hallituskausien toteuma.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--semantic-view", default=settings.table)
    args = parser.parse_args()

    from google.cloud import bigquery

    client = bigquery.Client(project=args.project)
    base_year = _run(client, base_year_sql(args.project, args.dataset))[0]["base_year"]
    eras = _run(client, era_sql(args.project, args.dataset, args.semantic_view))
    monthly = _run(client, monthly_sql(args.project, args.dataset, args.semantic_view))
    missing = _run(client, missing_months_sql(args.project, args.dataset, args.semantic_view))

    payload = build_payload(eras, monthly, missing, base_year)
    validate(payload)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    meta = payload["meta"]
    print(f"{OUT.relative_to(ROOT)}: {len(payload['eras'])} hallituskautta, "
          f"{meta['observed_months']} kuukautta "
          f"({meta['first_observed_month']}..{meta['last_observed_month']}), "
          f"reaalivuosi {meta['real_base_year']}, "
          f"puuttuvia kuukausia {len(meta['missing_months'])}")


if __name__ == "__main__":
    main()
