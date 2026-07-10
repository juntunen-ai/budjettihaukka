#!/usr/bin/env python3
"""Lataa rakenteelliset tapahtumat (data/reference/structural_events.yaml)
BigQueryyn tauluksi structural_events_v1.

Tapahtumat annotoivat aikasarjavastauksia: tasosiirtymä joka johtuu
uudistuksesta merkitään kaavioon ja selitykseen, jotta se ei näytä
todelliselta meno-/leikkausmuutokselta.
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

EVENTS_PATH = ROOT / "data" / "reference" / "structural_events.yaml"


def load_events() -> list[dict]:
    doc = yaml.safe_load(EVENTS_PATH.read_text(encoding="utf-8"))
    rows = []
    for event in doc["events"]:
        for field in ("year", "id", "label_fi", "description_fi", "affects"):
            if not event.get(field):
                raise ValueError(f"structural event missing {field}: {event}")
        rows.append(
            {
                "event_id": event["id"],
                "year": int(event["year"]),
                "label_fi": event["label_fi"],
                "description_fi": " ".join(str(event["description_fi"]).split()),
                "affects_concepts": list(event["affects"]),
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Lataa rakennemuutostapahtumat BigQueryyn.")
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    rows = load_events()
    print(f"Validoitu {len(rows)} tapahtumaa")
    if args.validate_only:
        return 0

    client = bigquery.Client(project=args.project)
    table_id = f"{args.project}.{args.dataset}.structural_events_v1"
    client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("event_id", "STRING"),
                bigquery.SchemaField("year", "INT64"),
                bigquery.SchemaField("label_fi", "STRING"),
                bigquery.SchemaField("description_fi", "STRING"),
                bigquery.SchemaField("affects_concepts", "STRING", mode="REPEATED"),
            ],
        ),
    ).result()
    print(f"BQ-taulu -> {table_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
