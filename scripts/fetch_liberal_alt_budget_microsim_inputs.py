#!/usr/bin/env python3
"""Snapshot public inputs for the alternative-budget household model.

The snapshots make the distributional calculation reproducible without a
network connection. They contain only public aggregate statistics, never
person-level records.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = (
    ROOT
    / "data"
    / "reference"
    / "liberaali_vaihtoehtobudjetti"
    / "microsim_inputs"
)
PXWEB_ROOT = "https://pxweb2.stat.fi/PxWeb/api/v1/fi/StatFin"
RETRIEVED_AT = "2026-08-23"


def items(code: str, values: list[str]) -> dict[str, Any]:
    return {
        "code": code,
        "selection": {"filter": "item", "values": values},
    }


SOURCES: dict[str, dict[str, Any]] = {
    "statfin_income_deciles_2024": {
        "table": "tjt/128c.px",
        "landing_page": (
            "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/"
            "StatFin__tjt/statfin_tjt_pxt_128c.px/"
        ),
        "description": "Asuntokuntien tulot ja tulojen rakenne tulokymmenyksittain",
        "query": [
            items(
                "contentscode",
                [
                    "asuntok",
                    "tjt-henkiloita",
                    "kokonum_mean",
                    "modoecd_mean",
                    "tjt-palk",
                    "tjt-yrtu",
                    "tjt-omtu",
                    "saatusi",
                    "tjt-vanel",
                    "tjt-pelake",
                    "tjt-perav",
                    "tjt-tyoel",
                    "tjt-asutu",
                    "tjt-optuki",
                    "tjt-muutuki",
                    "bruttotu",
                    "makstu",
                    "tjt-ltva",
                    "tjt-ltvp",
                    "tjt-lkuve",
                    "kturaha",
                    "tjt-ekvikturaha_mean",
                ],
            ),
            items("timeperiod_y", ["2024"]),
            items(
                "desiilit_2_20120101",
                ["SS", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            ),
        ],
    },
    "statfin_life_stage_deciles_2024": {
        "table": "tjt/12ew.px",
        "landing_page": (
            "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/"
            "StatFin__tjt/statfin_tjt_pxt_12ew.px/"
        ),
        "description": "Kotitaloudet tulokymmenyksen ja elinvaiheen mukaan",
        "query": [
            items(
                "desiilit_2_20120101",
                ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            ),
            # Non-overlapping detailed groups plus the residual "other" group.
            items(
                "elinvaihe_5_20200201",
                ["10", "11", "12", "21", "22", "23", "31", "32", "4"],
            ),
            items("contentscode", ["ykorpop_sumwgt", "ykor_sumwgt", "desiprosentti"]),
            items("timeperiod_y", ["2024"]),
        ],
    },
    "statfin_socioeconomic_deciles_2024": {
        "table": "tjt/15ar.px",
        "landing_page": (
            "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/"
            "StatFin__tjt/statfin_tjt_pxt_15ar.px/"
        ),
        "description": "Henkilot tulokymmenyksen ja sosioekonomisen aseman mukaan",
        "query": [
            items(
                "desiilit_2_20120101",
                ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            ),
            items("sosioekon_asema_4_20200214", ["1", "2", "6", "7", "81", "8"]),
            items("contentscode", ["ykorpop_sumwgt", "hdesiprosentti"]),
            items("timeperiod_y", ["2024"]),
        ],
    },
    "statfin_transfer_recipients_2024": {
        "table": "tjt/122s.px",
        "landing_page": (
            "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/"
            "StatFin__tjt/statfin_tjt_pxt_122s.px/"
        ),
        "description": "Tulonsiirtoja saaneet asuntokunnat tulokymmenyksittain",
        "query": [
            items(
                "desiilit_2_20120101",
                ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            ),
            items(
                "tulolaji_1_20161118",
                ["PT6", "PT7", "PT8", "PT9", "PT16", "MT1", "MT2"],
            ),
            items("timeperiod_y", ["2024"]),
            items(
                "contentscode",
                [
                    "asuntok_ehd",
                    "osuus_tulokymmenys",
                    "keskiarvo_ehd",
                    "summa",
                    "kturaha_ak_ehd_mean",
                ],
            ),
        ],
    },
    "statfin_consumption_quintiles_2022": {
        "table": "ktutk/14pg.px",
        "landing_page": (
            "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/"
            "StatFin__ktutk/statfin_ktutk_pxt_14pg.px/"
        ),
        "description": "Kotitalouksien kulutusmenot tuloviidenneksittain",
        "query": [
            items("timeperiod_y", ["2022"]),
            items(
                "coicop_46_20231201",
                [
                    "0",
                    "01",
                    "0611",
                    "073",
                    "08391",
                    "0946",
                    "096",
                    "0971",
                    "0972",
                    "111",
                    "112",
                    "045",
                    "071",
                    "0722",
                    "0724",
                    "0126",
                    "021",
                    "023",
                ],
            ),
            items("tuloviidennes_2_20140501", ["SS", "1", "2", "3", "4", "5"]),
            items("contentscode", ["kulu_kt_hk_1_2"]),
        ],
    },
    "statfin_consumption_household_types_2022": {
        "table": "ktutk/14ph.px",
        "landing_page": (
            "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/"
            "StatFin__ktutk/statfin_ktutk_pxt_14ph.px/"
        ),
        "description": "Kotitalouksien kulutusmenot kotitaloustyypeittain",
        "query": [
            items("timeperiod_y", ["2022"]),
            items(
                "coicop_46_20231201",
                [
                    "0",
                    "01",
                    "0611",
                    "073",
                    "08391",
                    "0946",
                    "096",
                    "0971",
                    "0972",
                    "111",
                    "112",
                    "045",
                    "071",
                    "0722",
                    "0724",
                    "0126",
                    "021",
                    "023",
                ],
            ),
            items("elinvaihe_13_20160101", ["1", "2", "3", "4", "5", "6", "SSS"]),
            items("contentscode", ["kulu_kt_hk_1_2"]),
        ],
    },
    "statfin_consumption_type_background_2022": {
        "table": "ktutk/14lv.px",
        "landing_page": (
            "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/"
            "StatFin__ktutk/statfin_ktutk_pxt_14lv.px/"
        ),
        "description": "Kulutustutkimuksen taustatiedot kotitaloustyypeittain",
        "query": [
            items("timeperiod_y", ["2022"]),
            items("elinvaihe_13_20160101", ["1", "2", "3", "4", "5", "6", "SSS"]),
            items(
                "contentscode",
                ["pklkm_sum", "jlkm_mean", "oecdmod_mean", "kaytetmk_2_mean"],
            ),
        ],
    },
    "statfin_consumption_background_2022": {
        "table": "ktutk/14qa.px",
        "landing_page": (
            "https://pxdata.stat.fi/PxWeb/pxweb/fi/StatFin/"
            "StatFin__ktutk/statfin_ktutk_pxt_14qa.px/"
        ),
        "description": "Kulutustutkimuksen taustatiedot tuloviidenneksittain",
        "query": [
            items("timeperiod_y", ["2022"]),
            items("tuloviidennes_2_20140501", ["SS", "1", "2", "3", "4", "5"]),
            items(
                "contentscode",
                ["pklkm_sum", "jlkm_mean", "oecdmod_mean", "Kaytetmk_2_mean"],
            ),
        ],
    },
}


def fetch_json(url: str, query: list[dict[str, Any]]) -> dict[str, Any]:
    payload = json.dumps(
        {"query": query, "response": {"format": "json-stat2"}},
        ensure_ascii=True,
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "Budjettihaukka/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch public microsimulation inputs.")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    manifest: list[dict[str, Any]] = []
    for source_id, source in SOURCES.items():
        api_url = f"{PXWEB_ROOT}/{source['table']}"
        response = fetch_json(api_url, source["query"])
        if response.get("class") != "dataset" or not response.get("value"):
            raise ValueError(f"Unexpected or empty PxWeb response for {source_id}")
        snapshot = {
            "source_id": source_id,
            "description": source["description"],
            "retrieved_at": RETRIEVED_AT,
            "api_url": api_url,
            "landing_page": source["landing_page"],
            "query": source["query"],
            "response": response,
        }
        snapshot["response_sha256"] = canonical_hash(response)
        path = args.out_dir / f"{source_id}.json"
        path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        manifest.append(
            {
                "source_id": source_id,
                "path": path.name,
                "response_sha256": snapshot["response_sha256"],
                "retrieved_at": RETRIEVED_AT,
                "landing_page": source["landing_page"],
            }
        )
        print(f"{path.relative_to(ROOT)} ({len(response['value'])} values)")

    manifest_path = args.out_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps({"sources": manifest}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(manifest_path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
