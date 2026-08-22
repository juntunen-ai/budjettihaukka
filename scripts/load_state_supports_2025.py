#!/usr/bin/env python3
"""2025 snapshot of state transfers that look like tuki, avustus or rahoitus.

This is a name-and-code curation of the yearly fiscal mart, not an official
subsidy register. Wellbeing-area service funding, operating costs, defence
procurement and most tax expenditures are kept out on purpose.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "reference" / "state_supports_2025_v1.json"

FAMILY_FI = {
    "etuudet": "Kotitalouksien etuudet",
    "sosiaalivakuutus": "Sosiaalivakuutuksen valtionosuudet",
    "valtionosuudet": "Kuntien ja koulutuksen valtionosuudet",
    "korkeakoulu_tiede": "Korkeakoulu- ja tiederahoitus",
    "maatalous_maaseutu": "Maatalous ja maaseutu",
    "alue_rakenne": "Alue- ja rakennetuet",
    "yritykset_energia_tki": "Yritykset, energia ja TKI",
    "jarjestot_kulttuuri": "Järjestö-, kulttuuri- ja liikunta-avustukset",
    "kehitysyhteistyo": "Kehitysyhteistyö",
    "muut_siirrot": "Muut avustukset ja korvaukset",
}
EXCLUDE_CODES = {
    "28.89.31.",
    "28.50.15.",
    "36.01.90.",
    "27.10.19.",
    "27.10.18.",
    "28.92.69.",
    "31.10.20.",
    "31.20.60.",
    "31.10.77.",
    "26.20.70.",
    "28.10.95.",
    "27.10.01.",
}
EXCLUDE_NAME = re.compile(
    r"(toimintamenot|arvonlisäveromenot|vain liikekirjanpidossa|nettolainanotto|velanhallinta)"
)
TARGET_FAMILIES = {
    "maatalous_maaseutu",
    "alue_rakenne",
    "yritykset_energia_tki",
    "jarjestot_kulttuuri",
    "kehitysyhteistyo",
    "muut_siirrot",
}
OSASTO = {
    "21": ("eduskunta", "Eduskunta"),
    "23": ("vnk", "Valtioneuvoston kanslia"),
    "24": ("um", "Ulkoministeriö"),
    "25": ("om", "Oikeusministeriö"),
    "26": ("sm", "Sisäministeriö"),
    "27": ("plm", "Puolustusministeriö"),
    "28": ("vm", "Valtiovarainministeriö"),
    "29": ("okm", "Opetus- ja kulttuuriministeriö"),
    "30": ("mmm", "Maa- ja metsätalousministeriö"),
    "31": ("lvm", "Liikenne- ja viestintäministeriö"),
    "32": ("tem", "Työ- ja elinkeinoministeriö"),
    "33": ("stm", "Sosiaali- ja terveysministeriö"),
    "35": ("ym", "Ympäristöministeriö"),
}
SHORT_NAMES = {
    "33.40.60.": "Kansaneläke, valtion osuus",
    "28.90.30.": "Kuntien peruspalvelujen valtionosuus",
    "33.20.52.": "Työttömyysetuuksien perusturva",
    "33.10.54.": "Asumistuki",
    "33.10.57.": "Perustoimeentulotuki",
    "29.20.30.": "VOS ammatillinen koulutus",
    "33.40.51.": "Maatalousyrittäjien eläkkeet, valtion osuus",
    "29.70.55.": "Opintoraha ja asumislisä",
    "29.10.30.": "VOS esi- ja perusopetus ja varhaiskasvatus",
    "33.40.52.": "Yrittäjäeläkkeet, valtion osuus",
    "33.20.50.": "Työttömyysetuuksien ansioturva",
    "24.30.66.": "Varsinainen kehitysyhteistyö",
    "28.90.35.": "Korvaus kunnille veroperustemuutoksista",
    "30.20.41.": "EU-tulotuki ja EU-markkinatuki",
    "30.20.44.": "Luonnonhaittakorvaukset",
    "29.40.50.": "Yliopistojen perusrahoitus",
    "29.40.55.": "Ammattikorkeakoulujen perusrahoitus",
    "33.30.60.": "Sairausvakuutus, valtion osuus",
    "33.10.50.": "Perhe-etuudet",
    "32.20.40.": "TKI-toiminnan tukeminen",
    "32.30.64.": "EU:n alue- ja rakennepolitiikka",
    "33.90.50.": "Sote-järjestöjen avustukset",
}


def _norm(name: str) -> str:
    return (name or "").lower().replace("\xa0", " ")


def budget_authority(code: str, hallinnonala: str) -> tuple[str, str]:
    prefix = (code or "").split(".")[0]
    if prefix in OSASTO:
        return OSASTO[prefix]
    label = (hallinnonala or "Muu").replace(" hallinnonala", "").strip() or "Muu"
    return ("muu", label)


def granting_agency(code: str, name: str, authority_id: str, authority_fi: str) -> tuple[str, str, str]:
    n = _norm(name)
    c = code or ""
    if "finnvera" in n or c.startswith(("32.40.47", "32.40.48", "32.40.82")):
        return ("finnvera", "Finnvera", "Vienti- ja takaustuet")
    if "teollisuussijoitus" in n:
        return ("tesi", "Tesi", "Pääomasijoitukset")
    if "vtt" in n:
        return ("vtt", "VTT", "TEM:n avustus tutkimuslaitokselle")
    if "malmijalostus" in n:
        return ("malmi", "Suomen Malmijalostus", "VNK:n pääomasijoitus")
    if c.startswith("32.20.40") or c.startswith("32.20.83"):
        return ("business_finland", "Business Finland", "TKI-avustukset ja -lainat, tyypillinen myöntäjä")
    if any(token in n for token in ("energiatuki", "sähköistämis", "uusiutuvan energian")):
        return ("tem_energia", "TEM, energiatuet", "Energiatuki ja sähköistäminen")
    if c.startswith("30.20") or any(token in n for token in ("eu-tulo", "luonnonhaitta", "ympäristökorvau", "puutarhatalouden kansallinen")):
        return ("ruokavirasto", "Ruokavirasto", "CAP ja kansalliset maataloustuet")
    if c.startswith("32.30.64") or c.startswith("30.10.64"):
        return ("ely", "ELY / elinvoimakeskukset", "EU:n alue- ja rakennerahastot")
    if c.startswith("33.90.50"):
        return ("stea", "STEA", "Sote-järjestöjen avustukset")
    if c.startswith("24.30"):
        return ("um", "Ulkoministeriö", "Kehitysyhteistyö")
    if c.startswith("32.30.51"):
        return ("tem_te", "TEM / työllisyysalueet", "Työvoima- ja yrityspalvelut")
    if c.startswith("29.80") or c.startswith("29.90") or "valtionavustus järjestöille" in n:
        return ("okm", "Opetus- ja kulttuuriministeriö", "Kulttuuri, liikunta ja järjestöt")
    return (authority_id, authority_fi, "Momentin budjettivastuu, toteuttavaa virastoa ei nimetty")


def classify(code: str, name: str) -> str | None:
    n = _norm(name)
    c = code or ""
    if c in EXCLUDE_CODES or EXCLUDE_NAME.search(name or ""):
        return None
    if any(token in n for token in ("kansaneläke", "sairausvakuutus", "maatalousyrittäjän eläke", "yrittäjän eläke", "kelan sosiaaliturva")):
        return "sosiaalivakuutus"
    if any(
        token in n
        for token in (
            "asumistuki",
            "toimeentulotuki",
            "opintoraha",
            "asumislis",
            "perhe-etuudet",
            "työttömyysetu",
            "opintolain",
            "koulumatka",
            "ateriatuki",
            "elatustuki",
        )
    ):
        return "etuudet"
    if c.startswith("28.90.") or "ahvenanmaan tasoitus" in n:
        return "valtionosuudet"
    if "valtionosuus" in n and any(
        token in n for token in ("kunnille", "esi- ja perus", "ammatillisen", "lukio", "vapaan sivisty", "taiteen perus", "esittävän")
    ):
        return "valtionosuudet"
    if "veroperustemuutoksista" in n:
        return "valtionosuudet"
    if any(token in n for token in ("yliopistojen toimintaan", "ammattikorkeakoulujen toimintaan", "suomen akatemian", "korkeakoululaitoksen")):
        return "korkeakoulu_tiede"
    if any(
        token in n
        for token in (
            "maatalous",
            "maaseutu",
            "eu-tulo",
            "eu-markkina",
            "luonnonhaitta",
            "ympäristökorvau",
            "puutarhatalouden",
            "viljelij",
            "hevos",
            "lomitus",
            "eläinten hyvinvointi",
        )
    ):
        return "maatalous_maaseutu"
    if any(token in n for token in ("alue- ja rakenne", "rakennepoliti", "alueelliseen ja paikalliseen")):
        return "alue_rakenne"
    if any(token in n for token in ("kehitysyhteisty", "kehityspoliitt", "humanitaar", "tuki ukrain")):
        return "kehitysyhteistyo"
    if any(
        token in n
        for token in (
            "tutkimus-, kehittämis",
            "innovaatiotoimin",
            "lainat tutkimus",
            "vtt oy",
            "energiatuki",
            "sähköistämis",
            "uusiutuvan energian",
            "finnvera",
            "vienti- ja alus",
            "teollisuussijoitus",
            "työvoima- ja yrityspalvelut",
            "yritysten kehittämis",
            "kustannustuki",
            "kuljetustuki",
            "malmijalostus",
            "lääkekehitys",
            "meriliikenteessä käytettävien",
            "energiaverotuki",
        )
    ):
        return "yritykset_energia_tki"
    if any(
        token in n
        for token in (
            "yhdistyks",
            "säätiö",
            "järjestö",
            "seurakunt",
            "evankelis-luterilaisen",
            "liikunnan",
            "liikunnallisen",
            "taidelaitos",
            "kansallisten taide",
        )
    ):
        return "jarjestot_kulttuuri"
    if any(token in n for token in ("avustus", "valtionavustus", "valtionosuus", "korkotuki", "korvaus kunn", "korvaukset", "tuki", "tuen", "tuet")):
        return "muut_siirrot"
    return None


def fetch_rows(project: str, dataset: str) -> list[dict]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    sql = f"""
    SELECT fiscal_side, hallinnonala, momentti_tunnusp, momentti_snimi,
           SUM(net_accumulation_nominal_eur) AS eur
    FROM `{project}.{dataset}.analytics_fiscal_yearly_core_v1`
    WHERE coverage_status = 'complete' AND year = 2025
    GROUP BY 1, 2, 3, 4
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(use_query_cache=True, maximum_bytes_billed=400_000_000),
    )
    return [
        {
            "side": row.fiscal_side,
            "ha": (row.hallinnonala or "").replace(" hallinnonala", ""),
            "code": (row.momentti_tunnusp or "").strip(),
            "name": (row.momentti_snimi or "").replace("\xa0", " ").strip(),
            "eur": float(row.eur or 0),
        }
        for row in job.result()
    ]


def build_payload(rows: list[dict]) -> dict:
    expense_total = sum(row["eur"] for row in rows if row["side"] == "expense")
    hva = sum(row["eur"] for row in rows if row["code"] == "28.89.31." and row["side"] == "expense")
    energiaverotuki = sum(abs(row["eur"]) for row in rows if row["code"] == "28.91.41.")

    agg: dict[str, dict] = {}
    for row in rows:
        if row["side"] != "expense":
            continue
        key = row["code"] or row["name"]
        item = agg.setdefault(key, {"code": row["code"], "name": row["name"], "ha": row["ha"], "eur": 0.0})
        item["eur"] += row["eur"]
        item["name"] = row["name"] or item["name"]
        item["ha"] = row["ha"] or item["ha"]

    families: dict[str, float] = defaultdict(float)
    moments: list[dict] = []
    for item in agg.values():
        family = classify(item["code"], item["name"])
        if not family:
            continue
        families[family] += item["eur"]
        auth_id, auth_fi = budget_authority(item["code"], item["ha"])
        agency_id, agency_fi, agency_note = granting_agency(item["code"], item["name"], auth_id, auth_fi)
        moments.append(
            {
                "code": item["code"],
                "name": item["name"],
                "name_short": SHORT_NAMES.get(item["code"], item["name"][:64]),
                "hallinnonala": item["ha"],
                "authority_id": auth_id,
                "authority_fi": auth_fi,
                "agency_id": agency_id,
                "agency_fi": agency_fi,
                "agency_note": agency_note,
                "family": family,
                "family_fi": FAMILY_FI[family],
                "nominal_eur": round(item["eur"], 2),
            }
        )

    if energiaverotuki:
        families["yritykset_energia_tki"] += energiaverotuki
        moments.append(
            {
                "code": "28.91.41.",
                "name": "Energiaverotuki",
                "name_short": "Energiaverotuki (verotuki)",
                "hallinnonala": "Valtiovarainministeriön",
                "authority_id": "vm",
                "authority_fi": "Valtiovarainministeriö",
                "agency_id": "vm",
                "agency_fi": "Valtiovarainministeriö",
                "agency_note": "Verotuki, ei menomomentin maksatus",
                "family": "yritykset_energia_tki",
                "family_fi": FAMILY_FI["yritykset_energia_tki"],
                "nominal_eur": round(energiaverotuki, 2),
            }
        )

    moments.sort(key=lambda item: -abs(item["nominal_eur"]))
    family_rows = [
        {"id": key, "label_fi": FAMILY_FI[key], "nominal_eur": round(value, 2), "moment_count": sum(1 for item in moments if item["family"] == key)}
        for key, value in families.items()
        if value > 0
    ]
    family_rows.sort(key=lambda item: -item["nominal_eur"])
    included = sum(item["nominal_eur"] for item in family_rows)
    discretionary = sum(families[key] for key in TARGET_FAMILIES)
    targeted_moments = [item for item in moments if item["family"] in TARGET_FAMILIES]
    by_authority: dict[str, dict] = {}
    by_agency: dict[str, dict] = {}
    for item in targeted_moments:
        auth = by_authority.setdefault(
            item["authority_id"],
            {"id": item["authority_id"], "label_fi": item["authority_fi"], "nominal_eur": 0.0, "families": defaultdict(float)},
        )
        auth["nominal_eur"] += item["nominal_eur"]
        auth["families"][item["family"]] += item["nominal_eur"]
        agency = by_agency.setdefault(
            item["agency_id"],
            {
                "id": item["agency_id"],
                "label_fi": item["agency_fi"],
                "note": item["agency_note"],
                "nominal_eur": 0.0,
                "moment_count": 0,
            },
        )
        agency["nominal_eur"] += item["nominal_eur"]
        agency["moment_count"] += 1
    authority_rows = []
    for auth in sorted(by_authority.values(), key=lambda row: -row["nominal_eur"]):
        authority_rows.append(
            {
                "id": auth["id"],
                "label_fi": auth["label_fi"],
                "nominal_eur": round(auth["nominal_eur"], 2),
                "families": [
                    {"id": key, "label_fi": FAMILY_FI[key], "nominal_eur": round(value, 2)}
                    for key, value in sorted(auth["families"].items(), key=lambda pair: -pair[1])
                ],
            }
        )
    agency_rows = [
        {**row, "nominal_eur": round(row["nominal_eur"], 2)}
        for row in sorted(by_agency.values(), key=lambda row: -row["nominal_eur"])
    ]
    return {
        "meta": {
            "year": 2025,
            "coverage_status": "complete",
            "metric": "nettokertymä, toteuma",
            "unit": "EUR",
            "source_fi": "Valtiokonttori, valtion taloushallinnon kuukausidata",
            "source_url": "https://www.tutkihallintoa.fi/valtiontalous/valtion-taloustiedot/",
            "caveat_fi": (
                "Kokoelma momentteja, joiden nimi viittaa tukeen, avustukseen, valtionosuuteen "
                "tai rahoitukseen, täydennettynä TKI-tuella ja energiaverotuella. Ei ole virallinen "
                "tukirekisteri. Hyvinvointialueiden sote-rahoitus ja suurin osa verotuista puuttuvat."
            ),
        },
        "headline": {
            "included_eur": round(included, 2),
            "expense_eur": round(expense_total, 2),
            "hva_excluded_eur": round(hva, 2),
            "discretionary_eur": round(discretionary, 2),
            "moment_count": len(moments),
            "energiaverotuki_eur": round(energiaverotuki, 2),
        },
        "families": family_rows,
        "moments": moments,
        "top_moments": moments[:18],
        "targeted_by_authority": authority_rows,
        "targeted_by_agency": agency_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="budjettihaukka-gpt")
    parser.add_argument("--dataset", default="valtiodata")
    args = parser.parse_args()
    payload = build_payload(fetch_rows(args.project, args.dataset))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUT} moments={payload['headline']['moment_count']} mrd={payload['headline']['included_eur']/1e9:.2f}")


if __name__ == "__main__":
    main()
