#!/usr/bin/env python3
"""Rikastaa budjettiontologian aliaksia Finto/YSO-sanastosta.

Hakee jokaiselle ontologian konseptille YSO-käsitteen (api.finto.fi),
poimii viralliset synonyymit (altLabel) ja lähikäsitteet (narrower) ja
tuottaa alias-KANDIDAATTEJA. Kandidaatit kirjoitetaan
data/ontology/finto_alias_candidates.jsonl -tiedostoon; --apply lisää ne
ontologia-YAML:iin (alias_type: finto_alt / finto_narrower), minkä jälkeen
eval-portit (robustness + semantic goldens) ratkaisevat kelpaavatko ne.

Prosessiperiaate: kone ehdottaa, evalit portittavat, ihminen omistaa.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FINTO_API = "https://api.finto.fi/rest/v1"
ONTOLOGY_PATH = ROOT / "data" / "ontology" / "budjettihaukka_ontology.yaml"
CANDIDATES_PATH = ROOT / "data" / "ontology" / "finto_alias_candidates.jsonl"

# Käsin valitut hakusanat konsepteille, joiden label ei osu YSO:on suoraan.
SEARCH_OVERRIDES = {
    "poliisi_ja_sisainen_turvallisuus": "poliisi",
    "sosiaali_ja_terveys": "terveyspalvelut",
    "hyvinvointialueiden_rahoitus": "hyvinvointialueet",
    "tyollisyys_ja_tyovoimapolitiikka": "työvoimapolitiikka",
    "kuntien_valtionosuudet": "valtionosuudet",
    "liikenne_ja_infra": "liikenne",
    "ymparisto_ja_ilmasto": "ympäristönsuojelu",
    "tutkimus_ja_innovaatiot": "tutkimus",
    "velka_ja_korkomenot": "valtionvelka",
    "maa_ja_metsatalous": "maatalous",
    "kulttuuri_ja_taide": "kulttuuri",
    "kehitysyhteistyo": "kehitysyhteistyö",
    "ulkoasiainhallinto": "ulkopolitiikka",
    "energiapolitiikka": "energiapolitiikka",
    "valtionhallinto": "valtionhallinto",
}


def _get(url: str, params: dict) -> dict | None:
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
        except requests.RequestException:
            pass
        time.sleep(1 + attempt)
    return None


def find_yso_concept(term: str) -> dict | None:
    data = _get(f"{FINTO_API}/search", {"vocab": "yso", "query": term, "lang": "fi"})
    results = (data or {}).get("results") or []
    exact = [r for r in results if str(r.get("prefLabel", "")).lower() == term.lower()]
    return (exact or results or [None])[0]


def fetch_labels(uri: str) -> tuple[list[str], list[str]]:
    """(altLabels_fi, narrower_prefLabels_fi) YSO-käsitteelle."""
    data = _get(f"{FINTO_API}/yso/data", {"uri": uri, "format": "application/ld+json"})
    if not data:
        return [], []
    alt: list[str] = []
    narrower_uris: list[str] = []
    target = None
    for node in data.get("graph", []):
        if node.get("uri") == uri:
            target = node
            break
    if not target:
        return [], []

    def _fi_values(field) -> list[str]:
        values = field if isinstance(field, list) else [field]
        out = []
        for value in values:
            if isinstance(value, dict) and value.get("lang") == "fi" and value.get("value"):
                out.append(str(value["value"]))
        return out

    alt = _fi_values(target.get("altLabel") or [])
    narrower_field = target.get("narrower") or []
    narrower_list = narrower_field if isinstance(narrower_field, list) else [narrower_field]
    narrower_uris = [n.get("uri") for n in narrower_list if isinstance(n, dict) and n.get("uri")]

    narrower_labels: list[str] = []
    label_by_uri = {
        node.get("uri"): node.get("prefLabel")
        for node in data.get("graph", [])
        if node.get("uri") and node.get("prefLabel")
    }
    for n_uri in narrower_uris[:12]:
        labels = _fi_values(label_by_uri.get(n_uri) or [])
        narrower_labels.extend(labels)
    return alt, narrower_labels


def main() -> int:
    parser = argparse.ArgumentParser(description="Rikasta ontologia Finto/YSO-aliaksilla.")
    parser.add_argument("--apply", action="store_true", help="Lisää kandidaatit ontologia-YAML:iin")
    parser.add_argument("--max-narrower", type=int, default=8)
    args = parser.parse_args()

    ontology = yaml.safe_load(ONTOLOGY_PATH.read_text(encoding="utf-8"))
    concepts = ontology["concepts"]

    candidates: list[dict] = []
    for concept in concepts:
        concept_id = concept["concept_id"]
        existing = {str(a.get("alias", "")).lower() for a in concept.get("aliases", [])}
        term = SEARCH_OVERRIDES.get(concept_id) or concept.get("label_fi") or concept.get("label") or concept_id
        hit = find_yso_concept(term)
        if not hit or not hit.get("uri"):
            print(f"  {concept_id}: ei YSO-osumaa haulle {term!r}")
            continue
        uri = hit["uri"]
        alt_labels, narrower_labels = fetch_labels(uri)
        new_here = 0
        for label, alias_type in (
            [(a, "finto_alt") for a in alt_labels]
            + [(n, "finto_narrower") for n in narrower_labels[: args.max_narrower]]
        ):
            normalized = label.strip().lower()
            if not normalized or normalized in existing or len(normalized) < 4:
                continue
            existing.add(normalized)
            candidates.append(
                {
                    "concept_id": concept_id,
                    "alias": normalized,
                    "alias_type": alias_type,
                    "source": "finto_yso",
                    "yso_uri": uri,
                    "lang": "fi",
                }
            )
            new_here += 1
        print(f"  {concept_id}: YSO {uri.rsplit('/',1)[-1]} ({hit.get('prefLabel')}), {new_here} uutta kandidaattia")

    CANDIDATES_PATH.write_text(
        "".join(json.dumps(c, ensure_ascii=False) + "\n" for c in candidates),
        encoding="utf-8",
    )
    print(f"\n{len(candidates)} kandidaattia -> {CANDIDATES_PATH.relative_to(ROOT)}")

    if args.apply and candidates:
        by_concept: dict[str, list[dict]] = {}
        for candidate in candidates:
            by_concept.setdefault(candidate["concept_id"], []).append(candidate)
        for concept in concepts:
            for candidate in by_concept.get(concept["concept_id"], []):
                concept.setdefault("aliases", []).append(
                    {
                        "alias": candidate["alias"],
                        "source": "finto_yso",
                        "alias_type": candidate["alias_type"],
                        "lang": "fi",
                    }
                )
        ONTOLOGY_PATH.write_text(
            yaml.safe_dump(ontology, allow_unicode=True, sort_keys=False, width=100),
            encoding="utf-8",
        )
        print(f"Lisätty ontologiaan: {ONTOLOGY_PATH.relative_to(ROOT)} — aja evalit ennen committia!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
