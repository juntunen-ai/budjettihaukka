from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ingest_external_semantic_candidates import _extract_finto_rows, _extract_sanastot_rows


def test_extract_finto_rows() -> None:
    payload = {
        "graph": [
            {
                "uri": "http://www.yso.fi/onto/yso/p1650",
                "prefLabel": [{"lang": "fi", "value": "varhaiskasvatus"}],
                "hiddenLabel": [{"lang": "fi", "value": "päivähoito"}],
                "broader": {"uri": "http://www.yso.fi/onto/yso/p1030"},
                "related": [{"uri": "http://www.yso.fi/onto/yso/p200"}],
            },
            {
                "uri": "http://www.yso.fi/onto/yso/p1030",
                "prefLabel": [{"lang": "fi", "value": "koulutus"}],
            },
            {
                "uri": "http://www.yso.fi/onto/yso/p200",
                "prefLabel": [{"lang": "fi", "value": "esiopetus"}],
            },
        ]
    }
    rows = _extract_finto_rows(
        concept_id="varhaiskasvatus",
        concept_label_fi="Varhaiskasvatus",
        source_system="finto_yso",
        source_uri="http://www.yso.fi/onto/yso/p1650",
        payload=payload,
    )
    aliases = {row["alias"]: row for row in rows}
    assert "varhaiskasvatus" in aliases
    assert "päivähoito" in aliases
    assert aliases["päivähoito"]["source_kind"] == "hidden"
    assert aliases["varhaiskasvatus"]["context_broader_labels"] == ["koulutus"]
    assert aliases["varhaiskasvatus"]["context_related_labels"] == ["esiopetus"]


def test_extract_sanastot_rows() -> None:
    next_data = {
        "props": {
            "pageProps": {
                "reduxWrapperActionsGSSP": [
                    {
                        "type": "conceptAPI/executeQuery/fulfilled",
                        "payload": {
                            "uri": "https://iri.suomi.fi/terminology/oksa/c125",
                            "definition": {"fi": "<p>suunnitelmallinen toiminta</p>"},
                            "recommendedTerms": [
                                {"language": "fi", "label": "varhaiskasvatus"},
                                {"language": "en", "label": "early childhood education and care"},
                            ],
                            "synonyms": [{"language": "fi", "label": "småbarnspedagogik"}],
                            "notRecommendedTerms": [{"language": "fi", "label": "päivähoito"}],
                            "broader": [{"label": {"fi": "koulutus"}}],
                            "narrower": [{"label": {"fi": "kokopäiväinen varhaiskasvatus"}}],
                            "related": [{"label": {"fi": "esiopetus"}}],
                        },
                    },
                    {
                        "type": "terminologyApi/executeQuery/fulfilled",
                        "payload": {"label": {"fi": "OKSA – Opetus- ja koulutussanasto, 3. laitos"}},
                    },
                ]
            }
        }
    }
    html = '<script id="__NEXT_DATA__" type="application/json">' + json.dumps(next_data, ensure_ascii=False) + "</script>"
    rows = _extract_sanastot_rows(
        concept_id="varhaiskasvatus",
        concept_label_fi="Varhaiskasvatus",
        source_system="sanastot_oksa",
        source_url="https://sanastot.suomi.fi/terminology/oksa/concept/c125",
        html_text=html,
    )
    aliases = {row["alias"]: row for row in rows}
    assert aliases["varhaiskasvatus"]["source_kind"] == "recommended"
    assert aliases["päivähoito"]["source_kind"] == "not_recommended"
    assert aliases["varhaiskasvatus"]["definition_fi"] == "suunnitelmallinen toiminta"
    assert aliases["varhaiskasvatus"]["context_broader_labels"] == ["koulutus"]


if __name__ == "__main__":
    test_extract_finto_rows()
    test_extract_sanastot_rows()
    print("External semantic candidate tests PASSED")
