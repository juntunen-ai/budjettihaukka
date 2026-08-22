from __future__ import annotations

import re
from typing import Iterable

FISCAL_SIDES = {"expense", "revenue", "financing", "technical", "mixed", "unknown"}

CUT_KEYWORDS = (
    "leikattu",
    "leikata",
    "leikkaus",
    "leikkaus",
    "säästö",
    "saasto",
    "säästetty",
    "supistettu",
    "supistui",
    "vähennetty",
)

DECLINE_KEYWORDS = (
    "pieneni",
    "pienentynyt",
    "laski",
    "laskenut",
    "romahti",
    "heikkeni",
    "heikentyi",
    "väheni",
    "vaheni",
    "kutistui",
)

REVENUE_KEYWORDS = (
    "vero",
    "verot",
    "verotulo",
    "verotulot",
    "verokertymä",
    "verokertyma",
    "tulokertymä",
    "tulokertyma",
    "tulot",
    "verolaji",
)

EXPENSE_KEYWORDS = (
    "menot",
    "meno",
    "rahoitus",
    "tuki",
    "tuet",
    "avustus",
    "avustukset",
    "kustannus",
    "kustannukset",
)

TECHNICAL_NAME_KEYWORDS = (
    "vain liikekirjanpidossa",
    "siirrettyjen määrärahojen peruutukset",
    "siirrettyjen määrärahojen peruutus",
    "tekninen",
)

FINANCING_NAME_KEYWORDS = (
    "nettolainanotto",
    "velanhallinta",
    "lainojen takaisinmaksu",
    "lyhennys",
    "lyhennykset",
    "rahoitusomaisuus",
)

REVENUE_NAME_KEYWORDS = (
    "arvonlisävero",
    "tulovero",
    "yhteisövero",
    "energiaverot",
    "alkoholijuomavero",
    "tupakkavero",
    "autovero",
    "ajoneuvovero",
    "osinkotulot",
    "myyntitulot",
    "pääomanpalautukset",
    "tuloarvio",
    "vero",
)

EXPENSE_NAME_KEYWORDS = (
    "menot",
    "toimintamenot",
    "toimintaan",
    "rahoitus",
    "avustus",
    "tuki",
    "maksut euroopan unionille",
    "valtionvelan korko",
)


def normalize_fiscal_side(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in FISCAL_SIDES else "unknown"


def _contains_any(text: str, values: Iterable[str]) -> bool:
    return any(value in text for value in values)


def infer_fiscal_side_from_question(text: str, concept_default_fiscal_side: str | None = None) -> str:
    lower = (text or "").strip().lower()
    concept_side = normalize_fiscal_side(concept_default_fiscal_side)

    if _contains_any(lower, CUT_KEYWORDS):
        return "expense"

    if _contains_any(lower, REVENUE_KEYWORDS):
        return "revenue"

    if _contains_any(lower, EXPENSE_KEYWORDS):
        return "expense"

    if concept_side not in {"unknown", "mixed"}:
        return concept_side
    return concept_side


def infer_intent_from_question(text: str, current_intent: str, fiscal_side: str) -> str:
    lower = (text or "").strip().lower()
    has_top = any(token in lower for token in ("top", "eniten", "suurin", "suurimmat", "suurimpia"))
    has_cut = _contains_any(lower, CUT_KEYWORDS)
    has_decline = _contains_any(lower, DECLINE_KEYWORDS)

    if has_cut and has_top:
        return "top_cuts"

    if fiscal_side == "revenue" and has_decline and has_top:
        return "revenue_decline"

    return current_intent


def infer_metric_from_question(text: str, current_metric: str = "nettokertyma") -> str:
    lower = (text or "").strip().lower()
    if "per capita" in lower or "asukasta kohti" in lower:
        return "nettokertyma_per_capita"
    return current_metric


def classify_moment_fiscal_side(
    momentti_tunnusp: str | None,
    momentti_snimi: str | None,
    hallinnonala: str | None = None,
) -> str:
    code = str(momentti_tunnusp or "").strip()
    name = str(momentti_snimi or "").strip().lower()
    hallinnonala_name = str(hallinnonala or "").strip().lower()

    if not code and not name:
        return "technical"

    if code.lower() == "tapahtumia" or _contains_any(name, TECHNICAL_NAME_KEYWORDS):
        return "technical"

    if _contains_any(name, FINANCING_NAME_KEYWORDS) or code.startswith("15."):
        return "financing"

    if "arvonlisäveromenot" in name:
        return "expense"

    if re.match(r"^(11|12|13|14)\.", code):
        return "revenue"

    if _contains_any(name, REVENUE_NAME_KEYWORDS) and "veromenot" not in name and "arvonlisäveromenot" not in name:
        return "revenue"

    if _contains_any(name, EXPENSE_NAME_KEYWORDS):
        return "expense"

    if "hallinnonala" in hallinnonala_name and hallinnonala_name:
        return "expense"

    return "expense"


def fiscal_side_case_sql(
    *,
    code_expr: str,
    name_expr: str,
    hallinnonala_expr: str | None = None,
) -> str:
    hall_expr = hallinnonala_expr or "''"
    # Tekniset erät johdetaan TECHNICAL_NAME_KEYWORDS-listasta eikä kirjoiteta
    # tähän erikseen. Aiemmin lista ja SQL olivat eri linjoilla: lista
    # tunnisti sekä yksikön "peruutus" että monikon "peruutukset", mutta SQL
    # vain yksikön. Koska "peruutukset" ei sisällä merkkijonoa "peruutus",
    # 19,4 miljardin tekninen erä luettiin tuloksi vuosina 1998-2026.
    technical_conditions = " ".join(
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%{keyword}%' THEN 'technical' "
        for keyword in TECHNICAL_NAME_KEYWORDS
    )
    return (
        "CASE "
        f"WHEN COALESCE(CAST({code_expr} AS STRING), '') = '' AND COALESCE(CAST({name_expr} AS STRING), '') = '' THEN 'technical' "
        f"WHEN LOWER(COALESCE(CAST({code_expr} AS STRING), '')) = 'tapahtumia' THEN 'technical' "
        + technical_conditions
        +
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%nettolainanotto%' THEN 'financing' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%velanhallinta%' THEN 'financing' "
        f"WHEN CAST({code_expr} AS STRING) LIKE '15.%' THEN 'financing' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%arvonlisäveromenot%' THEN 'expense' "
        f"WHEN REGEXP_CONTAINS(COALESCE(CAST({code_expr} AS STRING), ''), r'^(11|12|13|14)\\.') THEN 'revenue' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%arvonlisävero%' AND LOWER(COALESCE(CAST({name_expr} AS STRING), '')) NOT LIKE '%arvonlisäveromenot%' THEN 'revenue' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%tulovero%' THEN 'revenue' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%yhteisövero%' THEN 'revenue' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%energiavero%' THEN 'revenue' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%myyntitulot%' THEN 'revenue' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%osinkotulot%' THEN 'revenue' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%toimintamenot%' THEN 'expense' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%rahoitus%' THEN 'expense' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%tuki%' THEN 'expense' "
        f"WHEN LOWER(COALESCE(CAST({name_expr} AS STRING), '')) LIKE '%avustus%' THEN 'expense' "
        f"WHEN LOWER(COALESCE(CAST({hall_expr} AS STRING), '')) LIKE '%hallinnonala%' THEN 'expense' "
        "ELSE 'expense' END"
    )
