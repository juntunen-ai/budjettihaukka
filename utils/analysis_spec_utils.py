from __future__ import annotations

import re
from dataclasses import dataclass, field

DATA_MIN_YEAR = 1998
DATA_MAX_YEAR = 2025


@dataclass(frozen=True)
class ClarificationField:
    field: str
    question: str
    options: tuple[str, ...]
    recommended: str


@dataclass
class AnalysisSpec:
    intent: str
    metric: str
    entity_level: str
    growth_type: str
    requested_time_from: int | None
    requested_time_to: int | None
    time_from: int | None
    time_to: int | None
    ranking_n: int | None
    confidence: float
    assumptions: list[str] = field(default_factory=list)
    clarifications: list[ClarificationField] = field(default_factory=list)


def _extract_years(text: str) -> list[int]:
    return [int(m) for m in re.findall(r"\b(?:19|20)\d{2}\b", text or "")]


def _extract_top_n(text: str) -> int | None:
    for pattern in (r"\btop\s*(\d{1,3})\b", r"\b(\d{1,3})\s*(suurin|eniten)\b"):
        m = re.search(pattern, text or "", flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None


def _normalize_time_bounds(years: list[int]) -> tuple[int | None, int | None]:
    if not years:
        return None, None
    if len(years) == 1:
        y = years[0]
        return max(min(y, DATA_MAX_YEAR), DATA_MIN_YEAR), max(min(y, DATA_MAX_YEAR), DATA_MIN_YEAR)
    start = max(min(min(years), DATA_MAX_YEAR), DATA_MIN_YEAR)
    end = max(min(max(years), DATA_MAX_YEAR), DATA_MIN_YEAR)
    if start > end:
        start, end = end, start
    return start, end


def _requested_time_bounds(years: list[int]) -> tuple[int | None, int | None]:
    if not years:
        return None, None
    if len(years) == 1:
        return years[0], years[0]
    return min(years), max(years)


def infer_analysis_spec(question: str) -> AnalysisSpec:
    text = (question or "").lower()
    years = _extract_years(text)
    requested_from, requested_to = _requested_time_bounds(years)
    time_from, time_to = _normalize_time_bounds(years)
    ranking_n = _extract_top_n(text)

    has_growth = any(
        token in text
        for token in (
            "kasv",
            "muutos",
            "mutos",
            "muuttu",
            "nous",
            "nousi",
            "noussut",
            "laski",
            "yoy",
            "vuosimuutos",
        )
    )
    has_top = any(token in text for token in ("top", "eniten", "suurin", "suur", "absoluutt"))
    has_trend = any(token in text for token in ("trend", "aikasarja", "kehitty", "kehitys", "pitkaaik"))
    has_composition = any(token in text for token in ("jakauma", "osuus", "osuu", "raken", "rakenn"))
    has_seasonality = any(token in text for token in ("kuukaus", "kausivaihtelu", "season", "kausi"))
    has_moment = "moment" in text
    has_alamoment = "alamoment" in text

    if has_top and has_growth:
        intent = "top_growth"
    elif has_seasonality:
        intent = "seasonality"
    elif has_composition:
        intent = "composition"
    elif has_growth:
        intent = "growth"
    elif has_trend:
        intent = "trend"
    else:
        intent = "overview"

    if has_moment and has_alamoment:
        entity_level = "molemmat"
    elif has_alamoment:
        entity_level = "alamomentti"
    elif has_moment:
        entity_level = "momentti"
    elif "hallinnonala" in text or "hallinnonal" in text:
        entity_level = "hallinnonala"
    else:
        entity_level = "kokonais"

    if any(token in text for token in ("prosent", "%", "pct")):
        growth_type = "pct"
    elif any(token in text for token in ("absoluutt", "euro", "eur")):
        growth_type = "absolute"
    else:
        growth_type = "unknown"

    confidence = 0.90
    assumptions: list[str] = []
    clarifications_candidates: list[ClarificationField] = []
    missing_key_dimensions = False

    if growth_type == "unknown" and intent in {"growth", "top_growth"}:
        clarifications_candidates.append(
            ClarificationField(
                field="growth_type",
                question="Kasvun mittaustapa",
                options=("Absoluuttinen kasvu (€)", "Suhteellinen kasvu (%)"),
                recommended="Absoluuttinen kasvu (€)",
            )
        )
        missing_key_dimensions = True
        assumptions.append("Kasvutyyppi oletetaan absoluuttiseksi (€), ellei toisin valita.")
        confidence -= 0.15

    if intent == "top_growth" and entity_level in {"momentti", "hallinnonala", "kokonais"}:
        clarifications_candidates.append(
            ClarificationField(
                field="entity_level",
                question="Tarkastelutaso",
                options=("Molemmat", "Momentti", "Alamomentti", "Hallinnonala"),
                recommended="Molemmat",
            )
        )
        missing_key_dimensions = True
        assumptions.append("Tasoksi oletetaan sekä momentit että alamomentit.")
        confidence -= 0.10

    if ranking_n is None and intent == "top_growth":
        ranking_n = 10
        assumptions.append("Top-listan koko oletetaan 10 visualisointiin ja 100 taulukkoon.")
        confidence -= 0.06

    if years and (min(years) < DATA_MIN_YEAR or max(years) > DATA_MAX_YEAR):
        assumptions.append(
            f"Aikaväli rajataan datan saatavuuteen {DATA_MIN_YEAR}-{DATA_MAX_YEAR}."
        )
        confidence -= 0.05

    if time_from is None or time_to is None:
        assumptions.append("Aikaväliä ei annettu selkeästi, käytetään laajaa saatavilla olevaa jaksoa.")
        confidence -= 0.10

    should_ask_clarifications = confidence < 0.75 or missing_key_dimensions
    clarifications = clarifications_candidates[:2] if should_ask_clarifications else []

    return AnalysisSpec(
        intent=intent,
        metric="nettokertyma",
        entity_level=entity_level,
        growth_type=growth_type if growth_type != "unknown" else "absolute",
        requested_time_from=requested_from,
        requested_time_to=requested_to,
        time_from=time_from,
        time_to=time_to,
        ranking_n=ranking_n,
        confidence=max(0.0, min(1.0, confidence)),
        assumptions=assumptions,
        clarifications=clarifications,
    )


def apply_clarifications_to_question(question: str, selections: dict[str, str]) -> str:
    if not selections:
        return question
    hints = []
    growth_val = selections.get("growth_type", "")
    if growth_val:
        if "Suhteellinen" in growth_val:
            hints.append("käytä suhteellista kasvua prosentteina")
        elif "Absoluuttinen" in growth_val:
            hints.append("käytä absoluuttista kasvua euroissa")

    level_val = selections.get("entity_level", "")
    if level_val:
        if level_val == "Molemmat":
            hints.append("taso momentti ja alamomentti")
        elif level_val == "Momentti":
            hints.append("taso momentti")
        elif level_val == "Alamomentti":
            hints.append("taso alamomentti")
        elif level_val == "Hallinnonala":
            hints.append("taso hallinnonala")

    if not hints:
        return question
    return f"{question}\nLisätarkenne: {', '.join(hints)}."


def renderable_summary(spec: AnalysisSpec) -> str:
    requested_txt = "ei määritelty"
    if spec.requested_time_from is not None and spec.requested_time_to is not None:
        requested_txt = f"{spec.requested_time_from}-{spec.requested_time_to}"

    effective_txt = "ei määritelty"
    if spec.time_from is not None and spec.time_to is not None:
        effective_txt = f"{spec.time_from}-{spec.time_to}"
    rank_txt = f"Top {spec.ranking_n}" if spec.ranking_n else "Top ei määritelty"
    return (
        f"Intentti: {spec.intent} | Mittari: {spec.metric} | Taso: {spec.entity_level} | "
        f"Kasvu: {spec.growth_type} | Pyyntöaikaväli: {requested_txt} | Käytetty aikaväli: {effective_txt} | {rank_txt} | "
        f"Luottamus: {spec.confidence:.0%}"
    )


def coverage_notice(spec: AnalysisSpec) -> str:
    if spec.requested_time_from is None or spec.requested_time_to is None:
        return ""
    if spec.time_from is None or spec.time_to is None:
        return (
            f"Pyyntö {spec.requested_time_from}-{spec.requested_time_to} on datan saatavuuden ulkopuolella "
            f"({DATA_MIN_YEAR}-{DATA_MAX_YEAR})."
        )
    if spec.requested_time_from != spec.time_from or spec.requested_time_to != spec.time_to:
        return (
            f"Pyyntö {spec.requested_time_from}-{spec.requested_time_to} rajattiin datan saatavuuteen "
            f"{spec.time_from}-{spec.time_to}."
        )
    return ""
