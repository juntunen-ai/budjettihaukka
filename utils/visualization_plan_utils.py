from __future__ import annotations

from utils.analysis_spec_utils import AnalysisSpec


def extract_intent_signals(question: str) -> dict[str, bool]:
    text = (question or "").lower()
    return {
        "trend": any(token in text for token in ("kehitty", "trend", "aikasarja", "over time")),
        "growth": any(token in text for token in ("kasv", "muutos", "yoy", "vuosimuutos")),
        "comparison": any(token in text for token in ("vertaa", "vs", "between", "välillä")),
        "composition": any(token in text for token in ("jakauma", "osuus", "hallinnonaloittain", "rakenn")),
        "top": any(token in text for token in ("top", "suurin", "eniten", "korkein")),
        "seasonality": any(token in text for token in ("kuukaus", "kausi", "season")),
    }


def template_order(spec: AnalysisSpec, intent_signals: dict[str, bool]) -> list[str]:
    if spec.intent == "top_growth":
        return ["top_growth", "trend", "growth"]
    if spec.intent == "growth":
        return ["trend", "growth", "top_categories"]
    if spec.intent == "trend":
        return ["trend", "growth", "top_categories"]
    if spec.intent == "composition":
        return ["composition", "trend", "top_categories"]
    if spec.intent == "seasonality":
        return ["seasonality", "trend", "top_categories"]
    if intent_signals.get("seasonality"):
        return ["seasonality", "trend", "top_categories"]
    if intent_signals.get("composition"):
        return ["composition", "trend", "top_categories"]
    return ["trend", "top_categories", "growth", "seasonality"]

