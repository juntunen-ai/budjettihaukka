"""Conversational budget-analysis agent (Milestone B MVP).

Gemini with function-calling tools over the governed data platform:
every SQL statement the model writes passes the same security gate
(SELECT-only, table whitelist, year clamp, LIMIT cap, cost ceiling) as the
deterministic pipeline, and concept grounding prefers the human-decided
concept→code maps.

The agent's contract with the user, enforced by the system prompt:
answer + how-it-was-computed (definition, codes, metric) + uncertainties +
suggested follow-ups, in the user's language.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from google import genai
from google.genai import types

from config import settings
from utils import concept_code_map_utils
from utils.analysis_spec_utils import infer_analysis_spec
from utils.bigquery_utils import _prepare_sql_for_execution, run_sql_query, get_last_execution_meta

logger = logging.getLogger("agent_engine")

MAX_TOOL_ROWS = 150
CHAT_LOG_PATH = Path("agent_data/chat_observability.jsonl")


# --------------------------------------------------------------------------
# Tools (exposed to Gemini via automatic function calling)
# --------------------------------------------------------------------------

def get_data_overview() -> dict:
    """Describes the available budget data: tables, columns, metrics and their meaning.

    Call this first if unsure how to query.
    """
    project, dataset = settings.project_id, settings.dataset
    return {
        "tables": {
            f"{project}.{dataset}.concept_yearly_totals_real_v1": {
                "description": (
                    "Curated concept-level yearly totals from human-decided definitions, "
                    "BOTH nominal and real (deflated with Statistics Finland cost-of-living "
                    "index). PREFER THIS for concept trend questions. Tiny and cheap. "
                    "For ranges longer than ~5 years, report REAL euros or show both."
                ),
                "columns": {
                    "concept": "concept id, e.g. 'koulutus', 'tutkimus', 'kulttuuri'",
                    "vuosi": "year (INT64)",
                    "role": "'include' core spending | 'component' separable part | 'exclude' money mapped away",
                    "component": "component name when role='component', e.g. 'opintotuki'",
                    "target_concept": "where excluded money belongs",
                    "total_meur_nominal": "millions of euros, nominal (toteuma/nettokertymä)",
                    "total_meur_real": "millions of euros in latest-year prices",
                    "real_base_year": "price base year of total_meur_real",
                },
                "usage": "Concept total = SUM(total_meur_*) WHERE role IN ('include','component').",
            },
            f"{project}.{dataset}.budget_vs_actual_v1": {
                "description": (
                    "BUDGETED vs ACTUAL per momentti per year (2014->). Use when the "
                    "user asks what was BUDGETED ('budjetoitiin', 'talousarvio', "
                    "'määräraha') or wants budget-vs-outcome comparison. tae_eur = "
                    "government budget proposal, ltae_eur = supplementary budgets, "
                    "budjetoitu_eur = their sum, toteuma_eur = actual bookkeeping."
                ),
                "columns": {
                    "vuosi": "year (2014->)", "momentti_koodi": "moment code e.g. '29.10.30.'",
                    "momentti_nimi": "moment name", "puoli": "'meno' | 'tulo'",
                    "tae_eur": "budget proposal EUR", "ltae_eur": "supplementary budgets EUR",
                    "budjetoitu_eur": "total budgeted EUR", "toteuma_eur": "actual EUR",
                    "toteuma_aste": "actual/budgeted ratio",
                },
                "usage": "Always constrain vuosi. Momentti-level only; no concept mapping yet.",
            },
            f"{project}.{dataset}.structural_events_v1": {
                "description": (
                    "Known structural breaks and one-off events (reforms) that create level "
                    "shifts in series. ALWAYS check this before interpreting a jump/drop as a "
                    "real spending change, and mention relevant events in the answer."
                ),
                "columns": {
                    "year": "event year",
                    "label_fi": "short name, e.g. 'Sote-uudistus / hyvinvointialueet'",
                    "description_fi": "what the event does to the series",
                    "affects_concepts": "ARRAY of concept ids affected",
                },
                "usage": "SELECT year, label_fi, description_fi FROM ... WHERE 'koulutus' IN UNNEST(affects_concepts)",
            },
            f"{project}.{dataset}.valtiontalous_yearly_agg_v1": {
                "description": "Yearly sums per momentti and talousarviotili. Alamomentti queries are disabled until official chart validation is complete.",
                "columns": {
                    "vuosi": "year", "hallinnonala": "administrative branch name",
                    "momentti_tunnusp": "budget moment code, e.g. '29.10.30.'",
                    "momentti_snimi": "moment name",
                    "talousarviotili_tunnusp": "full budget-account code below or equal to moment",
                    "talousarviotili_snimi": "budget-account name",
                    "maararahalaji_tunnus": "appropriation type code; never a sub-moment",
                    "maararahalaji_snimi": "appropriation type name; never a sub-moment",
                    "nettokertyma_sum": "yearly actual net accrual in euros (NOT millions)",
                },
            },
            f"{project}.{dataset}.{settings.table}": {
                "description": (
                    "Monthly row-level semantic view (large: ~1.6GB scans; the cost gate may "
                    "reject queries). Only for monthly detail; prefer the yearly tables."
                ),
                "columns": {
                    "`Vuosi`": "year (STRING, cast with SAFE_CAST)", "`Kk`": "month",
                    "`Momentti_TunnusP`": "moment code", "momentti_canonical": "canonical moment name",
                    "talousarviotili_tunnusp": "budget-account code",
                    "maararahalaji_tunnus": "appropriation type code, not sub-moment",
                    "`Nettokertymä`": "net accrual EUR", "hallinnonala_canonical": "canonical branch",
                },
            },
        },
        "data_coverage": "Years 1998–2026 (2026 partial, through May). Amounts are nominal euros, actual bookkeeping (toteuma).",
        "caveats": [
            "Momentti codes are REUSED across eras — always constrain years when using code prefixes.",
            "Expense/revenue sign conventions vary by side; revenue moments (osasto 11-15) can be negative.",
            "Municipal service funding since 2010 flows via lump-sum valtionosuus (28.90.30) and cannot be split by sector.",
        ],
    }


def resolve_concept(term: str) -> dict:
    """Resolves a topic/concept mentioned by the user (in any language) to its
    grounding: a curated human-decided definition if one exists, otherwise
    heuristic ontology matching. ALWAYS call this before answering a concept
    question (education, defence, koulutus, puolustus, ...).
    """
    spec = infer_analysis_spec(term)
    concept_id = spec.resolved_concept_id
    # Direct lookups for common English terms the Finnish parser misses.
    if not concept_id:
        english_map = {
            "education": "koulutus", "defence": "puolustus", "defense": "puolustus",
            "health": "terveys", "research": "tutkimus", "culture": "kulttuuri",
            "social security": "sosiaaliturva",
        }
        lowered = term.lower()
        for eng, fin in english_map.items():
            if eng in lowered:
                concept_id = fin
                break
    if not concept_id:
        return {"resolved": False, "hint": "No concept matched; query by momentti code/name instead, and say so."}

    meta = concept_code_map_utils.definition_meta(concept_id)
    if meta:
        return {
            "resolved": True,
            "concept_id": concept_id,
            "grounding": "curated_human_decided_map",
            "definition": meta,
            "how_to_query": (
                "SELECT vuosi, SUM(total_meur_nominal) nominal_meur, SUM(total_meur_real) real_meur "
                "FROM concept_yearly_totals_real_v1 "
                f"WHERE concept='{concept_code_map_utils.CONCEPT_ID_ALIASES.get(concept_id, concept_id)}' "
                "AND role IN ('include','component') GROUP BY vuosi"
            ),
            "check_structural_events": (
                f"SELECT year, label_fi, description_fi FROM structural_events_v1 "
                f"WHERE '{concept_id}' IN UNNEST(affects_concepts)"
            ),
            "must_disclose": meta.get("disclosure_fi"),
        }
    return {
        "resolved": True,
        "concept_id": concept_id,
        "grounding": "heuristic_ontology_match",
        "warning": (
            "No human-reviewed definition exists for this concept yet — results are "
            "name-matching based and LESS RELIABLE. Say this openly in the answer."
        ),
    }


def run_sql(sql: str) -> dict:
    """Runs a BigQuery SELECT through the security gate (table whitelist,
    year clamp, LIMIT cap, cost ceiling). Returns rows as JSON (max 150).
    Allowed tables: concept_yearly_totals_v1, valtiontalous_yearly_agg_v1,
    valtiontalous_yearly_agg_guarded_v1, and the monthly semantic view.
    """
    secured_sql, error = _prepare_sql_for_execution(sql)
    if error or not secured_sql:
        return {"ok": False, "error": error or "SQL preparation failed", "hint": "Fix the SQL and retry."}
    df = run_sql_query(secured_sql)
    meta = get_last_execution_meta()
    if meta.get("error") or (df is None):
        return {"ok": False, "error": str(meta.get("error", "query failed"))[:400]}
    truncated = len(df) > MAX_TOOL_ROWS
    rows = json.loads(df.head(MAX_TOOL_ROWS).to_json(orient="records"))
    return {
        "ok": True,
        "row_count": int(len(df)),
        "truncated_to": MAX_TOOL_ROWS if truncated else None,
        "rows": rows,
    }


AGENT_TOOLS = [get_data_overview, resolve_concept, run_sql]

SYSTEM_PROMPT = """You are Budjettihaukka, an analyst of the Finnish state budget
(data: Valtiokonttori bookkeeping 1998–2026/05). You answer in the user's language.

NON-NEGOTIABLE RULES:
1. Ground every concept question via resolve_concept BEFORE querying. If the
   grounding is a curated human-decided map, use its how_to_query guidance and
   state the definition (name, version) in the answer. If grounding is heuristic,
   say openly that no reviewed definition exists yet and results are approximate.
2. Never invent numbers. Every figure must come from run_sql results. If a query
   fails or data does not cover the question, say so.
3. ALWAYS include in the answer, briefly and readably:
   - the answer itself, leading with the key number/finding
   - "Näin laskin": definition used, metric (toteuma/nettokertymä, nominal euros),
     and inclusion scope
   - Uncertainties that apply (e.g. the municipal valtionosuus disclosure,
     nominal-vs-real over long ranges, partial year 2026)
4. Amounts in concept_yearly_totals_* are MILLIONS of euros; in other tables plain
   euros. Convert and label units clearly (e.g. "6,6 mrd €").
4b. METRIC CHOICE: "budjetoitiin / talousarvio / määräraha" -> budgeted figures
   (budget_vs_actual_v1, 2014->); "käytettiin / meni / kului / toteuma" -> actual
   bookkeeping (concept/yearly tables). If the question is ambiguous about which,
   default to actual AND state which metric you used. Before 2014 only actuals
   exist — say so if asked for budgeted figures.
5. If the question is ambiguous, ask ONE clarifying question instead of guessing.
6. Sanity-check results: before interpreting any jump or drop as a real change,
   query structural_events_v1 for the concept — reforms like the 2010 VOS or the
   2023 sote transfer create level shifts that are NOT spending changes. Mention
   the relevant event in the answer when it falls inside the asked range.
6b. For ranges longer than ~5 years, use real euros (total_meur_real) or present
   both — nominal growth over decades is mostly inflation, and saying only the
   nominal figure misleads.
7. End with exactly the marker line: FOLLOW_UPS: q1 | q2 | q3 (three short
   suggested follow-up questions in the user's language).

Style: concise, numerate, honest about limits. Use Finnish budget terminology
correctly (momentti, pääluokka, hallinnonala, valtionosuus)."""


@dataclass
class AgentTurn:
    answer: str
    follow_ups: list[str] = field(default_factory=list)
    elapsed_s: float = 0.0
    tool_calls: list[str] = field(default_factory=list)
    error: str | None = None


FALLBACK_MODELS = ["gemini-2.5-flash", "gemini-2.0-flash"]


class BudgetAgent:
    """Stateful conversation wrapper around Gemini + tools.

    Falls back through FALLBACK_MODELS on capacity errors (503), carrying the
    text-level conversation history over so the thread survives the switch.
    """

    def __init__(self, model: str | None = None):
        if not settings.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured (.env.local)")
        self._client = genai.Client(api_key=settings.gemini_api_key)
        preferred = model or settings.gemini_model
        self._model_candidates = [preferred] + [m for m in FALLBACK_MODELS if m != preferred]
        self._model_index = 0
        self._history: list[types.Content] = []
        self._chat = self._make_chat()

    @property
    def active_model(self) -> str:
        return self._model_candidates[self._model_index]

    def _make_chat(self):
        return self._client.chats.create(
            model=self.active_model,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                tools=AGENT_TOOLS,
                temperature=0.2,
            ),
            history=list(self._history),
        )

    def _remember(self, question: str, answer: str) -> None:
        self._history.append(types.Content(role="user", parts=[types.Part(text=question)]))
        self._history.append(types.Content(role="model", parts=[types.Part(text=answer)]))

    def ask(self, question: str) -> AgentTurn:
        started = time.time()
        response = None
        last_error: Exception | None = None
        while self._model_index < len(self._model_candidates):
            for attempt in range(2):
                try:
                    response = self._chat.send_message(question)
                    break
                except Exception as error:
                    last_error = error
                    text_err = str(error)
                    if "429" in text_err:
                        # Free-tier per-minute quota: honor the API's retry
                        # hint instead of burning the window with fast retries.
                        import re as _re

                        hint = _re.search(r"retry in (\d+(?:\.\d+)?)s", text_err)
                        wait_s = min(60.0, float(hint.group(1)) + 2 if hint else 35.0)
                        if attempt == 0:
                            time.sleep(wait_s)
                            continue
                        break
                    if "503" in text_err or "UNAVAILABLE" in text_err:
                        time.sleep(3 * (attempt + 1))
                        continue
                    break
            if response is not None:
                break
            capacity_issue = last_error is not None and (
                "503" in str(last_error) or "UNAVAILABLE" in str(last_error) or "429" in str(last_error)
            )
            if capacity_issue and self._model_index + 1 < len(self._model_candidates):
                self._model_index += 1
                logger.warning("Model capacity issue; falling back to %s", self.active_model)
                self._chat = self._make_chat()
                continue
            break
        if response is None:
            logger.error("Agent turn failed: %s", last_error)
            return AgentTurn(
                answer="",
                error=f"Agentin vastaus epäonnistui: {str(last_error)[:300]}",
                elapsed_s=time.time() - started,
            )
        text = (response.text or "").strip()
        tool_calls = []
        history = getattr(response, "automatic_function_calling_history", None) or []
        for content in history:
            for part in getattr(content, "parts", None) or []:
                fc = getattr(part, "function_call", None)
                if fc is not None and getattr(fc, "name", None):
                    tool_calls.append(fc.name)

        answer, follow_ups = _split_follow_ups(text)
        self._remember(question, text)
        turn = AgentTurn(
            answer=answer,
            follow_ups=follow_ups,
            elapsed_s=time.time() - started,
            tool_calls=tool_calls,
        )
        _log_turn(question, turn)
        return turn


def _split_follow_ups(text: str) -> tuple[str, list[str]]:
    marker = "FOLLOW_UPS:"
    if marker not in text:
        return text, []
    body, _, tail = text.rpartition(marker)
    follow_ups = [q.strip(" -•") for q in tail.split("|") if q.strip()][:3]
    return body.strip(), follow_ups


def _log_turn(question: str, turn: AgentTurn) -> None:
    try:
        CHAT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with CHAT_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        "question": question,
                        "answer_chars": len(turn.answer),
                        "tool_calls": turn.tool_calls,
                        "elapsed_s": round(turn.elapsed_s, 2),
                        "error": turn.error,
                        "model": settings.gemini_model,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    except Exception:
        logger.warning("chat observability log write failed", exc_info=True)
