import logging
import os
import json
import re
from typing import Any

import vertexai
from google.cloud import bigquery
from vertexai.generative_models import GenerationConfig, GenerativeModel

from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Backward-compatible exports used by other modules.
PROJECT_ID = settings.project_id
LOCATION = settings.location
TABLE_ID = settings.demo_sql_table if settings.use_google_sheets_demo else settings.full_table_id

ALLOWED_INTENTS = {"overview", "trend", "growth", "top_growth", "composition", "seasonality"}
ALLOWED_ENTITY_LEVELS = {"kokonais", "hallinnonala", "momentti", "alamomentti", "molemmat"}
ALLOWED_GROWTH_TYPES = {"absolute", "pct"}
ALLOWED_METRICS = {"nettokertyma"}


def _get_model() -> GenerativeModel:
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    return GenerativeModel(settings.gemini_model)


def _generate_sql_with_aistudio(prompt: str) -> str:
    from google import genai
    from google.genai import types

    api_key = settings.gemini_api_key or ""
    # Avoid duplicate-key warnings from SDK when both vars exist.
    if os.getenv("GOOGLE_API_KEY") and os.getenv("GEMINI_API_KEY"):
        os.environ.pop("GOOGLE_API_KEY", None)

    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model=settings.gemini_model,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.2,
            top_p=0.8,
            max_output_tokens=512,
        ),
    )
    return (response.text or "").strip()


def _generate_sql_with_vertex(prompt: str) -> str:
    model = _get_model()
    generation_config = GenerationConfig(
        temperature=0.2,
        top_p=0.8,
        max_output_tokens=512,
    )
    response = model.generate_content(prompt, generation_config=generation_config)
    return response.text.strip()


def _strip_code_fence(text: str) -> str:
    value = (text or "").strip()
    if value.startswith("```"):
        value = re.sub(r"^```(?:json|sql)?\s*", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s*```$", "", value)
    return value.strip()


def _extract_json_object(text: str) -> dict[str, Any] | None:
    cleaned = _strip_code_fence(text)
    if not cleaned:
        return None
    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    # Fallback: poimi ensimmäinen {...} lohko.
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        parsed = json.loads(cleaned[start : end + 1])
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _sanitize_query_plan(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    plan: dict[str, Any] = {}
    intent = str(raw.get("intent", "")).strip().lower()
    metric = str(raw.get("metric", "")).strip().lower()
    entity_level = str(raw.get("entity_level", "")).strip().lower()
    growth_type = str(raw.get("growth_type", "")).strip().lower()

    if intent in ALLOWED_INTENTS:
        plan["intent"] = intent
    if metric in ALLOWED_METRICS:
        plan["metric"] = metric
    if entity_level in ALLOWED_ENTITY_LEVELS:
        plan["entity_level"] = entity_level
    if growth_type in ALLOWED_GROWTH_TYPES:
        plan["growth_type"] = growth_type

    for key in ("time_from", "time_to", "ranking_n"):
        value = raw.get(key)
        if value is None or value == "":
            continue
        try:
            plan[key] = int(value)
        except Exception:
            continue

    return plan or None


def generate_query_plan_from_natural_language(
    question: str,
    fallback_plan: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Generate a structured QueryPlan JSON. Returns None on failure."""
    if not question or not question.strip():
        return None

    fallback = fallback_plan or {}
    prompt = f"""Muodosta kysymyksestä yksi JSON-objekti nimeltä QueryPlan.

Palauta VAIN JSON. Ei markdownia, ei selityksiä.

Sallitut arvot:
- intent: {sorted(ALLOWED_INTENTS)}
- metric: {sorted(ALLOWED_METRICS)}
- entity_level: {sorted(ALLOWED_ENTITY_LEVELS)}
- growth_type: {sorted(ALLOWED_GROWTH_TYPES)}

Kentät:
{{
  "intent": "overview|trend|growth|top_growth|composition|seasonality",
  "metric": "nettokertyma",
  "entity_level": "kokonais|hallinnonala|momentti|alamomentti|molemmat",
  "growth_type": "absolute|pct",
  "time_from": 1998,
  "time_to": 2024,
  "ranking_n": 10
}}

Jos kenttä ei ole pääteltävissä, käytä fallback-arvoja:
{json.dumps(fallback, ensure_ascii=False)}

Kysymys:
{question}
"""
    try:
        if settings.llm_provider == "aistudio":
            raw_text = _generate_sql_with_aistudio(prompt)
        else:
            raw_text = _generate_sql_with_vertex(prompt)
        parsed = _extract_json_object(raw_text)
        return _sanitize_query_plan(parsed or {})
    except Exception as e:
        logger.error("Error generating QueryPlan JSON: %s", e)
        return None


def _get_schema_context() -> str:
    if settings.use_google_sheets_demo:
        from utils.demo_data_utils import get_demo_schema_context

        return get_demo_schema_context()

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = client.get_table(TABLE_ID)
    return "\n".join(
        f"- `{field.name}` : {field.description} ({field.field_type})"
        for field in table_ref.schema
    )


def generate_sql_from_natural_language(question: str) -> str:
    """Generate a BigQuery SELECT statement from a natural-language question."""
    try:
        schema_context = _get_schema_context()
        table_for_prompt = TABLE_ID

        if settings.use_google_sheets_demo:
            prompt = f"""SQLite-taulu: `{table_for_prompt}`

Sarakkeet ja tietotyypit
------------------------
{schema_context}

Ohjeita mallille
----------------
- Käytä vain taulua `{table_for_prompt}`.
- Käytä SQLite-yhteensopivaa SQL:ää.
- Vältä BigQuery-spesifejä funktioita.
- Palauta vain SQL SELECT -lause, joka alkaa sanalla SELECT.
- Älä sisällytä kommentteja, otsikoita tai muuta ylimääräistä.

Kysymys: {question}

Luo SQL-kysely yllä olevaan kysymykseen käyttäen taulua `{table_for_prompt}`.
Palauta AINOASTAAN SQL-kysely. Älä lisää mitään selityksiä tai kommentteja.

SQL-kysely:
"""
        else:
            prompt = f"""BigQuery-taulu: `{table_for_prompt}`

Sarakkeet ja selitteet
----------------------
{schema_context}

Ohjeita mallille
----------------
- Käytä backtick-merkkejä (`) taulun ja sarakkeiden nimissä.
- Palauta vain SQL SELECT -lause, joka alkaa sanalla SELECT.
- Älä sisällytä kommentteja, otsikoita tai muuta ylimääräistä.

Kysymys: {question}

Luo BigQuery SQL-kysely, joka hakee vastauksen yllä olevaan kysymykseen käyttäen taulua `{table_for_prompt}`.
Palauta AINOASTAAN SQL-kysely. Älä lisää mitään selityksiä tai kommentteja.

SQL-kysely:
"""

        if settings.llm_provider == "aistudio":
            sql_text = _generate_sql_with_aistudio(prompt)
        else:
            sql_text = _generate_sql_with_vertex(prompt)
        logger.debug("Raw model response: %s", sql_text)

        if "```sql" in sql_text:
            start = sql_text.find("```sql") + 6
            end = sql_text.rfind("```")
            sql_text = sql_text[start:end].strip()
        elif "```" in sql_text:
            start = sql_text.find("```") + 3
            end = sql_text.rfind("```")
            sql_text = sql_text[start:end].strip()

        if not sql_text.lower().startswith("select"):
            logger.error("Generated SQL does not start with SELECT: %s", sql_text)
            return ""

        if settings.use_google_sheets_demo:
            from utils.demo_data_utils import adapt_sql_to_demo_table

            sql_text = adapt_sql_to_demo_table(sql_text)
        elif TABLE_ID in sql_text and f"`{TABLE_ID}`" not in sql_text:
            sql_text = sql_text.replace(TABLE_ID, f"`{TABLE_ID}`")

        logger.info("Final SQL query generated with provider=%s.", settings.llm_provider)
        return sql_text
    except Exception as e:
        logger.error("Error generating content with model: %s", e)
        return ""
