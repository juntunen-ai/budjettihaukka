import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
import logging
from google.cloud import bigquery

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Asetukset ---
PROJECT_ID = "valtion-budjetti-data"
LOCATION = "us-central1"

vertexai.init(project=PROJECT_ID, location=LOCATION)

# Valitaan malli
model = GenerativeModel("gemini-2.0-flash-001")

# --- Funktio: Generoi SQL luonnollisesta kielestä ---
def generate_sql_from_natural_language(question: str) -> str:
    """
    Generates an SQL query using Vertex AI Gemini model from a natural language question.

    Parameters:
        question (str): The natural language question to convert into an SQL query.
    """
    # Fetch schema dynamically (if needed)
    try:
        client = bigquery.Client()
        table_ref = client.get_table(f"{PROJECT_ID}.valtiodata.budjettidata")
        schema_context = "\n".join([
            f"- `{field.name}` : {field.description} ({field.field_type})"
            for field in table_ref.schema
        ])
    except Exception as e:
        logger.error(f"Failed to fetch schema from BigQuery: {e}")
        return ""

    # Prompt construction
    prompt = f"""BigQuery-taulu: `{PROJECT_ID}.valtiodata.budjettidata`

Sarakkeet ja selitteet
----------------------
{schema_context}

Ohjeita mallille
----------------
• Käytä backtick-merkkejä (`) taulun ja sarakkeiden nimissä.
• Palauta vain SQL SELECT -lause.
• Älä sisällytä kommentteja, otsikoita tai muuta ylimääräistä.

Kysymys: {question}

Luo BigQuery SQL-kysely, joka hakee vastauksen yllä olevaan kysymykseen käyttäen taulua `{PROJECT_ID}.valtiodata.budjettidata`.
Palauta AINOASTAAN SQL-kysely. Älä lisää mitään selityksiä tai kommentteja.

SQL-kysely:
"""

    try:
        generation_config = GenerationConfig(
            temperature=0.2,
            top_p=0.8,
            max_output_tokens=512
        )
        response = model.generate_content(prompt, generation_config=generation_config)
        sql_text = response.text.strip()

        # Validate and clean SQL response
        if not sql_text:
            logger.warning("Generated SQL is empty.")
            return ""

        if sql_text.startswith("```sql"):
            sql_text = sql_text.replace("```sql", "").strip()
        if sql_text.endswith("```"):
            sql_text = sql_text[:-3].strip()

        # Sanitize the generated SQL to remove invalid characters
        sql_text = sql_text.encode("utf-8", "ignore").decode("utf-8")

        # Ensure all table and column names are enclosed in backticks
        import re
        sql_text = re.sub(r'(?<!`)\b(\w+)\b(?!`)', r'`\1`', sql_text)

        # Log the generated SQL for debugging
        logger.info(f"Generated SQL: {sql_text}")

        return sql_text
    except Exception as e:
        logger.error(f"Error generating SQL with Vertex AI: {e}")
        return ""
