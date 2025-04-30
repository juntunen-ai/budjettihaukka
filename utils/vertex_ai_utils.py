import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
import logging
from google.cloud import bigquery
from google.cloud import bigquery_storage  # Preferred

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Asetukset ---
PROJECT_ID = "valtion-budjetti-data"
LOCATION = "us-central1"

vertexai.init(project=PROJECT_ID, location=LOCATION)

# Valitaan malli
model = GenerativeModel("gemini-2.5-pro-preview-03-25")

# Initialize BigQuery Storage client
bq_storage_client = bigquery_storage.BigQueryReadClient()

# --- Funktio: Generoi SQL luonnollisesta kielestä ---
def generate_sql_from_natural_language(question: str) -> str:
    """
    Generates an SQL query using Vertex AI Gemini model from a natural language question.
    """
    try:
        # Initialize Vertex AI
        vertexai.init(project=PROJECT_ID, location=LOCATION)

        # Get model
        model = GenerativeModel("gemini-2.5-pro-preview-03-25")

        # Fetch schema dynamically (if needed)
        client = bigquery.Client()
        table_ref = client.get_table(f"{PROJECT_ID}.valtiodata.budjettidata")
        schema_context = "\n".join([
            f"- `{field.name}` : {field.description} ({field.field_type})"
            for field in table_ref.schema
        ])

        # API schema context stays the same
        api_schema_context = """
        ### 📊 Budjettitaloudentapahtumat-endpointin parametrit

        # [The rest of your API context stays the same]
        """

        # Prompt construction
        prompt = f"""BigQuery-taulu: `{PROJECT_ID}.valtiodata.budjettidata`

Sarakkeet ja selitteet
----------------------
{schema_context}

Ohjeita mallille
----------------
- Käytä backtick-merkkejä (`) taulun ja sarakkeiden nimissä.
- Palauta vain SQL SELECT -lause, joka alkaa sanalla SELECT.
- Älä sisällytä kommentteja, otsikoita tai muuta ylimääräistä.

{api_schema_context}

Kysymys: {question}

Luo BigQuery SQL-kysely, joka hakee vastauksen yllä olevaan kysymykseen käyttäen taulua `{PROJECT_ID}.valtiodata.budjettidata`.
Palauta AINOASTAAN SQL-kysely. Älä lisää mitään selityksiä tai kommentteja.

SQL-kysely:
"""

        # Create generation config here, right before using it
        generation_config = GenerationConfig(
            temperature=0.2,
            top_p=0.8,
            max_output_tokens=512
        )

        # Generate content
        response = model.generate_content(prompt, generation_config=generation_config)
        sql_text = response.text.strip()

        # Log the raw response
        logger.debug(f"Raw model response: {sql_text}")

        # Simplified cleanup - just extract the SQL part
        if "```sql" in sql_text:
            # Extract content from SQL code block
            start = sql_text.find("```sql") + 6
            end = sql_text.rfind("```")
            sql_text = sql_text[start:end].strip()
        elif "```" in sql_text:
            # Extract content from generic code block
            start = sql_text.find("```") + 3
            end = sql_text.rfind("```")
            sql_text = sql_text[start:end].strip()

        # Check if it starts with SELECT
        if not sql_text.lower().startswith("select"):
            logger.error(f"Generated SQL does not start with SELECT: {sql_text}")
            return ""

        # Handle table name specifically if needed
        table_name = f"{PROJECT_ID}.valtiodata.budjettidata"
        if table_name in sql_text and f"`{table_name}`" not in sql_text:
            sql_text = sql_text.replace(table_name, f"`{table_name}`")

        logger.info(f"Final SQL query: {sql_text}")
        return sql_text

    except Exception as e:
        logger.error(f"Error generating content with model: {e}")
        return ""
