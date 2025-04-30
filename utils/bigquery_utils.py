# Lisää tämä bigquery_utils.py tiedoston alkuun muiden importien kanssa:
from utils.vertex_ai_utils import generate_sql_from_natural_language, PROJECT_ID
# Tiedostossa: bigquery_utils.py

from google.cloud import bigquery
import pandas as pd
import re
# Tuodaan funktio ja projektitunnus toisesta tiedostosta
from utils.vertex_ai_utils import generate_sql_from_natural_language, PROJECT_ID

# --- Alustus ---
# Käytetään samaa projektitunnusta kuin vertex_ai_utils.py:ssä
# Alustetaan BigQuery Client (tämä voi vaatia autentikoinnin ympäristössäsi)
try:
    bq_client = bigquery.Client(project=PROJECT_ID)
    print("✅ BigQuery Client alustettu.")
except Exception as e:
    print(f"❌ Virhe BigQuery Clientin alustuksessa: {e}")
    bq_client = None # Estetään jatkotoimet, jos alustus epäonnistuu

# Säilötään viimeisin BQ-virhe debuggausta varten (globaali muuttuja ei paras tapa, mutta yksinkertainen)
last_bq_error = None

# --- Funktiot ---
def validate_sql(sql: str) -> str:
    """
    Tarkistaa ja yrittää korjata yleisimpiä SQL-syntaksivirheitä,
    erityisesti liittyen backtickeihin ja taulun nimeen.
    """
    global last_bq_error # Nollataan virhe ennen validointia
    last_bq_error = None

    if not sql: # Jos syöte on tyhjä, palautetaan tyhjä
         return ""

    # Poistetaan mahdolliset codeblock-merkinnät (varmuuden vuoksi)
    sql = re.sub(r'^```sql`?\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s*`?```$', '', sql)
    sql = sql.strip()

    # Tarkistetaan backtickien parillisuus per rivi (yksinkertainen tarkistus)
    # Voitaisiin parantaa myöhemmin
    lines = sql.split('\n')
    validated_lines = []
    original_sql_for_print = "\n".join(lines) # Tallennetaan alkuperäinen muoto tulostukseen

    for line in lines:
        # Lisää tähän tarvittaessa tarkempia rivikohtaisia korjauksia
        validated_lines.append(line)

    result = '\n'.join(validated_lines)

    # Varmistetaan, että taulun nimi on oikeassa muodossa (tärkein korjaus!)
    table_name = f"{PROJECT_ID}.valtiodata.budjettidata"
    correct_table_name = f"`{table_name}`"

    # Korvaa väärät muodot oikealla, käsittelee `projektinimi.data.taulu` ja projektinimi.data.taulu`
    # Poistetaan ensin kaikki backtickit sen ympäriltä ja lisätään sitten oikeat
    pattern = r'`?' + re.escape(table_name) + r'`?'
    result = re.sub(pattern, correct_table_name, result)

    # Debuggausta varten
    # if original_sql_for_print != result:
    #     print(f"🔧 SQL Validoitu:\nOriginal:\n{original_sql_for_print}\nValidated:\n{result}")
    # else:
    #     print("✓ SQL Validointi ei tehnyt muutoksia.")

    return result

def run_sql_query(query: str) -> pd.DataFrame:
    """
    Suorittaa SQL-kyselyn BigQueryssä ja palauttaa tulokset Pandas DataFramena.
    """
    global last_bq_error # Tallennetaan virhe globaaliin muuttujaan
    last_bq_error = None

    if not bq_client:
         print("❌ BigQuery Client ei ole alustettu. Kyselyä ei voi suorittaa.")
         last_bq_error = "BigQuery Client ei ole alustettu."
         return pd.DataFrame()
    if not query:
        print("❌ Tyhjä SQL-kysely annettu.")
        last_bq_error = "Tyhjä SQL-kysely annettu."
        return pd.DataFrame()

    try:
        # print(f"🚀 Suoritetaan BigQuery-kysely...")
        query_job = bq_client.query(query) # API request.
        results_df = query_job.result().to_dataframe() # Waits for query to finish.

        # Tarkistetaan query_jobin virheet erikseen, vaikka dataframe olisi tyhjä
        if query_job.error_result:
             # print(f"❌ Virhe BigQuery-kyselyssä (job.error_result): {query_job.error_result}")
             last_bq_error = f"BigQuery Job Error: {query_job.error_result}"
             return pd.DataFrame() # Palauta tyhjä, jos jobissa oli virhe

        # print(f"✅ Kysely suoritettu, palautettiin {len(results_df)} riviä.")
        return results_df

    except Exception as e:
        # print(f"❌ Virhe BigQuery-kyselyssä (Python Exception): {str(e)}")
        # print(f"❌ SQL, jota yritettiin suorittaa:\n{query}")
        last_bq_error = f"Python Exception: {str(e)}"
        return pd.DataFrame() # Palauta tyhjä DataFrame virhetilanteessa


def process_natural_language_query(question: str) -> dict:
    """
    Käsittelee luonnollisen kielen kysymyksen: generoi SQL, validoi, suorittaa ja palauttaa tulokset.
    """
    print(f"➡️ Käsitellään kysymys: {question}")
    generated_sql = generate_sql_from_natural_language(question)

    if not generated_sql:
        return {
            "sql_query": "",
            "results_df": pd.DataFrame(),
            "error": "SQL-kyselyn generointi Vertex AI:lla epäonnistui.",
            "explanation": "❌ Ei saatu SQL-kyselyä tekoälyltä."
        }

    validated_sql = validate_sql(generated_sql)

    # Validointi itsessään ei pitäisi palauttaa tyhjää, jos syöte ei ollut tyhjä,
    # mutta varmistetaan silti.
    if not validated_sql:
         return {
            "sql_query": generated_sql, # Näytetään alkuperäinen generoitu
            "results_df": pd.DataFrame(),
            "error": "SQL-kyselyn validointi epäonnistui tai tyhjensi kyselyn.",
            "explanation": "❌ Generoitu SQL ei läpäissyt validointia."
        }

    results_df = run_sql_query(validated_sql)

    # Tarkistetaan tulos ja mahdollinen BQ-virhe
    global last_bq_error
    if last_bq_error:
         explanation = f"❌ Virhe suoritettaessa SQL-kyselyä BigQueryssä: {last_bq_error}"
         return {
             "sql_query": validated_sql,
             "results_df": results_df, # Todennäköisesti tyhjä
             "error": last_bq_error,
             "explanation": explanation
         }
    elif results_df.empty:
         explanation = "✅ Kysely suoritettiin onnistuneesti, mutta se ei palauttanut tuloksia."
         return {
            "sql_query": validated_sql,
            "results_df": results_df, # Tyhjä DataFrame
            "error": None, # Ei varsinaista virhettä
            "explanation": explanation
         }
    else:
        # Onnistunut suoritus ja dataa löytyi
         explanation = f"✅ Kysely onnistui ja palautti {len(results_df)} riviä dataa."
         return {
            "sql_query": validated_sql,
            "results_df": results_df,
            "error": None,
            "explanation": explanation
         }
