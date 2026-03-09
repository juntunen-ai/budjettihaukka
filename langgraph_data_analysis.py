# langgraph_data_analysis.py

import os
from functools import lru_cache
from typing import Dict, List, TypedDict, Annotated, Sequence, Any
import json
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pickle
from tempfile import mkdtemp

# LangGraph ja LangChain tuonnit
from langgraph.graph import StateGraph, END, START
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_google_vertexai import VertexAI
from langchain_community.tools.tavily_search import TavilySearchResults
from pydantic import BaseModel, Field

# BigQuery tuonnit
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import vertexai

from config import settings

# Määritellään projektiasetukset
PROJECT_ID = settings.project_id
DATASET = settings.dataset
TABLE = settings.table

vertexai.init(project=PROJECT_ID, location=settings.location)

# Määritellään LLM
llm = VertexAI(
    model_name=settings.gemini_model,
    project=PROJECT_ID,
    location=settings.location,
)

# Määritellään tilan rakenne
class GraphState(TypedDict):
    question: str
    analysis_plan: Dict
    sql_query: str 
    sql_result: Any  # Voi olla DataFrame, dict tai error
    error_message: str
    need_web_search: bool
    web_search_results: List
    pandas_analysis: Dict  # Uusi kenttä pandas-analyysiä varten
    pandas_code: str  # Uusi kenttä pandas-koodia varten
    analysis_result: str
    final_answer: str
    execution_history: List

# -- TYÖKALUT --

def init_bigquery_client():
    """Alustaa BigQuery-asiakkaan."""
    try:
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        print(f"BigQuery-asiakkaan alustus epäonnistui: {e}")
        return None

def get_schema_info():
    """Hakee taulun rakenteen."""
    client = init_bigquery_client()
    if not client:
        return {"error": "BigQuery-asiakas ei saatavilla"}
    
    try:
        table_ref = client.get_table(f"{PROJECT_ID}.{DATASET}.{TABLE}")
        schema_info = [{
            "name": field.name,
            "type": field.field_type,
            "description": field.description or "Ei kuvausta"
        } for field in table_ref.schema]
        return schema_info
    except Exception as e:
        return {"error": f"Taulun rakenteen hakeminen epäonnistui: {e}"}

def execute_sql_query(query):
    """Suorittaa SQL-kyselyn BigQueryssä ja palauttaa tulokset Pandas DataFramena."""
    client = init_bigquery_client()
    if not client:
        return {"error": "BigQuery-asiakas ei saatavilla"}

    try:
        # Suoritetaan kysely ja palautetaan sekä raakatulokset että DataFrame
        query_job = client.query(query)
        results = query_job.result()

        # Muunna tulokset DataFrameksi
        df = results.to_dataframe()

        # Luo perinteiset rivi-tulos-objektit yhteensopivuutta varten
        rows = []
        for _, row in df.iterrows():
            rows.append(row.to_dict())

        # Updated to remove the DataFrame and include only JSON-serializable data
        result_summary = {
            "row_count": len(rows),
            "columns": list(df.columns),
            "sample": rows[:5] if len(rows) > 5 else rows,
            "rows": rows
        }

        return result_summary
    except Exception as e:
        return {"error": f"Kyselyn suorittaminen epäonnistui: {str(e)}"}

@lru_cache(maxsize=1)
def _get_search_tool():
    api_key = settings.tavily_api_key
    if not api_key:
        return None
    os.environ.setdefault("TAVILY_API_KEY", api_key)
    return TavilySearchResults()

def search_web(query):
    """Suorittaa verkkohakun."""
    try:
        search_tool = _get_search_tool()
        if search_tool is None:
            return {"error": "TAVILY_API_KEY puuttuu ympäristömuuttujista."}
        search_results = search_tool.invoke(query)
        return search_results
    except Exception as e:
        return {"error": f"Verkkohaku epäonnistui: {str(e)}"}

# Oma ToolExecutor-toteutus LangGraph-kirjaston sijaan
class SimpleToolExecutor:
    """Yksinkertainen työkalujen suorittaja joka emuloi LangGraph:n ToolExecutor-luokkaa."""
    
    def __init__(self, tools):
        """Alustaa työkalusuorittajan annetuilla työkaluilla."""
        self.tools = {tool["name"]: tool["func"] for tool in tools}
    
    def invoke(self, tool_input):
        """Kutsuu työkalua annetulla syötteellä."""
        tool_name = tool_input.get("name")
        tool_input_value = tool_input.get("input", None)
        
        if tool_name not in self.tools:
            return {"error": f"Työkalua {tool_name} ei löydy"}
        
        try:
            if tool_input_value is not None:
                return self.tools[tool_name](tool_input_value)
            else:
                return self.tools[tool_name]()
        except Exception as e:
            return {"error": f"Virhe työkalun {tool_name} suorituksessa: {str(e)}"}

# Luodaan työkalujen suorittaja
tools = [
    {"name": "get_schema_info", "func": get_schema_info},
    {"name": "execute_sql_query", "func": execute_sql_query},
    {"name": "search_web", "func": search_web}
]
tool_executor = SimpleToolExecutor(tools)

# -- JÄSENTÄJÄT --

class AnalysisPlan(BaseModel):
    """Malli analyyssuunnitelmalle."""
    required_data: List[str] = Field(description="Mitä tietoja tarvitaan kysymykseen vastaamiseksi")
    columns_needed: List[str] = Field(description="Mitkä taulun sarakkeet ovat oleellisia")
    filters_needed: List[str] = Field(description="Mitä suodattimia tarvitaan (esim. vuosi, ministeriö)")
    aggregations_needed: List[str] = Field(description="Mitä aggregointeja tarvitaan (esim. summa, keskiarvo)")
    order_by: List[str] = Field(description="Miten tulokset tulisi järjestää")
    potential_challenges: List[str] = Field(description="Mahdolliset haasteet kyselyn muodostamisessa")

class NeedWebSearch(BaseModel):
    need_search: bool = Field(description="Tarvitaanko verkkohakua täydentävien tietojen saamiseksi")
    search_query: str = Field(description="Hakukysely verkkohakua varten, jos tarpeen")
    rationale: str = Field(description="Perustelu päätökselle")

# -- SOLMUFUNKTIOT --

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain_core.output_parsers import StrOutputParser

# Define the JSON structure you expect
json_parser = JsonOutputParser()

# Update your prompt to explicitly request JSON
prompt = PromptTemplate.from_template(
    """Analyze the following question about budget data:
    {question}

    First, determine what columns in the table structure correspond to the requested information.
    Then, create an appropriate SQL query.

    Return your response in the following JSON format:
    {{"analysis": "your analysis of what data is needed",
     "sql_query": "the SQL query to execute"}}
    """
)

# Chain your components together
analyze_chain = prompt | llm | json_parser

# Define your prompt template
prompt = ChatPromptTemplate.from_messages([
    ("system", "Olet data-analyysiavustaja, joka käsittelee budjettidataa suomeksi."),
    ("human", "{question}")
])

# Create chain correctly
chain = prompt | llm | StrOutputParser()

# Update analyze_question to use the chain with output parser
def analyze_question(state: GraphState) -> GraphState:
    question = state["question"]
    try:
        # Use the modern pattern
        response = chain.invoke({"question": question})

        # response is now directly a string, not an object with content

        # Continue with your code using the string response
        return {
            **state,
            "analysis_plan": response,
            "execution_history": state.get("execution_history", []) + ["Question analyzed and analysis plan created"]
        }

    except Exception as e:
        return {
            **state,
            "error_message": f"Virhe analyysissä: {str(e)}",
            "execution_history": state.get("execution_history", []) + ["Error during question analysis"]
        }

def generate_sql_query(state: GraphState) -> GraphState:
    """Luo SQL-kyselyn analyysisuunnitelman perusteella."""
    question = state["question"]
    analysis_plan = state["analysis_plan"]

    # Haetaan skeema
    schema_info = tool_executor.invoke({"name": "get_schema_info"})

    # Luodaan prompt SQL-generointiin
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Olet SQL-asiantuntija, joka erikoistuu BigQuery-kyselyihin.
Luo täsmällinen SQL-kysely käyttäjän kysymykseen.
Käytä taulua `{project_id}.{dataset}.{table}`.

Taulun rakenne:
{schema_info}

Analyysisuunnitelma:
{analysis_plan}

Ohjeet:
1. Älä käytä * (tähti), vaan nimeä tarvittavat sarakkeet
2. Käytä vain olemassa olevia sarakkeita
3. Muista käyttää oikeita taulujen nimiä muodossa `projekti.dataset.taulu`
4. Käytä selkeitä alias-nimiä tarvittaessa
5. Jos vastaus vaatii tiettyä formaattia, huomioi se kyselyssä
6. Käytä backtickejä (``) taulujen ja sarakkeiden nimissä
"""),
        ("user", "Kysymys: {question}\n\nLuo SQL-kysely, joka vastaa tähän kysymykseen.")
    ])

    # Use the chain approach with StrOutputParser
    chain = prompt | llm | StrOutputParser()

    # Suoritetaan SQL:n generointi
    sql_query = chain.invoke({
        "project_id": PROJECT_ID,
        "dataset": DATASET,
        "table": TABLE,
        "schema_info": json.dumps(schema_info, indent=2),
        "analysis_plan": json.dumps(analysis_plan, indent=2),
        "question": question
    })

    # Puhdistetaan SQL (poistetaan mahdolliset markdown-syntaksit)
    if "```sql" in sql_query:
        sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
    elif "```" in sql_query:
        sql_query = sql_query.split("```")[1].split("```")[0].strip()

    # Päivitetään tila
    return {
        **state,
        "sql_query": sql_query,
        "execution_history": state.get("execution_history", []) + ["SQL-kysely generoitu"]
    }

def execute_query(state: GraphState) -> GraphState:
    """Suorittaa SQL-kyselyn."""
    sql_query = state["sql_query"]
    
    # Suoritetaan kysely
    result = tool_executor.invoke({"name": "execute_sql_query", "input": sql_query})
    
    # Tarkistetaan virheet
    if result and "error" in result:
        return {
            **state,
            "sql_result": None,
            "error_message": result["error"],
            "execution_history": state.get("execution_history", []) + [f"SQL-kyselyn suoritus epäonnistui: {result['error']}"]
        }
    
    # Päivitetään tila onnistuneella tuloksella
    return {
        **state,
        "sql_result": result,
        "error_message": "",
        "execution_history": state.get("execution_history", []) + ["SQL-kysely suoritettu onnistuneesti"]
    }

def handle_error(state: GraphState) -> Dict:
    """Käsittelee SQL-virheet ja yrittää korjata kyselyä."""
    error_message = state["error_message"]
    original_query = state["sql_query"]
    
    # Luodaan prompt virheenkorjaukseen
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Olet SQL-virheiden korjaaja, joka erikoistuu BigQuery-kyselyihin.
Korjaa SQL-kysely perustuen virheviestiin.
"""),
        ("user", """Virheellinen SQL-kysely:
{query}

Virheviesti:
{error}

Korjaa tämä SQL-kysely. Palauta vain korjattu SQL ilman selityksiä.
""")
    ])
    
    # Suoritetaan korjaus
    corrected_sql = prompt.invoke({
        "query": original_query,
        "error": error_message
    })

    # Puhdistetaan SQL (poistetaan mahdolliset markdown-syntaksit)
    if "```sql" in corrected_sql:
        corrected_sql = corrected_sql.split("```sql")[1].split("```")[0].strip()
    elif "```" in corrected_sql:
        corrected_sql = corrected_sql.split("```")[1].split("```")[0].strip()
    
    # Jos virhe on liian vakava korjattavaksi
    if "ei voida korjata" in corrected_sql.lower() or corrected_sql == original_query:
        # Siirry suoraan loppuvastaukseen
        return {
            "next": "format_final_answer",
            "state": {
                **state,
                "execution_history": state.get("execution_history", []) + ["Kriittinen SQL-virhe, jota ei voitu korjata"]
            }
        }
    
    # Päivitetään tila korjatulla kyselyllä ja kokeillaan uudelleen
    return {
        "next": "generate_sql_query",
        "state": {
            **state,
            "sql_query": corrected_sql,
            "execution_history": state.get("execution_history", []) + ["SQL-kysely korjattu"]
        }
    }

def check_web_search_need(state: GraphState) -> Dict:
    """Tarkistaa, tarvitaanko verkkohakua täydentämään tietoja."""
    question = state["question"]
    sql_result = state["sql_result"]

    # Create a more explicit prompt that asks for JSON
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Olet tiedon analysointiin erikoistunut assistentti.
Päätä, tarvitaanko lisätietoja verkosta alkuperäiseen kysymykseen vastaamiseksi.

TÄRKEÄÄ: Vastaa AINOASTAAN seuraavassa JSON-muodossa:
{
  \"need_search\": true/false,
  \"search_query\": \"hakukysely jos tarvitaan hakua\",
  \"rationale\": \"perustelu päätökselle\"
}
"""),
        ("user", """Alkuperäinen kysymys:
{question}

SQL-kyselyn tulokset:
{sql_result}

Analysoi SQL-tulokset ja päätä, puuttuuko oleellisia tietoja, jotka voitaisiin löytää verkosta.
Vastaa VAIN JSON-muodossa, kuten ohjeistettu.
""")
    ])

    # Use a more robust approach with json parser
    from langchain_core.output_parsers import JsonOutputParser

    # Create a chain with JSON output parser
    chain = prompt | llm | JsonOutputParser()

    try:
        # Get JSON result
        json_result = chain.invoke({
            "question": question,
            "sql_result": json.dumps(sql_result, indent=2, default=str)  # Added default=str to handle non-serializable objects
        })

        # Now manually create a NeedWebSearch object from the parsed JSON
        search_decision = NeedWebSearch(
            need_search=json_result.get("need_search", False),
            search_query=json_result.get("search_query", ""),
            rationale=json_result.get("rationale", "")
        )

        # Päätöksen perusteella
        if search_decision.need_search:
            # Tehdään verkkohaku
            return {
                "next": "search_web",
                "state": {
                    **state,
                    "need_web_search": True,
                    "web_search_results": [],
                    "execution_history": state.get("execution_history", []) + [
                        f"Verkkohaku tarpeen: {search_decision.rationale}",
                        f"Hakukysely: {search_decision.search_query}"
                    ]
                }
            }
        else:
            # Jatketaan suoraan analyysiin
            return {
                "next": "analyze_data",
                "state": {
                    **state,
                    "need_web_search": False,
                    "execution_history": state.get("execution_history", []) + ["Verkkohaku ei tarpeen"]
                }
            }
    except Exception as e:
        # If parsing fails, go directly to analysis without web search
        return {
            "next": "analyze_data",
            "state": {
                **state,
                "need_web_search": False,
                "error_message": f"Virhe verkkohakutarpeen analysoinnissa: {str(e)}",
                "execution_history": state.get("execution_history", []) + [
                    f"Verkkohakutarpeen analysointi epäonnistui: {str(e)}",
                    "Jatketaan ilman verkkohakua"
                ]
            }
        }

def search_web_info(state: GraphState) -> GraphState:
    """Suorittaa verkkohakuja lisätietojen saamiseksi."""
    question = state["question"]
    
    # Luodaan haku perustuen alkuperäiseen kysymykseen ja analyysiin
    search_query = f"Valtion budjetti {question}"
    
    # Suoritetaan haku
    search_results = tool_executor.invoke({"name": "search_web", "input": search_query})
    
    # Päivitetään tila hakutuloksilla
    return {
        **state,
        "web_search_results": search_results,
        "execution_history": state.get("execution_history", []) + [f"Verkkohaku suoritettu kyselyllä: {search_query}"]
    }

def analyze_data(state: GraphState) -> GraphState:
    """Analysoi kerätyt tiedot ja luo analyysin."""
    question = state["question"]
    sql_result = state["sql_result"]
    web_results = state.get("web_search_results", [])
    
    # Luodaan prompt analyysia varten
    prompt = ChatPromptTemplate.from_messages([
        ("system", """Olet data-analyytikko, joka erikoistuu taloustietojen analysointiin.
Luo perusteellinen analyysi annetuista tiedoista.
"""),
        ("user", """Alkuperäinen kysymys:
{question}

SQL-kyselyn tulokset:
{sql_result}

{web_info}

Analysoi tulokset ja luo perusteellinen analyysi, joka vastaa alkuperäiseen kysymykseen.
Sisällytä numerotiedot, kehityssuunnat ja oleelliset havainnot.
""")
    ])
    
    # Muodostetaan web-info osuus
    web_info = ""
    if web_results:
        web_info = "Verkkohakutulokset:\n" + json.dumps(web_results, indent=2)
    else:
        web_info = "Ei verkkohakutuloksia."
    
    # Suoritetaan analyysi
    analysis = prompt.invoke({
        "question": question,
        "sql_result": json.dumps(sql_result, indent=2),
        "web_info": web_info
    })

    # Päivitetään tila analyysin tuloksilla
    return {
        **state,
        "analysis_result": analysis,
        "execution_history": state.get("execution_history", []) + ["Data analysoitu"]
    }

def analyze_with_pandas(state: GraphState) -> GraphState:
    """Analysoi kyselyn tuloksia pandas-operaatioilla."""
    sql_result = state["sql_result"]
    question = state["question"]
    
    # Jos SQL-kysely epäonnistui tai tulos on tyhjä
    if not sql_result or "error" in sql_result:
        return {
            **state,
            "execution_history": state.get("execution_history", []) + ["Pandas-analyysi ohitettiin"]
        }
    
    # Yritä ladata DataFrame
    try:
        # Luo DataFrame SQL-tuloksista
        if "rows" in sql_result and sql_result["rows"]:
            df = pd.DataFrame(sql_result["rows"])
        else:
            return {
                **state,
                "execution_history": state.get("execution_history", []) + ["Pandas-analyysi ohitettiin: ei rivejä"]
            }
        
        # INSTEAD OF GENERATING CODE, do a fixed analysis based on the question
        analysis_result = {}
        
        # Simple built-in analysis based on common question types
        if "pääluok" in question.lower():
            # For questions about "pääluokat" (main categories)
            if "PaaluokkaOsasto_sNimi" in df.columns:
                unique_categories = df["PaaluokkaOsasto_sNimi"].unique().tolist()
                analysis_result = {
                    "pääluokat": unique_categories,
                    "lukumäärä": len(unique_categories)
                }
            elif "Paaluokka" in df.columns:
                unique_categories = df["Paaluokka"].unique().tolist()
                analysis_result = {
                    "pääluokat": unique_categories,
                    "lukumäärä": len(unique_categories)
                }
            else:
                # Try to find any column that might contain category information
                for col in df.columns:
                    if "luok" in col.lower() or "osasto" in col.lower() or "nimi" in col.lower():
                        unique_values = df[col].unique().tolist()
                        if len(unique_values) < 50:  # Reasonable number for categories
                            analysis_result = {
                                "mahdolliset_pääluokat": unique_values,
                                "lukumäärä": len(unique_values),
                                "sarake": col
                            }
                            break
        
        # If no specific analysis was done, do a generic summary
        if not analysis_result:
            analysis_result = {
                "rivien_määrä": len(df),
                "sarakkeet": df.columns.tolist(),
                "esimerkkiarvot": df.head(3).to_dict(orient="records")
            }
        
        return {
            **state,
            "pandas_analysis": analysis_result,
            "execution_history": state.get("execution_history", []) + ["Pandas-analyysi suoritettu"]
        }
    except Exception as e:
        return {
            **state,
            "error_message": f"Pandas-analyysi epäonnistui: {str(e)}",
            "execution_history": state.get("execution_history", []) + [f"Pandas-analyysi epäonnistui: {str(e)}"]
        }

def format_final_answer(state: GraphState) -> GraphState:
    """Muotoilee lopullisen vastauksen käyttäjälle."""
    question = state["question"]
    analysis = state.get("analysis_result", "")
    pandas_analysis = state.get("pandas_analysis", {})
    error_message = state.get("error_message", "")
    execution_history = state.get("execution_history", [])
    
    # Käytä ensin pandas-analyysiä jos saatavilla
    if pandas_analysis:
        prompt = ChatPromptTemplate.from_messages([
            ("system", """Olet avulias data-assistentti.
Muotoile Pandas-analyysin tulokset selkeäksi ja ytimekkääksi vastaukseksi käyttäjälle.
"""),
            ("user", """
Alkuperäinen kysymys:
{question}

Pandas-analyysin tulokset:
{pandas_results}

Perinteinen analyysi:
{analysis}

Muotoile nämä tiedot selkeäksi vastaukseksi käyttäjälle, priorisoiden Pandas-analyysin tuloksia.
""")
        ])
        
        final_answer = llm.invoke(prompt.format(
            question=question,
            pandas_results=json.dumps(pandas_analysis, indent=2),
            analysis=analysis
        )).content
    elif not analysis and error_message:
        # Jos analyysia ei ole (virhetilanne), sama kuin ennen
        prompt = ChatPromptTemplate.from_messages([
            ("system", """Olet avulias data-assistentti.
Selvitä käyttäjälle, miksi heidän kysymykseensä ei voitu vastata.
"""),
            ("user", """
Alkuperäinen kysymys:
{question}

Virhe:
{error}

Suoritushistoria:
{history}

Selitä ystävällisesti, miksi kysymykseen ei voitu vastata, ja ehdota mahdollisia vaihtoehtoja.
""")
        ])
        
        final_answer = llm.invoke(prompt.format(
            question=question,
            error=error_message,
            history="\n".join(execution_history)
        )).content
    else:
        # Perinteinen vastaus analyysin perusteella
        prompt = ChatPromptTemplate.from_messages([
            ("system", """Olet avulias data-assistentti.
Muotoile analyysi selkeäksi ja ytimekkääksi vastaukseksi käyttäjälle.
Vastaa suoraan kysymykseen, ja sisällytä tärkeimmät löydökset ja johtopäätökset.
"""),
            ("user", """
Alkuperäinen kysymys:
{question}

Analyysi:
{analysis}

Muotoile tämä analyysi selkeäksi vastaukseksi käyttäjälle.
""")
        ])
        
        # Update the final_answer assignment to handle the result as a string directly
        final_answer = llm.invoke(prompt.format(
            question=question,
            analysis=analysis
        ))

    # Päivitetään tila lopullisella vastauksella
    return {
        **state,
        "final_answer": final_answer,
        "execution_history": execution_history + ["Lopullinen vastaus muotoiltu"]
    }

def execute_sql(state):
    if "error" in state:
        # Handle previous error gracefully
        sample_data = {"columns": ["col1", "col2"], "rows": [["sample1", "sample2"]]}  # Example sample data
        return {
            "results": "Analysis encountered an error. Showing sample data instead.",
            "data": sample_data
        }

    try:
        # Execute the SQL query
        query = state["sql_query"]
        result = execute_sql_query(query)  # Reuse existing SQL execution logic
        return {
            "results": result,
            "data": result.get("rows", [])  # Extract rows if available
        }
    except Exception as e:
        return {
            "error": f"SQL execution failed: {str(e)}",
            "results": None
        }

# -- TILAKAAVION MÄÄRITTELY --

def build_graph():
    """Rakentaa agentin tilakaavion."""
    # Määritellään kaavio
    workflow = StateGraph(GraphState)
    
    # Lisätään solmut (mukaan lukien uusi Pandas-analyysi)
    workflow.add_node("analyze_question", analyze_question)
    workflow.add_node("generate_sql_query", generate_sql_query)
    workflow.add_node("execute_query", execute_query)
    workflow.add_node("handle_error", handle_error)
    workflow.add_node("check_web_search_need", check_web_search_need)
    workflow.add_node("search_web", search_web_info)
    workflow.add_node("analyze_data", analyze_data)
    workflow.add_node("format_final_answer", format_final_answer)
    
    # Määritellään siirtymät
    workflow.add_edge("analyze_question", "generate_sql_query")
    workflow.add_edge("generate_sql_query", "execute_query")
    
    # Ehdollinen siirtymä: Kyselyn suoritus → Virheenkäsittely tai Verkkohakutarpeen tarkistus
    workflow.add_conditional_edges(
        "execute_query",
        lambda x: "handle_error" if x.get("error_message") else "check_web_search_need",
    )
    
    # Virheenkäsittelijän ehdolliset lopputulemat
    workflow.add_conditional_edges(
        "handle_error",
        lambda _: _["next"],
        {
            "generate_sql_query": "generate_sql_query",
            "format_final_answer": "format_final_answer"
        }
    )
    
    # MODIFY THIS: Verkkohakutarkistuksen ehdolliset lopputulemat - go directly to analyze_data
    workflow.add_conditional_edges(
        "check_web_search_need",
        lambda _: _["next"],
        {
            "search_web": "search_web",
            "analyze_data": "analyze_data"  # Changed from analyze_with_pandas to analyze_data
        }
    )
    
    # MODIFY THIS: Verkkohaku → suoraan analyysiin
    workflow.add_edge("search_web", "analyze_data")  # Changed from analyze_with_pandas
    
    workflow.add_edge("analyze_data", "format_final_answer")
    workflow.add_edge("format_final_answer", END)
    
    # Add an edge from START to the first node
    workflow.add_edge(START, "analyze_question")
    
    # Käännä ja palauta graafi
    return workflow.compile()

# -- AGENTTI JA KÄYTTÖLIITTYMÄ --

class BudgetAnalysisAgent:
    """Luokka, joka kapseloi budjettianalyysiagentin toiminnallisuuden."""
    
    def __init__(self):
        """Alustaa agentin."""
        self.graph = build_graph()
    
    def analyze(self, question: str) -> Dict:
        """Käsittelee kysymyksen ja palauttaa analyysin."""
        # Alkutila
        initial_state = {
            "question": question,
            "analysis_plan": {},
            "sql_query": "",
            "sql_result": None,
            "error_message": "",
            "need_web_search": False,
            "web_search_results": [],
            "analysis_result": "",
            "final_answer": "",
            "execution_history": []
        }
        
        # Suorita graafi
        result = self.graph.invoke(initial_state)
        
        return {
            "answer": result["final_answer"],
            "sql_query": result["sql_query"],
            "analysis": result["analysis_result"],
            "execution_steps": result["execution_history"]
        }

# -- KÄYTTÖESIMERKKI --

def main():
    """Esimerkki agentin käytöstä."""
    agent = BudgetAnalysisAgent()
    
    # Esimerkkikysymyksiä
    questions = [
        "Paljonko opetusministeriöllä oli määrärahoja vuonna 2022?",
        "Mitkä ministeriöt saivat eniten rahoitusta vuosina 2020-2023?",
        "Miten sosiaali- ja terveysministeriön budjetti on kehittynyt viimeisen 5 vuoden aikana?"
    ]
    
    # Käsitellään kysymykset
    for question in questions:
        print(f"\nKYSYMYS: {question}")
        print("=" * 80)
        
        try:
            result = agent.analyze(question)
            
            print("VASTAUS:")
            print(result["answer"])
            print("\nKÄYTETTY SQL-KYSELY:")
            print(result["sql_query"])
            print("\nSUORITUSVAIHEET:")
            for step in result["execution_steps"]:
                print(f"- {step}")
                
        except Exception as e:
            print(f"Virhe kysymyksen käsittelyssä: {e}")
        
        print("=" * 80)

if __name__ == "__main__":
    main()
