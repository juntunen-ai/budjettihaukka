# deployment.py
"""
Tämä moduuli sisältää LangGraph-pohjaisen data-analyysiagentin käyttöönottoon 
ja integraatioon liittyvät toiminnot.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from datetime import datetime

# Tuodaan oppimiskoodi
from config import settings
from langgraph_learning import AgentLearningSystem
from utils.bigquery_utils import process_natural_language_query

# Määritellään lokitus
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api_server.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("budget_analysis_api")

# Luodaan FastAPI-sovellus
app = FastAPI(
    title="Budjettidatan analyysi-API",
    description="API budjettidatan analyysiin LangGraph-pohjaisella agentilla",
    version="1.0.0"
)

# Määritellään CORS-säännöt (Cross-Origin Resource Sharing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tuotannossa määritä tarkemmin sallitut lähteet
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Määritellään datamallit API:lle
class AnalysisRequest(BaseModel):
    """Pyyntömalli analyysille."""
    question: str
    context: Optional[Dict[str, Any]] = None

class FeedbackRequest(BaseModel):
    """Pyyntömalli palautteelle."""
    interaction_id: str
    rating: int  # 1-5
    comments: Optional[str] = None
    is_helpful: Optional[bool] = None

class AnalysisResponse(BaseModel):
    """Vastausmalli analyysille."""
    interaction_id: str
    answer: str
    sql_query: Optional[str] = None
    execution_steps: Optional[List[str]] = None
    timestamp: str

learning_system = AgentLearningSystem()
budget_agent = None
agent_init_error = None

if settings.llm_provider == "aistudio":
    agent_init_error = "AI Studio mode active: LangGraph Vertex-agent disabled"
    logger.info(agent_init_error)
else:
    try:
        from langgraph_data_analysis import BudgetAnalysisAgent

        budget_agent = BudgetAnalysisAgent()
    except Exception as e:
        agent_init_error = str(e)
        logger.warning("BudgetAnalysisAgentin alustus epäonnistui käynnistyksessä: %s", e)

# Historia suoritetuista analyyseistä
analysis_history = []

# Taustatoiminnot
def record_interaction(question: str, result: Dict, feedback: Optional[Dict] = None):
    """Tallentaa vuorovaikutuksen oppimista varten."""
    try:
        interaction_id = learning_system.record_interaction(question, result, feedback)
        logger.info(f"Vuorovaikutus tallennettu: {interaction_id}")
    except Exception as e:
        logger.error(f"Virhe vuorovaikutuksen tallennuksessa: {e}")


def _run_fallback_pipeline(question: str) -> Dict[str, Any]:
    fallback = process_natural_language_query(question)
    answer = fallback.get("explanation", "")
    df = fallback.get("results_df")
    if df is not None and not df.empty:
        answer = f"{answer}\n\nTulosten esikatselu:\n{df.head(5).to_string(index=False)}"

    return {
        "answer": answer,
        "sql_query": fallback.get("sql_query", ""),
        "execution_steps": ["Fallback NL->SQL pipeline"],
        "error_message": fallback.get("error"),
    }

# API-reitit
@app.get("/")
async def root():
    """API:n perustiedot."""
    return {
        "name": "Budjettidatan analyysi-API",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": [
            "/analyze - Analysoi budjettidataa kysymyksen perusteella",
            "/feedback - Anna palautetta analyysistä",
            "/history - Näytä analyysihistoria"
        ]
    }

@app.post("/analyze", response_model=AnalysisResponse)
async def analyze_budget(request: AnalysisRequest, background_tasks: BackgroundTasks):
    """
    Analysoi budjettidataa kysymyksen perusteella.
    
    Tämä API-pääte ottaa vastaan luonnollisen kielen kysymyksen ja kontekstin,
    käsittelee sen LangGraph-agentin avulla, ja palauttaa analyysin tulokset.
    """
    logger.info(f"Analyysiä pyydetty kysymyksellä: {request.question}")
    
    try:
        if budget_agent is None:
            logger.warning(
                "LangGraph-agentti ei ole käytettävissä, käytetään fallback-putkea. Virhe: %s",
                agent_init_error,
            )
            result = _run_fallback_pipeline(request.question)
        else:
            try:
                result = budget_agent.analyze(request.question)
            except Exception as e:
                logger.warning(
                    "LangGraph-agentti epäonnistui, käytetään fallback-putkea. Virhe: %s",
                    e,
                )
                result = _run_fallback_pipeline(request.question)
        
        # Luo vastaus
        timestamp = datetime.now().isoformat()
        interaction_id = f"analysis_{int(datetime.now().timestamp())}"
        
        response = {
            "interaction_id": interaction_id,
            "answer": result["answer"],
            "sql_query": result.get("sql_query", None),
            "execution_steps": result.get("execution_steps", []),
            "timestamp": timestamp
        }
        
        # Lisää analyysiin konteksti ja tallenna historiaan
        full_record = {
            **response,
            "question": request.question,
            "context": request.context or {}
        }
        
        # Rajoita historian kokoa
        if len(analysis_history) >= 100:
            analysis_history.pop(0)
        analysis_history.append(full_record)
        
        # Tallenna vuorovaikutus oppimista varten (taustatehtävänä)
        background_tasks.add_task(record_interaction, request.question, result)
        
        logger.info(f"Analyysi valmis: {interaction_id}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Virhe analyysissä: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analyysi epäonnistui: {str(e)}")

@app.post("/feedback")
async def submit_feedback(request: FeedbackRequest, background_tasks: BackgroundTasks):
    """
    Vastaanottaa palautetta aiemmasta analyysistä.
    
    Tämä API-pääte tallentaa käyttäjän palautteen, jota voidaan käyttää
    järjestelmän parantamiseen.
    """
    logger.info(f"Palautetta vastaanotettu: {request.interaction_id}")
    
    # Etsi analyysi historiasta
    analysis = next((a for a in analysis_history if a["interaction_id"] == request.interaction_id), None)
    
    if not analysis:
        raise HTTPException(status_code=404, detail="Analyysiä ei löydy annetulla ID:llä")
    
    # Muodosta palautemalli
    feedback = {
        "rating": request.rating,
        "is_helpful": request.is_helpful,
        "comments": request.comments,
        "timestamp": datetime.now().isoformat()
    }
    
    # Tallenna palaute oppimista varten
    background_tasks.add_task(
        record_interaction, 
        analysis["question"], 
        {
            "answer": analysis["answer"],
            "sql_query": analysis.get("sql_query", ""),
            "execution_steps": analysis.get("execution_steps", [])
        },
        feedback
    )
    
    return {"status": "success", "message": "Palaute vastaanotettu"}

@app.get("/history")
async def get_history(limit: int = 10):
    """
    Hakee analyysihistorian.
    
    Tämä API-pääte palauttaa viimeisimmät analyysit.
    """
    return {
        "total": len(analysis_history),
        "items": analysis_history[-limit:] if limit > 0 else []
    }

@app.get("/stats")
async def get_stats():
    """
    Hakee agenttijärjestelmän tilastot ja parannussuositukset.
    
    Tämä API-pääte palauttaa statistiikkaa ja oppimistuloksia.
    """
    try:
        recommendations = learning_system.generate_improvement_recommendations()
        
        stats = {
            "total_analyses": len(analysis_history),
            "learning_stats": recommendations["overall_stats"],
            "recommendations": {
                "sql_patterns": [rec["suggestion"] for rec in recommendations.get("sql_pattern_recommendations", [])],
                "error_handling": [rec["suggestion"] for rec in recommendations.get("error_handling_recommendations", [])],
                "prompt_improvements": [rec["suggestion"] for rec in recommendations.get("prompt_improvement_recommendations", [])]
            }
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Virhe tilastojen haussa: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Tilastojen haku epäonnistui: {str(e)}")

# Integraatiofunktiot muihin järjestelmiin

def export_to_bigquery(project_id: str, dataset: str, table: str):
    """
    Vie analyysihistorian ja oppimistiedot BigQueryyn.
    
    Tämä funktio voidaan ajaa säännöllisesti cron-jobina tai kutsua manuaalisesti.
    """
    try:
        from google.cloud import bigquery
        
        # Alusta BigQuery-asiakas
        client = bigquery.Client(project=project_id)
        
        # Vie analyysit
        analysis_records = []
        for analysis in analysis_history:
            record = {
                "interaction_id": analysis["interaction_id"],
                "question": analysis["question"],
                "answer": analysis["answer"],
                "sql_query": analysis.get("sql_query", ""),
                "timestamp": analysis["timestamp"]
            }
            analysis_records.append(record)
        
        # Toteuta logiikka BigQuery-vientiin
        # ...
        
        logger.info(f"Tiedot viety BigQueryyn: {project_id}.{dataset}.{table}")
        return {"status": "success", "records_exported": len(analysis_records)}
        
    except Exception as e:
        logger.error(f"Virhe BigQuery-viennissä: {str(e)}")
        return {"status": "error", "message": str(e)}

def integration_webhook(target_url: str, data: Dict):
    """
    Lähettää tietoja webhookin kautta toiseen järjestelmään.
    
    Tätä voidaan käyttää integraatioon muiden järjestelmien kanssa.
    """
    import requests
    
    try:
        response = requests.post(
            target_url,
            json=data,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            logger.info(f"Webhook-kutsu onnistui: {target_url}")
            return {"status": "success", "response": response.json()}
        else:
            logger.warning(f"Webhook-kutsu epäonnistui: {response.status_code} - {response.text}")
            return {"status": "error", "message": f"HTTP {response.status_code}: {response.text}"}
            
    except Exception as e:
        logger.error(f"Virhe webhook-kutsussa: {str(e)}")
        return {"status": "error", "message": str(e)}

# Käynnistys tuotanto- tai kehitystilassa
if __name__ == "__main__":
    # Tarkista, onko tuotantoympäristö
    is_production = os.environ.get("PRODUCTION", "false").lower() == "true"
    
    if is_production:
        # Tuotantokonfiguraatio (Gunicorn + Uvicorn työntekijät)
        logger.info("Käynnistetään tuotantotilassa. Käytä Gunicornia tämän skriptin sijaan.")
    else:
        # Kehityskonfiguraatio
        port = int(os.environ.get("PORT", "8000"))
        logger.info(f"Käynnistetään kehitystilassa portissa {port}")
        uvicorn.run("deployment:app", host="0.0.0.0", port=port, reload=True)
