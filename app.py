Näyttää hyvältä alulta app.py -tiedostoon, mutta tämä on vasta lokituksen asetukset. Sinun kannattaisi täydentää tiedosto kattavammaksi käyttöliittymätoteutukseksi. Tässä on ehdotus täydellisestä Streamlit-pohjaisesta app.py -tiedostosta, joka integroituu luomaasi LangGraph-järjestelmään:

```python
# app.py
import streamlit as st
import logging
import time
import pandas as pd
import json
from datetime import datetime
from langgraph_data_analysis import BudgetAnalysisAgent
from langgraph_learning import AgentLearningSystem

# Configure logging to write to a file
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("application.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info("Application started")

# Sivu otsikko
st.set_page_config(page_title="Budjettidatan Analyysi-agentti", layout="wide")
st.title("Budjettidatan Analyysi-agentti")

# Alustetaan agentin ja oppimismoduulin tilat
@st.cache_resource
def load_agent():
    """Lataa agentti muistiin (Streamlitin välimuistiin)"""
    logger.info("Initializing Budget Analysis Agent")
    return BudgetAnalysisAgent()

@st.cache_resource
def load_learning_system():
    """Lataa oppimismoduuli muistiin"""
    logger.info("Initializing Learning System")
    return AgentLearningSystem()

# Lataa resurssit
agent = load_agent()
learning_system = load_learning_system()

# Sivupalkkiasetukset
with st.sidebar:
    st.header("Tietoa järjestelmästä")
    st.info("""
    Tämä sovellus käyttää LangGraph-pohjaista agenttia budjettidatan analysointiin.
    
    Agentti:
    1. Analysoi kysymyksesi
    2. Muodostaa SQL-kyselyn
    3. Hakee tiedot BigQuerystä
    4. Täydentää tietoja verkosta tarvittaessa
    5. Analysoi tulokset
    6. Muotoilee vastauksen
    
    Voit seurata agentin toimintaa reaaliajassa.
    """)
    
    st.header("Esimerkkikysymyksiä")
    example_questions = [
        "Paljonko opetusministeriöllä oli määrärahoja vuonna 2022?",
        "Mitkä ministeriöt saivat eniten rahoitusta vuosina 2020-2023?",
        "Miten sosiaali- ja terveysministeriön budjetti on kehittynyt viimeisen 5 vuoden aikana?",
        "Vertaile puolustusministeriön ja valtiovarainministeriön budjettikehitystä",
        "Mitkä momentit saivat suurimmat korotukset vuodesta 2021 vuoteen 2022?"
    ]
    
    for i, q in enumerate(example_questions):
        if st.button(f"Esimerkki {i+1}", key=f"example_{i}"):
            st.session_state.user_question = q

    # Tilastot ja parannussuositukset
    st.header("Järjestelmän oppimistilastot")
    if st.button("Näytä tilastot"):
        with st.spinner("Haetaan tilastoja..."):
            try:
                stats = learning_system.generate_improvement_recommendations()
                st.metric("Kysymyksiä yhteensä", 
                          stats["overall_stats"]["total_questions"])
                st.metric("Onnistumisprosentti", 
                          f"{stats['overall_stats']['success_rate']*100:.1f}%")
                
                if stats.get("prompt_improvement_recommendations"):
                    st.subheader("Parannussuositukset")
                    for rec in stats["prompt_improvement_recommendations"]:
                        st.info(f"{rec['suggestion']}\n\n{rec['rationale']}")
            except Exception as e:
                st.error(f"Virhe tilastojen haussa: {e}")

# Alustetaan istuntomuuttujat, jos niitä ei vielä ole
if "user_question" not in st.session_state:
    st.session_state.user_question = ""
if "analysis_result" not in st.session_state:
    st.session_state.analysis_result = None
if "execution_history" not in st.session_state:
    st.session_state.execution_history = []
if "is_processing" not in st.session_state:
    st.session_state.is_processing = False
if "interaction_id" not in st.session_state:
    st.session_state.interaction_id = None

# Kyselylomake
user_question = st.text_area("Kysy budjettidatasta:", value=st.session_state.user_question, height=100)

# Kyselypainike
col1, col2 = st.columns([1, 5])
with col1:
    analyze_button = st.button("Analysoi", type="primary", use_container_width=True)

# Kun painiketta painetaan
if analyze_button and user_question:
    st.session_state.user_question = user_question
    st.session_state.is_processing = True
    st.session_state.execution_history = []
    st.session_state.analysis_result = None
    
    # Analysoinnin suoritus
    with st.spinner("Agentti analysoi kysymystäsi..."):
        try:
            # Tässä käyttöliittymässä emuloimme agentin vaiheittaista suoritusta
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Suorita analyysi
            logger.info(f"Starting analysis for question: {user_question}")
            result = agent.analyze(user_question)
            st.session_state.analysis_result = result
            st.session_state.execution_history = result["execution_steps"]
            
            # Tallenna vuorovaikutus oppimista varten
            interaction_id = learning_system.record_interaction(user_question, result)
            st.session_state.interaction_id = interaction_id
            logger.info(f"Analysis completed with interaction_id: {interaction_id}")
            
            # Päivitä progressibar valmiiksi
            progress_bar.progress(100)
            status_text.text("Analyysi valmis!")
            
        except Exception as e:
            logger.error(f"Analysis error: {str(e)}")
            st.error(f"Virhe analyysissä: {str(e)}")
        finally:
            st.session_state.is_processing = False

# Näytetään tulokset, jos saatavilla
if st.session_state.analysis_result:
    result = st.session_state.analysis_result
    
    # Välilehtikäyttöliittymä tulosten näyttämiseen
    tab1, tab2, tab3 = st.tabs(["Vastaus", "SQL & Data", "Suoritusaskeleet"])
    
    with tab1:
        st.markdown("## Analyysin tulos")
        st.markdown(result["answer"])
        
        # Palautepainikkeet
        col1, col2, col3 = st.columns([1, 1, 5])
        with col1:
            if st.button("👍 Hyödyllinen", key="thumbs_up"):
                if st.session_state.interaction_id:
                    learning_system.record_interaction(
                        st.session_state.user_question,
                        result,
                        {"thumbs_up": True, "timestamp": datetime.now().isoformat()}
                    )
                    st.success("Kiitos palautteestasi!")
                    
        with col2:
            if st.button("👎 Ei hyödyllinen", key="thumbs_down"):
                if st.session_state.interaction_id:
                    learning_system.record_interaction(
                        st.session_state.user_question,
                        result,
                        {"thumbs_up": False, "timestamp": datetime.now().isoformat()}
                    )
                    st.success("Kiitos palautteestasi!")
    
    with tab2:
        st.markdown("## SQL-kysely")
        st.code(result["sql_query"], language="sql")
        
        # Jos tuloksissa on dataa ja se voidaan muuttaa taulukoksi
        if "sql_result" in result and result.get("sql_result") and "rows" in result.get("sql_result", {}):
            st.markdown("## Data")
            try:
                df = pd.DataFrame(result["sql_result"]["rows"])
                st.dataframe(df)
                
                # Lataa-painike
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Lataa CSV",
                    data=csv,
                    file_name=f"budget_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            except Exception as e:
                st.json(result.get("sql_result", {}))
                st.warning(f"Taulukon muodostaminen epäonnistui: {e}")
    
    with tab3:
        st.markdown("## Suoritusaskeleet")
        for i, step in enumerate(result["execution_steps"]):
            st.markdown(f"{i+1}. {step}")
        
        # Analyysiaika
        if "analysis_time" in result:
            st.info(f"Analyysi kesti {result['analysis_time']:.2f} sekuntia")

# Näytetään suorituksen vaiheet, jos prosessointi on käynnissä
elif st.session_state.is_processing:
    st.markdown("## Suorituksen vaiheet")
    for i, step in enumerate(st.session_state.execution_history):
        st.markdown(f"{i+1}. {step}")

# Samankaltaiset kysymykset, jos kysymys syötetty
if user_question and not st.session_state.is_processing:
    similar_questions = learning_system.get_similar_questions(user_question, limit=3)
    
    if similar_questions and len(similar_questions) > 0:
        st.markdown("## Samankaltaisia kysymyksiä")
        st.markdown("Järjestelmä on vastannut aiemmin näihin samantapaisiin kysymyksiin:")
        
        for q in similar_questions:
            with st.expander(q["question"]):
                st.markdown(q.get("final_answer", ""))
                st.caption(f"Kysytty: {q.get('timestamp', '')}")

# Alatunnisteosio
st.markdown("---")
st.markdown("Budjettidatan analysointijärjestelmä | Powered by LangGraph & Vertex AI")

if __name__ == "__main__":
    import os
    import subprocess
    import sys

    # Käynnistä Streamlit-sovellus tämän skriptin kautta
    streamlit_file = os.path.join(os.path.dirname(__file__), "streamlit_app.py")
    subprocess.run([sys.executable, "-m", "streamlit", "run", streamlit_file])
```