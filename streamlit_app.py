import streamlit as st
from utils.vertex_ai_utils import generate_sql_from_natural_language
from google.cloud import bigquery
import re
import pandas as pd

def execute_sql_query(sql_query: str):
    """
    Executes the given SQL query using BigQuery and returns the results as a Pandas DataFrame.

    Parameters:
        sql_query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: The query results as a DataFrame.
    """
    try:
        client = bigquery.Client()
        # Lisää diagnostiikkaa
        st.write(f"Luotu BigQuery-client: {client}")
        
        query_job = client.query(sql_query)
        st.write(f"Query job käynnistetty: {query_job.job_id}")
        
        # Lisää aikakatkaisu, jotta pitkäkestoiset kyselyt eivät aiheuttaisi ongelmia
        results = query_job.result(timeout=60)
        st.write(f"Query tulokset saatu. Rivejä: {results.total_rows}")
        
        # Varmista, että palautetaan tyhjä DataFrame, jos tuloksia ei ole
        df = results.to_dataframe()
        if df.empty:
            st.warning("Kysely palautti tyhjän tuloksen.")
        return df
    except Exception as e:
        st.error(f"Failed to execute SQL query: {str(e)}")
        # Tulosta täysi virheviesti kehittäjille
        st.exception(e)
        return pd.DataFrame()  # Palauta tyhjä DataFrame virheen sijaan

def sanitize_sql_query(sql_query: str) -> str:
    """
    Sanitoi SQL-kyselyn turvalliseksi ja varmistaa, että taulukoiden nimet on oikein muotoiltu.
    
    Parameters:
        sql_query (str): Alkuperäinen SQL-kysely
        
    Returns:
        str: Sanitoitu SQL-kysely
    """
    # Käsitellään taulukon nimi tarkemmin ja paremmalla tavalla
    table_name = "valtion-budjetti-data.valtiodata.budjettidata"
    backticked_table = f"`{table_name}`"
    
    # Jos taulukon nimi on kysyssä ja sitä ei ole vielä backtick-merkeissä
    if table_name in sql_query and backticked_table not in sql_query:
        # Regex korvaa vain taulukon nimen, joka ei ole jo backtick-merkeissä
        pattern = r'(?<!`)(valtion-budjetti-data\.valtiodata\.budjettidata)(?!`)'
        sql_query = re.sub(pattern, backticked_table, sql_query)
    
    # Varmistetaan, että kysely on SELECT-tyyppinen
    if not re.match(r'^\s*SELECT', sql_query, re.IGNORECASE):
        raise ValueError("Vain SELECT-kyselyt ovat sallittuja tietoturvan vuoksi.")
    
    return sql_query

def main():
    st.set_page_config(page_title="Budjettihaukka", layout="wide")
    st.title("Budjettihaukka")
    st.write("Budjettihaukka on avoimen lähdekoodin web-sovellus, jonka tarkoituksena on tuoda talouspolitiikkaan liittyvä tieto helposti saataville, analysoitavaksi ja visualisoitavaksi. Sovelluksen käyttäjä voi esittää kysymyksiä luonnollisella kielellä, ja tekoälyn avulla saa kansantaloudelliseen optimaalisuuteen ja empiiriseen taloustutkimukseen perustuvia analyyseja. Tulokset voidaan näyttää taulukkoina, dynaamisina visualisointeina sekä analyyttisinä raportteina.")

    # Lisätään diagnostiikkatila kehittäjille
    debug_mode = st.sidebar.checkbox("Kehittäjätila", value=False)

    # Input for natural language question
    question = st.text_area("Kirjoita kysymyksesi:", placeholder="Esim. Mitkä olivat puolustusministeriön menot vuonna 2023?", height=100)

    if st.button("Hae tulokset"):
        if not question.strip():
            st.warning("Ole hyvä ja kirjoita kysymys.")
            return
            
        with st.spinner("Generoidaan SQL-kyselyä..."):
            try:
                # Näytetään kysymys selkeästi
                st.subheader("Esitetty kysymys:")
                st.info(question)
                
                # Generoidaan SQL
                sql_query = generate_sql_from_natural_language(question)
                
                if not sql_query:
                    st.error("SQL-kyselyn generointi epäonnistui. Kokeile muotoilla kysymyksesi toisin.")
                    return
                
                # Sanitoidaan ja validoidaan SQL
                try:
                    sql_query = sanitize_sql_query(sql_query)
                except ValueError as e:
                    st.error(f"SQL-kyselyn validointi epäonnistui: {str(e)}")
                    return
                
                # Näytetään generoitu SQL kehittäjille tai debug-tilassa
                if debug_mode:
                    st.subheader("Generoitu SQL-kysely:")
                    st.code(sql_query, language="sql")
                
                # Suoritetaan kysely
                with st.spinner("Suoritetaan kyselyä..."):
                    results = execute_sql_query(sql_query)
                
                if results is not None and not results.empty:
                    st.subheader("Kyselyn tulokset:")
                    st.dataframe(results, use_container_width=True)
                    
                    # Tarjotaan CSV-latausmahdollisuus
                    csv = results.to_csv(index=False)
                    st.download_button(
                        label="Lataa tulokset CSV-tiedostona",
                        data=csv,
                        file_name="budjettihaukka_tulokset.csv",
                        mime="text/csv"
                    )
                    
                    # Visualisointiosuus
                    st.subheader("Visualisointi")
                    
                    # Tunnistetaan numeeriset sarakkeet
                    numeric_cols = results.select_dtypes(include=['float64', 'int64']).columns.tolist()
                    date_cols = [col for col in results.columns if 'date' in col.lower() or 'vuosi' in col.lower()]
                    
                    if numeric_cols and len(results) > 1:
                        # Jos on aikasarake, käytetään sitä x-akselina
                        if date_cols:
                            x_column = date_cols[0]
                            for y_column in numeric_cols:
                                st.subheader(f"{y_column} ajan funktiona")
                                chart_data = results.set_index(x_column)[y_column]
                                st.line_chart(chart_data)
                        # Muuten näytetään kaikki numeeriset sarakkeet
                        else:
                            st.line_chart(results[numeric_cols])
                    else:
                        st.warning("Ei tarpeeksi numeerista dataa visualisointia varten.")
                else:
                    st.warning("Kysely ei palauttanut tuloksia. Kokeile muokata kysymystäsi.")
            
            except Exception as e:
                st.error(f"Virhe sovelluksessa: {str(e)}")
                if debug_mode:
                    st.exception(e)

if __name__ == "__main__":
    main()