import streamlit as st
from utils.vertex_ai_utils import generate_sql_from_natural_language
from google.cloud import bigquery
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

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

def generate_sample_budget_data():
    """
    Generoi esimerkkidataa budjettidatasta visualisoinnin testaamiseen.
    
    Returns:
        pd.DataFrame: Esimerkkibudjettidataa
    """
    # Luodaan esimerkki kategoriat
    categories = [
        'Puolustusministeriö', 
        'Opetusministeriö', 
        'Sosiaali- ja terveysministeriö',
        'Liikenne- ja viestintäministeriö',
        'Valtiovarainministeriö',
        'Ympäristöministeriö'
    ]
    
    # Luodaan vuodet
    years = list(range(2018, 2025))
    
    # Alustetaan data lista
    data = []
    
    # Luodaan jokaiselle kategorialle ja vuodelle dataa
    for category in categories:
        base_amount = random.randint(100, 1000) * 1000000  # Perusmäärä miljoonissa
        
        for year in years:
            # Lisätään hieman satunnaisuutta, mutta pidetään trendi
            yearly_change = random.uniform(-0.1, 0.2)  # -10% to +20% vuosimuutos
            amount = base_amount * (1 + yearly_change)
            
            # Lisätään vuosineljännes data
            for quarter in range(1, 5):
                quarterly_amount = amount / 4 * (1 + random.uniform(-0.05, 0.05))
                
                data.append({
                    'Vuosi': year,
                    'Vuosineljännes': quarter,
                    'Ministeriö': category,
                    'Määräraha_EUR': round(quarterly_amount, 2),
                    'Päivämäärä': f"{year}-{quarter*3:02d}-01"
                })
                
        # Lisätään vaihtelua perusmäärään seuraavaa kategoriaa varten
        base_amount = base_amount * (1 + random.uniform(-0.3, 0.3))
    
    # Luodaan DataFrame
    df = pd.DataFrame(data)
    
    # Muunnetaan päivämäärä-sarake datetime-tyyppiseksi
    df['Päivämäärä'] = pd.to_datetime(df['Päivämäärä'])
    
    return df

def visualize_data(df, title="Budjettidata visualisointi"):
    """
    Visualisoi budjettidata monipuolisesti.
    
    Parameters:
        df (pd.DataFrame): Visualisoitava dataframe
        title (str): Visualisoinnin otsikko
    """
    st.subheader(title)
    
    # Tarkistetaan, onko dataframessa sarakkeita
    if df.empty or len(df.columns) == 0:
        st.warning("Ei dataa visualisoitavaksi.")
        return
    
    # Sarakkeet joita voi visualisoida
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    date_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    
    if not numeric_cols:
        st.warning("Ei numeerista dataa visualisoitavaksi.")
        return
    
    # Visualisointivaihtoehdot
    viz_options = st.radio(
        "Valitse visualisoinnin tyyppi:",
        ["Aikasarja", "Kategoriavertailu", "Ministeriöiden määrärahat vuosittain", "Vuosittainen kehitys"],
        index=0
    )
    
    if viz_options == "Aikasarja" and date_cols:
        # Aikasarjan visualisointi
        st.subheader("Määrärahojen kehitys ajan funktiona")
        
        # Mahdollisuus valita ministeriö
        if 'Ministeriö' in df.columns:
            ministries = df['Ministeriö'].unique().tolist()
            selected_ministries = st.multiselect(
                "Valitse ministeriöt:",
                ministries,
                default=ministries[:3] if len(ministries) > 3 else ministries
            )
            
            filtered_df = df[df['Ministeriö'].isin(selected_ministries)]
        else:
            filtered_df = df
        
        # Aikasarjan visualisointi
        if not filtered_df.empty:
            # Ryhmittely päivämäärän mukaan ja summataan määrärahat
            if 'Määräraha_EUR' in filtered_df.columns and date_cols:
                date_col = date_cols[0]
                
                # Ryhmittele päivämäärän ja ministeriön mukaan
                if 'Ministeriö' in filtered_df.columns:
                    chart_data = filtered_df.pivot_table(
                        index=date_col, 
                        columns='Ministeriö', 
                        values='Määräraha_EUR',
                        aggfunc='sum'
                    )
                else:
                    chart_data = filtered_df.groupby(date_col)['Määräraha_EUR'].sum()
                
                st.line_chart(chart_data)
    
    elif viz_options == "Kategoriavertailu" and 'Ministeriö' in df.columns and numeric_cols:
        st.subheader("Määrärahat ministeriöittäin")
        
        # Valitse vuosi jos mahdollista
        if 'Vuosi' in df.columns:
            years = sorted(df['Vuosi'].unique().tolist())
            selected_year = st.selectbox("Valitse vuosi:", years, index=len(years)-1)
            filtered_df = df[df['Vuosi'] == selected_year]
        else:
            filtered_df = df
        
        # Ryhmittele ministeriöittäin
        if 'Määräraha_EUR' in filtered_df.columns:
            chart_data = filtered_df.groupby('Ministeriö')['Määräraha_EUR'].sum().sort_values(ascending=False)
            st.bar_chart(chart_data)
    
    elif viz_options == "Ministeriöiden määrärahat vuosittain" and 'Ministeriö' in df.columns and 'Vuosi' in df.columns:
        st.subheader("Ministeriöiden määrärahat vuosittain")
        
        # Valitse ministeriö
        ministries = df['Ministeriö'].unique().tolist()
        selected_ministry = st.selectbox("Valitse ministeriö:", ministries)
        
        filtered_df = df[df['Ministeriö'] == selected_ministry]
        
        # Ryhmittele vuosittain
        if 'Määräraha_EUR' in filtered_df.columns:
            chart_data = filtered_df.groupby('Vuosi')['Määräraha_EUR'].sum()
            
            # Laske muutosprosentti edellisestä vuodesta
            chart_data_pct = chart_data.pct_change() * 100
            
            # Näytä määrärahat
            st.subheader(f"{selected_ministry} - Määrärahat vuosittain")
            st.line_chart(chart_data)
            
            # Näytä muutosprosentit
            st.subheader(f"{selected_ministry} - Määrärahojen vuosimuutos (%)")
            st.bar_chart(chart_data_pct.iloc[1:])  # Poistetaan ensimmäinen arvo (NaN)
    
    elif viz_options == "Vuosittainen kehitys" and 'Vuosi' in df.columns:
        st.subheader("Määrärahojen kehitys vuosittain")
        
        # Ryhmittele vuosittain ja ministeriöittäin
        if 'Määräraha_EUR' in df.columns:
            pivot_data = df.pivot_table(
                index='Vuosi',
                columns='Ministeriö' if 'Ministeriö' in df.columns else None,
                values='Määräraha_EUR',
                aggfunc='sum'
            )
            
            # Näytä absoluuttiset summat
            st.line_chart(pivot_data)
            
            # Näytä prosentuaalinen jakauma
            st.subheader("Määrärahojen suhteellinen jakauma vuosittain")
            relative_data = pivot_data.div(pivot_data.sum(axis=1), axis=0) * 100
            st.area_chart(relative_data)

def main():
    st.set_page_config(page_title="Budjettihaukka", layout="wide")
    st.title("Budjettihaukka")
    st.write("Budjettihaukka on avoimen lähdekoodin web-sovellus, jonka tarkoituksena on tuoda talouspolitiikkaan liittyvä tieto helposti saataville, analysoitavaksi ja visualisoitavaksi. Sovelluksen käyttäjä voi esittää kysymyksiä luonnollisella kielellä, ja tekoälyn avulla saa kansantaloudelliseen optimaalisuuteen ja empiiriseen taloustutkimukseen perustuvia analyyseja. Tulokset voidaan näyttää taulukkoina, dynaamisina visualisointeina sekä analyyttisinä raportteina.")

    # Lisätään diagnostiikkatila kehittäjille
    debug_mode = st.sidebar.checkbox("Kehittäjätila", value=False)
    
    # Lisätään visualisoinnin testaustila
    test_visualization = st.sidebar.checkbox("Testaa visualisointia", value=False)
    
    if test_visualization:
        st.header("Visualisoinnin testaus")
        st.write("Tämä tila käyttää generoitua esimerkkidataa visualisoinnin testaamiseen.")
        
        # Generoidaan esimerkkidata
        sample_data = generate_sample_budget_data()
        
        # Näytetään esimerkkidata
        st.subheader("Esimerkkidata")
        st.dataframe(sample_data.head(10), use_container_width=True)
        
        # Visualisoidaan data
        visualize_data(sample_data)
        
        # Tarjotaan CSV-latausmahdollisuus
        csv = sample_data.to_csv(index=False)
        st.download_button(
            label="Lataa esimerkkidata CSV-tiedostona",
            data=csv,
            file_name="budjettihaukka_esimerkkidata.csv",
            mime="text/csv"
        )
        
        return  # Palataan tästä, jos testataan visualisointia
    
    # Normaalin sovelluksen kulku tästä eteenpäin
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
                    
                    # Visualisoi tulokset
                    visualize_data(results, title="Kyselyn tulokset visualisoituna")
                else:
                    st.warning("Kysely ei palauttanut tuloksia. Kokeile muokata kysymystäsi.")
            
            except Exception as e:
                st.error(f"Virhe sovelluksessa: {str(e)}")
                if debug_mode:
                    st.exception(e)

    # Alatunnisteen näyttäminen
    st.markdown("---")
    st.markdown("Budjettihaukka | Powered by LangGraph & Vertex AI © 2025")

if __name__ == "__main__":
    main()