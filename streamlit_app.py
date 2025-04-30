import streamlit as st
from utils.vertex_ai_utils import generate_sql_from_natural_language
from google.cloud import bigquery
import re

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
        query_job = client.query(sql_query)
        results = query_job.result()
        return results.to_dataframe()
    except Exception as e:
        st.error(f"Failed to execute SQL query: {e}")
        return None

def main():
    st.title("Budjettihaukka")
    st.write("Budjettihaukka on avoimen lähdekoodin web-sovellus, jonka tarkoituksena on tuoda talouspolitiikkaan liittyvä tieto helposti saataville, analysoitavaksi ja visualisoitavaksi. Sovelluksen käyttäjä voi esittää kysymyksiä luonnollisella kielellä, ja tekoälyn avulla saa kansantaloudelliseen optimaalisuuteen ja empiiriseen taloustutkimukseen perustubia analyyseja. Tulokset voidaan näyttää taulukkoina, dynaamisina visualisointeina sekä analyyttisinä raportteina.")

    # Input for natural language question
    question = st.text_area("Enter your question:", "")

    if st.button("Generate SQL and Fetch Results"):
        if question.strip():
            with st.spinner("Generating SQL..."):
                sql_query = generate_sql_from_natural_language(question)

            if sql_query:
                # Log the generated SQL query for debugging purposes
                st.write("Debug: Generated SQL Query")
                st.code(sql_query, language="sql")

                # Fix backtick usage for table and column names, avoiding nested backticks
                def fix_backticks(query):
                    # Apply backticks only to valid identifiers (e.g., table and column names)
                    tokens = query.split()
                    fixed_tokens = []
                    for token in tokens:
                        # Allow Scandinavian alphabets and special characters like hyphens in identifiers
                        if re.match(r'^[a-zA-ZäöåÄÖÅ_][a-zA-ZäöåÄÖÅ0-9_\-]*$', token) and not token.startswith('`'):
                            fixed_tokens.append(f"`{token}`")
                        else:
                            fixed_tokens.append(token)
                    return " ".join(fixed_tokens)

                sql_query = fix_backticks(sql_query)

                st.subheader("Generated SQL Query")
                st.code(sql_query, language="sql")

                # Validate SQL query structure before execution
                if not sql_query.strip().lower().startswith("select"):
                    st.error("Invalid SQL query. Only SELECT statements are allowed.")

                    # Provide feedback to the user and ask for clarification
                    st.warning("The generated SQL query seems invalid or ambiguous. Please refine your question or provide more details.")
                    st.text_area("Refine your question:", value=question, key="refine_question")
                    return

                with st.spinner("Executing SQL query..."):
                    results = execute_sql_query(sql_query)

                if results is not None:
                    st.subheader("Query Results")
                    st.dataframe(results)

                    # Add dynamic visualization if there is enough data
                    if len(results) > 1:  # Check if there is enough data for visualization
                        st.subheader("Visualization")
                        st.line_chart(results)
                    else:
                        st.warning("Not enough data for visualization.")
                else:
                    st.error("Failed to fetch query results. Please try again.")
            else:
                st.error("Failed to generate SQL query. Please try again.")
        else:
            st.warning("Please enter a question to generate SQL.")

if __name__ == "__main__":
    main()