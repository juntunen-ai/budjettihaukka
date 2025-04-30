from google.cloud import bigquery
import re

def test_connection():
    try:
        client = bigquery.Client()
        query = "SELECT CURRENT_DATE() AS paiva"
        # Ensure all table and column names are enclosed in backticks in the test query
        query = re.sub(r'(?<!`)\b(\w+)\b(?!`)', r'`\1`', query)
        result = client.query(query).result()
        for row in result:
            print(f"✅ Yhteys toimii! Päivämäärä: {row.paiva}")
    except Exception as e:
        print(f"❌ Virhe yhteydessä: {e}")

if __name__ == "__main__":
    test_connection()

