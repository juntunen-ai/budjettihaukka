import unittest
from utils.bigquery_utils import process_natural_language_query

questions = [
    "Valitse kaikki rivit taulusta.",
    "Näytä kaikki sarakkeet taulusta.",
    "Kuinka monta riviä taulussa on?",
    "Mitkä ovat taulun sarakkeiden nimet?",
    "Näytä ensimmäiset 10 riviä taulusta."
]

for question in questions:
    print(f"\n🧠 Kysymys: {question}")
    result = process_natural_language_query(question)
    print("🔎 Generoitu SQL:\n", result["sql_query"])
    print("📋 Selitys:", result["explanation"])

    if not result["results_df"].empty:
        print("\n✅ Tulokset:")
        print(result["results_df"])
    else:
        print("\n⚠️ Ei tuloksia.")

class TestSQLQueryGeneration(unittest.TestCase):
    def test_query_generation(self):
        questions = [
            "Valitse kaikki rivit taulusta.",
            "Näytä kaikki sarakkeet taulusta.",
            "Kuinka monta riviä taulussa on?",
            "Mitkä ovat taulun sarakkeiden nimet?",
            "Näytä ensimmäiset 10 riviä taulusta."
        ]

        for question in questions:
            result = process_natural_language_query(question)

            # Check if SQL query is generated
            self.assertIn("sql_query", result, "SQL query key missing in result")
            self.assertTrue(result["sql_query"].strip(), "Generated SQL query is empty")

            # Check if explanation is provided
            self.assertIn("explanation", result, "Explanation key missing in result")
            self.assertTrue(result["explanation"].strip(), "Explanation is empty")

            # Check if results are returned or handled properly
            self.assertIn("results_df", result, "Results DataFrame key missing in result")
            self.assertIsNotNone(result["results_df"], "Results DataFrame is None")

if __name__ == "__main__":
    unittest.main()

