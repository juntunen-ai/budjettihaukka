from utils.bigquery_utils import process_natural_language_query

question = "Kuinka paljon budjetoitiin koulutukseen vuonna 2023?"

result = process_natural_language_query(question)

print("\n🧠 Kysymys:", question)
print("🔎 Generoitu SQL:\n", result["sql_query"])
print("📋 Selitys:", result["explanation"])

if not result["results_df"].empty:
    print("\n✅ Tulokset:")
    print(result["results_df"])
else:
    print("\n⚠️ Ei tuloksia.")

