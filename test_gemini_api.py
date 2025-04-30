import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

def test_gemini_api():
    vertexai.init(project="valtion-budjetti-data", location="us-central1")
    model = GenerativeModel("gemini-2.5-pro-preview-03-25")

    prompt = "SELECT * FROM `valtion-budjetti-data.valtiodata.budjettidata` LIMIT 10"
    generation_config = GenerationConfig(
        temperature=0.2,
        top_p=0.8,
        max_output_tokens=512
    )

    try:
        response = model.generate_content(prompt, generation_config=generation_config)
        print("Raw response from Gemini API:")
        print(response.text)
    except Exception as e:
        print(f"Error during Gemini API call: {e}")

if __name__ == "__main__":
    test_gemini_api()