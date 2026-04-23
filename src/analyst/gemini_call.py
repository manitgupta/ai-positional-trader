import os
from google import genai
from google.genai import types
from dotenv import load_dotenv
from src.analyst.prompts import SYSTEM_PROMPT

load_dotenv()

class GeminiAnalyst:
    def __init__(self):
        self.api_key = os.environ.get("GEMINI_API_KEY")
        self.model_name = "gemini-2.5-pro"
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required.")
            
        print("Initializing Gemini Client...")
        self.client = genai.Client(api_key=self.api_key)
            
    def generate_memo(self, context: str) -> str:
        print(f"Calling Gemini ({self.model_name})...")
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=context,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                temperature=1.0,
                thinking_config=types.ThinkingConfig(thinking_budget=8000),
            ),
        )
        return response.text


if __name__ == "__main__":
    analyst = GeminiAnalyst()
    test_context = "Macro looks good. No open positions. Candidates: RELIANCE, TCS."
    memo = analyst.generate_memo(test_context)
    print(memo)
