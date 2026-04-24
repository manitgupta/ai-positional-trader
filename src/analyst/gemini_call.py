import os
from google import genai
from google.genai import types
from dotenv import load_dotenv
from src.analyst.prompts import SYSTEM_PROMPT
from config import GEMINI_MODEL

load_dotenv(override=True)

class GeminiAnalyst:
    def __init__(self):
        self.api_key = os.environ.get("GEMINI_API_KEY")
        self.model_name = GEMINI_MODEL
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required.")
            
        print("Initializing Gemini Client...")
        self.client = genai.Client()
            
    def generate_memo(self, context: str) -> str:
        import time
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            print(f"Calling Gemini ({self.model_name})... (Attempt {attempt + 1}/{max_retries})")
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=context,
                    config=types.GenerateContentConfig(
                        system_instruction=SYSTEM_PROMPT,
                        temperature=1.0,
                    ),
                )
                return response.text
            except Exception as e:
                print(f"Gemini call failed: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise e

    def generate_summary(self, memo: str, prompt: str) -> str:
        """Generate a concise summary of the memo for Telegram."""
        import time
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            print(f"Calling Gemini ({self.model_name}) for summary... (Attempt {attempt + 1}/{max_retries})")
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=f"{prompt}\n\nResearch Memo:\n{memo}",
                    config=types.GenerateContentConfig(
                        temperature=0.7,
                    ),
                )
                return response.text
            except Exception as e:
                print(f"Error generating summary: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    return "Failed to generate summary."

if __name__ == "__main__":
    analyst = GeminiAnalyst()
    test_context = "Macro looks good. No open positions. Candidates: RELIANCE, TCS."
    memo = analyst.generate_memo(test_context)
    print(memo)
