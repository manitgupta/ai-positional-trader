import os
import time
from google import genai
from google.genai import types
from dotenv import load_dotenv

from src.analyst.prompts import SYSTEM_PROMPT
from src.analyst.tools import (
    get_price_history,
    get_weekly_history,
    get_annual_fundamentals,
    get_quarterly_fundamentals,
    get_news,
    get_research_notes,
    get_position_history,
    get_macro_snapshot,
    get_breadth,
    get_earnings_calendar,
    get_sector_peers,
    get_sector_relative_strength,
)
from src.analyst.db_tool import search_web
from config import GEMINI_MODEL

load_dotenv(override=True)

ANALYST_TOOLS = [
    get_price_history,
    get_weekly_history,
    get_annual_fundamentals,
    get_quarterly_fundamentals,
    get_news,
    get_research_notes,
    get_position_history,
    search_web,
    get_macro_snapshot,
    get_breadth,
    get_earnings_calendar,
    get_sector_peers,
    get_sector_relative_strength,
]


class GeminiAnalyst:
    def __init__(self):
        if not os.environ.get("GEMINI_API_KEY"):
            raise ValueError("GEMINI_API_KEY environment variable is required.")
        self.model_name = GEMINI_MODEL
        print("Initializing Gemini Client...")
        self.client = genai.Client()

    def _call(self, contents: str, config: types.GenerateContentConfig, label: str) -> str:
        delay = 2
        for attempt in range(10):
            print(f"Calling Gemini ({self.model_name}) [{label}] — attempt {attempt + 1}/10")
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=contents,
                    config=config,
                )
                return response.text
            except Exception as e:
                print(f"Gemini call failed: {e}")
                if attempt == 9:
                    raise
                print(f"Retrying in {delay}s...")
                time.sleep(delay)
                delay = min(delay * 2, 60)

    def generate_memo(self, kernel_context: str) -> str:
        """Run the full agentic research loop using a Chat session."""
        cfg = types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=1.0,
            tools=ANALYST_TOOLS,
            automatic_function_calling=types.AutomaticFunctionCallingConfig(
                maximum_remote_calls=500
            )
        )
        print("Creating chat session for memo...")
        chat = self.client.chats.create(
            model=self.model_name,
            config=cfg
        )
        
        delay = 2
        for attempt in range(10):
            print(f"Calling Gemini Chat ({self.model_name}) [memo] — attempt {attempt + 1}/10")
            try:
                response = chat.send_message(kernel_context)
                return response.text
            except Exception as e:
                print(f"Gemini chat failed: {e}")
                if attempt == 9:
                    raise
                print(f"Retrying in {delay}s...")
                time.sleep(delay)
                delay = min(delay * 2, 60)

    def generate_summary(self, memo: str, prompt: str) -> str:
        """Condense the memo into a Telegram-ready summary. No tools needed."""
        cfg = types.GenerateContentConfig(temperature=0.7)
        return self._call(f"{prompt}\n\nResearch Memo:\n{memo}", cfg, "summary")


if __name__ == "__main__":
    analyst = GeminiAnalyst()
    test_context = "Today: 2025-01-01\n\nMacro: Nifty above 200 MA.\n\nOpen positions: (none)\n\nCandidates: RELIANCE, TCS"
    memo = analyst.generate_memo(test_context)
    print(memo)
