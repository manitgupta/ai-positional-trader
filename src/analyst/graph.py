from typing import Annotated, List, Dict, Any
from typing_extensions import TypedDict
import operator
import json
import os
import time
from langgraph.graph import StateGraph, START, END
from langgraph.constants import Send
from google import genai
from google.genai import types
from dotenv import load_dotenv

from src.analyst.prompts_v2 import CANDIDATE_EVALUATOR_PROMPT, SYNTHESIZER_PROMPT, CRITIC_SELECTOR_PROMPT
from src.analyst.gemini_call import ANALYST_TOOLS
from src.analyst.parser import extract_json_blocks
from src.analyst.context_builder import ContextBuilder
from src.analyst.tools import get_macro_snapshot
from config import GEMINI_MODEL, DB_PATH
import pandas as pd

load_dotenv(override=True)

# 1. Define State
class OverallState(TypedDict):
    candidates: List[str]
    candidates_df: pd.DataFrame
    evaluations: Annotated[List[Dict[str, Any]], operator.add]
    selected_candidates: List[Dict[str, Any]]
    final_memo: str
    macro_snapshot: str

class CandidateState(TypedDict):
    candidate: str
    context: str

# 2. Define Nodes

def fetch_macro_data(state: OverallState):
    print("🔧 Fetching macro snapshot once for all candidates...")
    macro = get_macro_snapshot()
    return {"macro_snapshot": macro}

def evaluate_candidate(state: CandidateState):
    candidate = state["candidate"]
    context = state["context"]
    
    print(f"🤖 Evaluating candidate: {candidate}")
    
    client = genai.Client()
    
    cfg = types.GenerateContentConfig(
        system_instruction=CANDIDATE_EVALUATOR_PROMPT,
        temperature=0.7, # Lower temp for more consistent JSON
        tools=ANALYST_TOOLS,
        automatic_function_calling=types.AutomaticFunctionCallingConfig(
            maximum_remote_calls=50
        )
    )
    
    prompt = f"""
    Target Candidate: {candidate}
    
    General Context:
    {context}
    
    Check if {candidate} is in the "Open positions" list in the General Context.
    - If it IS an open position, analyze it to determine if the thesis is intact and if you should HOLD, EXIT, or TRAIL_STOP. Use the pre-fetched position details provided in the context. Do not call get_open_position_detail.
    - If it is NOT an open position, analyze it as a potential new opportunity or watchlist item.
    
    Please analyze {candidate} following the mandatory steps and output the JSON evaluation.
    """
    
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=cfg,
            )
            
            # Extract JSON from response
            text = response.text
            blocks = extract_json_blocks(text)
            
            if blocks:
                evaluation = blocks[0]
            else:
                # Fallback: try to parse whole text as JSON if no markdown blocks
                try:
                    evaluation = json.loads(text.strip())
                except json.JSONDecodeError:
                    print(f"❌ Failed to parse evaluation for {candidate}. Raw text: {text[:200]}...")
                    evaluation = {"symbol": candidate, "action": "HOLD", "thesis": f"Failed to parse analysis. Raw: {text[:100]}"}
                    
            return {"evaluations": [evaluation]}
            
        except Exception as e:
            print(f"❌ Error evaluating {candidate} (Attempt {attempt + 1}/{max_attempts}): {e}")
            if attempt == max_attempts - 1:
                print(f"Skipping {candidate} after {max_attempts} failed attempts.")
                return {"evaluations": [{"symbol": candidate, "action": "HOLD", "thesis": f"Error during analysis: {e}"}]}
            
            sleep_time = 2 ** attempt
            print(f"Waiting {sleep_time} seconds before retry...")
            time.sleep(sleep_time)

def critic_selector(state: OverallState):
    evaluations = state["evaluations"]
    
    print(f"🧐 Critically reviewing {len(evaluations)} candidates...")
    
    client = genai.Client()
    
    cfg = types.GenerateContentConfig(
        system_instruction=CRITIC_SELECTOR_PROMPT,
        temperature=0.5,
    )
    
    prompt = f"""
    Here are the evaluations provided by the analysts:
    
    {json.dumps(evaluations, indent=2)}
    
    Please critically review them and select the top opportunities.
    """
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=cfg,
        )
        
        text = response.text
        blocks = extract_json_blocks(text)
        
        if blocks:
            selection = blocks[0]
        else:
            try:
                selection = json.loads(text.strip())
            except json.JSONDecodeError:
                print(f"❌ Failed to parse selection. Raw text: {text[:200]}...")
                selection = []
                
        return {"selected_candidates": selection}
    except Exception as e:
        print(f"❌ Error in critic_selector: {e}")
        return {"selected_candidates": []}

def synthesize_memo(state: OverallState):
    evaluations = state["evaluations"]
    selected_candidates = state.get("selected_candidates", [])
    macro_snapshot = state.get("macro_snapshot", "")
    
    print(f"📝 Synthesizing memo for {len(selected_candidates)} selected candidates...")
    
    client = genai.Client()
    
    cfg = types.GenerateContentConfig(
        system_instruction=SYNTHESIZER_PROMPT,
        temperature=0.7,
    )
    
    # Fetch open positions to pass to synthesizer
    context_builder = ContextBuilder(DB_PATH)
    open_positions_text = context_builder._open_positions()
    
    prompt = f"""
    Here is the Macro Snapshot for the market:
    {macro_snapshot}
    
    Here are the current Open Positions (symbol + entry info):
    {open_positions_text}
    
    Here are the detailed evaluations provided by the analysts:
    {json.dumps(evaluations, indent=2)}
    
    Here is the final selection and justification from the Critic/Selector:
    {json.dumps(selected_candidates, indent=2)}
    
    Please synthesize these into the final research memo in the requested format. 
    Ensure that ALL Open Positions listed above are reviewed in SECTION 1 (Portfolio Review), utilizing the evaluations if available.
    Use the full evaluations to write detailed theses for the selected candidates in SECTION 2.
    Ensure you reference the Macro Snapshot where relevant to contextualize the environment.
    """
    
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=cfg,
        )
        
        return {"final_memo": response.text}
    except Exception as e:
        print(f"❌ Error synthesizing memo: {e}")
        return {"final_memo": f"Error synthesizing memo: {e}"}

# 3. Define Graph

def map_candidates(state: OverallState):
    # This is the fan-out logic
    context_builder = ContextBuilder(DB_PATH)
    
    # Pre-fetch open position details
    import duckdb
    open_positions_df = pd.DataFrame()
    try:
        with duckdb.connect(DB_PATH, read_only=True) as c:
            open_positions_df = c.execute("""
                SELECT p.symbol, p.entry_date, p.entry_price, p.quantity,
                       p.stop_loss, p.target, p.position_pct, p.thesis_summary,
                       pr.close AS current_close, s.rsi_14, s.adx_14,
                       round((pr.close - p.entry_price) / p.entry_price * 100, 2) AS pnl_pct
                FROM portfolio p
                LEFT JOIN signals s ON p.symbol = s.symbol
                LEFT JOIN prices  pr ON p.symbol = pr.symbol AND s.date = pr.date
                WHERE p.status = 'OPEN'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY p.symbol ORDER BY s.date DESC) = 1
            """).fetchdf()
    except Exception as e:
        print(f"Error pre-fetching open positions: {e}")
        
    open_position_symbols = set(open_positions_df["symbol"].tolist()) if not open_positions_df.empty else set()
    
    send_tasks = []
    for c in state["candidates"]:
        context = context_builder.build_context(state["candidates_df"], target_symbol=c, macro_snapshot=state.get("macro_snapshot"))
        
        if c in open_position_symbols:
            pos_detail = open_positions_df[open_positions_df["symbol"] == c]
            pos_detail_str = pos_detail.to_string(index=False)
            context += f"\n\n## Open Position Detail (pre-fetched — use this, do not call get_open_position_detail)\n{pos_detail_str}"
            
        send_tasks.append(Send("evaluate_candidate", {"candidate": c, "context": context}))
        
    return send_tasks

workflow = StateGraph(OverallState)

# Add nodes
workflow.add_node("fetch_macro_data", fetch_macro_data)
workflow.add_node("evaluate_candidate", evaluate_candidate)
workflow.add_node("critic_selector", critic_selector)
workflow.add_node("synthesize_memo", synthesize_memo)

# Add edges
workflow.add_edge(START, "fetch_macro_data")
workflow.add_conditional_edges(
    "fetch_macro_data",
    map_candidates,
    ["evaluate_candidate"]
)

workflow.add_edge("evaluate_candidate", "critic_selector")
workflow.add_edge("critic_selector", "synthesize_memo")
workflow.add_edge("synthesize_memo", END)

# Compile
app = workflow.compile()
