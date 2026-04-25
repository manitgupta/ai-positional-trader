from typing import Annotated, List, Dict, Any
from typing_extensions import TypedDict
import operator
import json
import os
from langgraph.graph import StateGraph, START, END
from langgraph.constants import Send
from google import genai
from google.genai import types
from dotenv import load_dotenv

from src.analyst.prompts_v2 import CANDIDATE_EVALUATOR_PROMPT, SYNTHESIZER_PROMPT
from src.analyst.gemini_call import ANALYST_TOOLS
from src.analyst.parser import extract_json_blocks
from src.analyst.context_builder import ContextBuilder
from config import GEMINI_MODEL, DB_PATH
import pandas as pd

load_dotenv(override=True)

# 1. Define State
class OverallState(TypedDict):
    candidates: List[str]
    candidates_df: pd.DataFrame
    evaluations: Annotated[List[Dict[str, Any]], operator.add]
    final_memo: str

class CandidateState(TypedDict):
    candidate: str
    context: str

# 2. Define Nodes

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
    
    Please analyze {candidate} following the mandatory steps and output the JSON evaluation.
    """
    
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
        print(f"❌ Error evaluating {candidate}: {e}")
        return {"evaluations": [{"symbol": candidate, "action": "HOLD", "thesis": f"Error during analysis: {e}"}]}

def synthesize_memo(state: OverallState):
    evaluations = state["evaluations"]
    
    print(f"📝 Synthesizing memo for {len(evaluations)} candidates...")
    
    client = genai.Client()
    
    cfg = types.GenerateContentConfig(
        system_instruction=SYNTHESIZER_PROMPT,
        temperature=0.7,
    )
    
    prompt = f"""
    Here are the individual evaluations for candidates:
    
    {json.dumps(evaluations, indent=2)}
    
    Please synthesize these into the final research memo in the requested format.
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
    return [Send("evaluate_candidate", {
        "candidate": c, 
        "context": context_builder.build_context(state["candidates_df"], target_symbol=c)
    }) for c in state["candidates"]]

workflow = StateGraph(OverallState)

# Add nodes
workflow.add_node("evaluate_candidate", evaluate_candidate)
workflow.add_node("synthesize_memo", synthesize_memo)

# Add edges
workflow.add_conditional_edges(
    START,
    map_candidates,
    ["evaluate_candidate"]
)

workflow.add_edge("evaluate_candidate", "synthesize_memo")
workflow.add_edge("synthesize_memo", END)

# Compile
app = workflow.compile()
