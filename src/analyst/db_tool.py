import os
import duckdb
import pandas as pd
from config import DB_PATH, connect_db
from google import genai
from google.genai import types

def execute_read_only_query(query: str) -> str:
    """
    Executes a read-only SQL query on the DuckDB database and returns results.
    Only SELECT statements are allowed.
    
    Args:
        query: The SQL query to execute.
        
    Returns:
        A string representation of the result (table format) or an error message.
    """
    print(f"Gemini requested DB query: {query}")
    
    # Basic validation for read-only
    query_clean = query.strip().upper()
    if not query_clean.startswith("SELECT"):
        return "Error: Only SELECT queries are allowed."
        
    forbidden_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "REPLACE", "TRUNCATE"]
    for keyword in forbidden_keywords:
        # Simple check for keyword presence
        if keyword in query_clean:
            return f"Error: Modification keyword '{keyword}' detected. Query rejected."
            
    conn = connect_db(DB_PATH, read_only=True)
    try:
        result_df = conn.execute(query).fetchdf()
        if result_df.empty:
            return "No results found."
        return result_df.to_string(index=False)
    except Exception as e:
        print(f"Error executing query: {e}")
        return f"Error executing query: {str(e)}"
    finally:
        conn.close()

def search_web(query: str) -> str:
    """
    Searches the web for information, documentation, or solutions to errors using Google Search.
    
    Args:
        query: The search query.
        
    Returns:
        A string containing the search results.
    """
    print(f"Gemini requested web search for: {query}")
    try:
        client = genai.Client()
        search_config = types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
            temperature=0.0
        )
        response = client.models.generate_content(
            model="gemini-2.5-flash", 
            contents=f"Search the web to answer this query: {query}",
            config=search_config
        )
        if response.text:
            return response.text
        return "No relevant information found."
    except Exception as e:
        print(f"Web search failed: {e}")
        return f"ERROR performing web search: {str(e)}"
