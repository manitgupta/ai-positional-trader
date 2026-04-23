import re
import json

def extract_json_blocks(text):
    """
    Extracts JSON blocks from a markdown text.
    Looks for ```json ... ``` blocks.
    """
    pattern = r'```json\s*([\s\S]*?)\s*```'
    matches = re.findall(pattern, text)
    
    extracted_data = []
    for match in matches:
        try:
            data = json.loads(match.strip())
            extracted_data.append(data)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON block: {e}")
            # Try to clean it up if needed, or just skip
            pass
            
    return extracted_data

if __name__ == "__main__":
    test_text = """
Some text here.

```json
{
  "section": "new_opportunities",
  "decisions": [
    {"symbol": "XYZ", "action": "BUY"}
  ]
}
```

More text.

```json
{
  "section": "watchlist",
  "decisions": []
}
```
"""
    blocks = extract_json_blocks(test_text)
    print(f"Extracted {len(blocks)} blocks.")
    print(blocks)
