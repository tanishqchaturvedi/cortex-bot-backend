import os
import requests
from fastapi import FastAPI
from pydantic import BaseModel
from snowflake.snowpark import Session

app = FastAPI()

connection_params = {
    "account": "ZJIANGD-ES06588",   # e.g. zjiangd-es06588.central-india.azure
    "user": "CHATURVEDIT",
    "password": "iL5WaCxsU8HQKbT",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "DEMO_INVENTORY",
    "schema": "PUBLIC"
}

session = Session.builder.configs(connection_params).create()

# --------------------
# Cortex Analyst API
# --------------------
PAT = os.getenv("cortex_pat")
# Replace with your actual account locator and token
ACCOUNT_IDENTIFIER = "zjiangd-es06588"
CORTEX_ANALYST_URL = f"https://{ACCOUNT_IDENTIFIER}.snowflakecomputing.com/api/v2/cortex/analyst/message"
AUTH_TOKEN = PAT  # ensure your PAT is set

class Query(BaseModel):
    user_query: str

@app.post("/chat")
async def chat(query: Query):
    payload = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": query.user_query}]}
        ],
        "semantic_view": "DEMO_INVENTORY.PUBLIC.INVENTORY_ANALYSIS",
        "stream": False
    }

    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(CORTEX_ANALYST_URL, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        response_json = resp.json()

        message = response_json.get("message", {})
        content_blocks = message.get("content", [])

        interpretation, sql, suggestions = None, None, []

        for block in content_blocks:
            if block["type"] == "text":
                interpretation = block.get("text")
            elif block["type"] == "sql":
                sql = block.get("statement")
            elif block["type"] == "suggestions":
                suggestions = block.get("suggestions", [])

        # ✅ Run the generated SQL in Snowflake
        results = []
        if sql:
            try:
                df = session.sql(sql)
                results = df.collect()  # fetch results into Python list of Row objects
                # convert to plain dicts
                results = [row.as_dict() for row in results]
            except Exception as e:
                results = [{"error": f"Failed to execute SQL: {str(e)}"}]

        return {
            "answer": interpretation,
            "sql": sql,
            "results": results,       # <-- actual data output here
            "suggestions": suggestions,
            "request_id": response_json.get("request_id"),
            "warnings": [w.get("message") for w in response_json.get("warnings", [])],
        }

    except Exception as e:
        return {"error": str(e)}