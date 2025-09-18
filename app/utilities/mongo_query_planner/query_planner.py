"""
MongoDB Query Planner and Generator Script

Features:
- Loads schema (JSON or CSV) - representing collections + fields
- Builds system prompt
- Accepts natural language query as input
- Returns structured query plan + MongoDB aggregation pipeline using OpenAI
"""

import json
import argparse
import pandas as pd
from collections import defaultdict
from openai import OpenAI
import os
from dotenv import load_dotenv
from natural_queries import natural_language_queries

# Load variables from .env file into environment
load_dotenv()

# Access them like normal environment variables
API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=API_KEY)

def load_schema(path="mongo_schema.json") -> str:
    """Read MongoDB schema file (JSON) as raw string."""
    with open(path, "r") as schema:
        return schema.read()


def load_schema_csv(path: str = "mongo_schema.csv") -> str:
    """Optional: Load MongoDB schema from CSV and format as string."""
    df = pd.read_csv(path)

    # Group fields by collection
    schema_dict = defaultdict(list)
    for _, row in df.iterrows():
        schema_dict[row["collection_name"]].append((row["field_name"], row["data_type"]))

    # Format schema
    formatted_schema = []
    for collection, fields in schema_dict.items():
        formatted_schema.append(f"Collection: {collection}")
        for field, dtype in fields:
            formatted_schema.append(f"- {field} ({dtype})")
        formatted_schema.append("")

    return "\n".join(formatted_schema)


def build_system_prompt(db_schema: str) -> str:
    """Build system prompt for MongoDB query planning."""

    return f"""
You are a MongoDB query planner assistant.
Your task:
1. Convert the user's natural language query into a structured query plan (JSON)
2. Generate a valid MongoDB aggregation pipeline for that plan

Database schema:
{db_schema}

Guidelines:
- Use aggregation framework (`$match`, `$group`, `$project`, `$lookup`, `$sort`, `$limit`).
- Use `$lookup` for joins between collections.
- If a collection has nested objects/arrays, use `$unwind` if necessary.
- Always include at least one human-readable field in the output (e.g., name, title, email).
- Avoid returning only `_id` fields.
- Validation checklist:
  1. The output must include readable fields.
  2. All lookups use correct localField/foreignField.
  3. Grouped queries include `$project` for clean output.

Return JSON only (no extra text) in this format:

{{
  "query_plan": {{
    "intent": "aggregation" | "filter" | "lookup" | "trend" | "comparison",
    "collection": "orders",
    "pipeline_steps": [
      {{ "stage": "$match", "expression": {{ "field": "value" }} }},
      {{ "stage": "$group", "expression": {{ "_id": "$customer_id", "total": {{ "$sum": "$amount" }} }} }},
      {{ "stage": "$sort", "expression": {{ "total": -1 }} }}
    ],
    "joins": [
      {{
        "from": "customers",
        "localField": "customer_id",
        "foreignField": "_id",
        "as": "customer"
      }}
    ],
    "display_fields": ["customer.name", "total"],
    "original_user_query": ...
  }},
  "mongo_query": [
    {{ "$match": {{ ... }} }},
    {{ "$group": {{ ... }} }},
    {{ "$lookup": {{ ... }} }},
    {{ "$project": {{ ... }} }},
    {{ "$sort": {{ ... }} }},
    {{ "$limit": ... }}
  ]
}}
"""


def get_plan_and_pipeline(system_prompt: str, user_query: str, model: str = "gpt-4o-mini") -> dict:
    """Call OpenAI to generate MongoDB query plan + aggregation pipeline."""
    if not API_KEY:
        raise ValueError("Missing OPENAI_API_KEY env variable")

    client = OpenAI(api_key=API_KEY)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ],
        temperature=0.2
    )

    content = response.choices[0].message.content.strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return {"error": "Invalid JSON returned", "raw_output": content}


def main():
    parser = argparse.ArgumentParser(description="MongoDB Query Planner CLI")
    parser.add_argument("--schema", type=str, default="mongo_schema.json", help="Path to schema file (JSON or CSV)")
    parser.add_argument("--output", type=str, default="results.json", help="File to save results")
    args = parser.parse_args()

    # Load schema
    if args.schema.endswith(".json"):
        db_schema = load_schema(args.schema)
    elif args.schema.endswith(".csv"):
        db_schema = load_schema_csv(args.schema)
    else:
        raise ValueError("Schema must be a .json or .csv file")

    # Build system prompt
    system_prompt = build_system_prompt(db_schema)

    # Store results
    results = []

    for query in natural_language_queries:
        result = get_plan_and_pipeline(system_prompt, query)
        results.append({
            "query": query,
            "result": result
        })

    # Print or save results
    print(json.dumps(results, indent=2))
    
    # Save to file
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    print(f"✅ Results saved to {args.output}")


if __name__ == "__main__":
    main()