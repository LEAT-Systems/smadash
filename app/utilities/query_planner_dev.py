"""
Query Planner and Generator Script

Features:
- Loads schema (JSON or CSV)
- Builds system prompt
- Accepts natural language query as input
- Returns structured query plan + SQL using OpenAI
"""

import os
import json
import argparse
import pandas as pd
from collections import defaultdict
from pprint import pprint
from dotenv import load_dotenv
from openai import OpenAI

# Access the API key
load_dotenv()
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))


def load_schema(path="db_schema_moc.json") -> str:
    """Read the database schema JSON file as a raw string."""
    with open(path, "r") as schema:
        return schema.read()
    


def load_schema_csv(path: str = "pg_schema.csv") -> str:
    """Optional: Load database schema from CSV and format as string."""
    df = pd.read_csv(path)

    # Organize by table
    schema_dict = defaultdict(list)
    for _, row in df.iterrows():
        schema_dict[row["table_name"]].append((row["column_name"], row["data_type"]))

    # Format as plain text
    formatted_schema = []
    for table, columns in schema_dict.items():
        formatted_schema.append(f"Table: {table}")
        for col, dtype in columns:
            formatted_schema.append(f"- {col} ({dtype})")
        formatted_schema.append("")

    # Join into final string for LLM
    return "\n".join(formatted_schema)



def build_system_prompt(db_schema: str, dialect: str = "SQLite3") -> str:
    """Build the system prompt for query planning."""

    return f"""
You are a query planner assistant and builder. Your job is to:
1. Convert a user's natural language request into a structured query plan (JSON)
2. Generate a valid SQL SELECT statement based on that plan

Use the following database schema:
{db_schema}

You must support the following operators:
- "=" for exact match (e.g., field = 'value')
- "between" for range filters (e.g., dates or numeric ranges)
- "like" for partial matches (e.g., names or categories)
- ">" and "<" for comparisons


Additional instructions:
- The SQL must be valid for {dialect}. Use {dialect}-specific syntax, data types, and functions where appropriate.
- If using the "between" operator, the value must be a 2-element array: [start, end]
- If using the "like" operator, wrap the value in % signs for partial match (e.g., '%phone%')
- Always include a descriptive attribute like a name for primary and foreign keys, JOIN a different table if necessary
- Output must be valid JSON only
- Use only tables and fields from the schema above
- Output the SQL statement as one statement


Use this JSON format:
{{
  "query_plan": {{
    "intent": "ranking" | "trend" | "filter" | "comparison",
    "table": "sales_data",
    "filters": [ {{ "field": ..., "operator": ..., "value": ... }} ],
    "metrics": [ {{ "name": ..., "aggregation": ..., "alias": ... }} ],
    "group_by": [...],
    "sort": [ {{ "field": ..., "order": "asc" | "desc" }} ],
    "limit": ...,
    "original_user_query": ...
  }},
  "sql": "SELECT ... FROM ... JOIN ..."
}}


Only return valid JSON output.
"""


def get_plan_and_sql(system_prompt: str, user_query: str, model: str = "gpt-4o-mini") -> dict:
    """Call OpenAI to generate query plan + SQL."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Missing OPENAI_API_KEY env variable")
    client = OpenAI(api_key=api_key)
    
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
    parser = argparse.ArgumentParser(description="Query Planner CLI")
    parser.add_argument("query", type=str, nargs="?", help="Natural language query to convert into SQL")
    parser.add_argument("--schema", type=str, default="db_schema_moc.json",
                        help="Path to schema file (JSON or CSV)")
    parser.add_argument("--dialect", type=str, default="SQLite3", help="SQL dialect")
    args = parser.parse_args()


    # If no query passed, ask interactively
    user_query = args.query or input("Enter your natural language query: ")


    # Load schema
    if args.schema.endswith(".json"):
        db_schema = load_schema(args.schema)
    elif args.schema.endswith(".csv"):
        db_schema = load_schema_csv(args.schema)
    else:
        raise ValueError("Schema must be a .json or .csv file")
    
    #Build system prompt
    system_prompt = build_system_prompt(db_schema, args.dialect)

    # Run query planner
    result = get_plan_and_sql(system_prompt, user_query)
    print(result)


if __name__ == "__main__":
    main()



