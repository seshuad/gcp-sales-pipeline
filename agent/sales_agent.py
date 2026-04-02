import json
from google.cloud import bigquery
import vertexai
from vertexai.generative_models import (
    FunctionDeclaration,
    GenerativeModel,
    Part,
    Tool,
)

PROJECT_ID = "project-0a33b36a-f359-40ab-93b"
REGION = "us-central1"
BQ_DATASET = "sales_processed"

# Available tables the agent knows about
SCHEMA_CONTEXT = """
You have access to the following BigQuery tables in the sales_processed dataset:

1. revenue_by_region
   - region (STRING): North, South, East, West
   - total_orders (INTEGER): number of completed orders
   - total_revenue (FLOAT): total revenue in USD
   - avg_order_value (FLOAT): average order value in USD

2. revenue_by_product
   - product_id (STRING)
   - product_name (STRING): Laptop, Monitor, Keyboard, Mouse, Headset, Webcam, Desk Chair, USB Hub
   - category (STRING): Electronics
   - units_sold (INTEGER)
   - total_revenue (FLOAT)

3. monthly_trend
   - month (STRING): format YYYY-MM
   - total_orders (INTEGER)
   - total_revenue (FLOAT)

Data covers completed orders only for year 2024.
Always use fully qualified table names: `project-0a33b36a-f359-40ab-93b.sales_processed.<table>`
"""

# ── Tool definition ──────────────────────────────────────────────────────────

query_bigquery_func = FunctionDeclaration(
    name="query_bigquery",
    description="Run a SQL query against the sales BigQuery dataset and return results. Use this to answer any questions about sales revenue, orders, products, or trends.",
    parameters={
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The BigQuery SQL query to execute",
            }
        },
        "required": ["sql"],
    },
)

sales_tool = Tool(function_declarations=[query_bigquery_func])


# ── Tool implementation ──────────────────────────────────────────────────────

def query_bigquery(sql: str) -> dict:
    """Execute SQL against BigQuery and return results as a list of dicts."""
    client = bigquery.Client(project=PROJECT_ID)
    try:
        print(f"\n  [Tool] Running SQL:\n  {sql}\n")
        rows = client.query(sql).result()
        results = [dict(row) for row in rows]
        return {"results": results, "row_count": len(results)}
    except Exception as e:
        return {"error": str(e)}


# ── Agent loop ───────────────────────────────────────────────────────────────

def run_agent():
    vertexai.init(project=PROJECT_ID, location=REGION)

    model = GenerativeModel(
        model_name="gemini-1.5-pro",
        system_instruction=f"""You are a helpful sales data analyst assistant.
You have access to sales pipeline data in BigQuery.
{SCHEMA_CONTEXT}
When asked a question:
1. Write a SQL query to get the data
2. Call the query_bigquery tool
3. Interpret the results and answer in plain English
4. Include specific numbers in your response
5. If relevant, highlight interesting insights or comparisons
""",
        tools=[sales_tool],
    )

    chat = model.start_chat()
    print("=" * 60)
    print("Sales Data Agent")
    print("Ask me anything about your sales data.")
    print("Type 'exit' to quit.")
    print("=" * 60)

    while True:
        user_input = input("\nYou: ").strip()
        if user_input.lower() in ("exit", "quit"):
            print("Goodbye!")
            break
        if not user_input:
            continue

        response = chat.send_message(user_input)

        # Agentic loop — keep going until no more tool calls
        while response.candidates[0].content.parts[0].function_call.name:
            function_call = response.candidates[0].content.parts[0].function_call
            tool_name = function_call.name
            tool_args = dict(function_call.args)

            if tool_name == "query_bigquery":
                tool_result = query_bigquery(tool_args["sql"])
            else:
                tool_result = {"error": f"Unknown tool: {tool_name}"}

            # Send tool result back to model
            response = chat.send_message(
                Part.from_function_response(
                    name=tool_name,
                    response=tool_result,
                )
            )

        print(f"\nAgent: {response.text}")


if __name__ == "__main__":
    run_agent()
