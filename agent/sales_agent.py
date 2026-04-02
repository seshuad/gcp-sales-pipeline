from google import genai
from google.genai import types
from google.cloud import bigquery

PROJECT_ID = "project-0a33b36a-f359-40ab-93b"
REGION = "us-central1"

SCHEMA_CONTEXT = """
You have access to the following BigQuery tables:

--- Dataset: sales_raw (raw transactional data, all order statuses) ---

1. sales_raw.orders
   - order_id (STRING): unique order identifier
   - customer_id (STRING): foreign key to customers
   - product_id (STRING): foreign key to products
   - quantity (INTEGER): number of units ordered
   - unit_price (FLOAT): price per unit in USD
   - order_date (DATE): date of order
   - status (STRING): completed, pending, cancelled

2. sales_raw.customers
   - customer_id (STRING): unique customer identifier
   - name (STRING): customer full name
   - email (STRING): customer email
   - region (STRING): North, South, East, West
   - signup_date (DATE): date customer signed up

3. sales_raw.products
   - product_id (STRING): unique product identifier
   - product_name (STRING): Laptop, Monitor, Keyboard, Mouse, Headset, Webcam, Desk Chair, USB Hub
   - unit_price (FLOAT): price in USD
   - category (STRING): Electronics

--- Dataset: sales_processed (pre-aggregated, completed orders only, 2024) ---

4. sales_processed.revenue_by_region
   - region (STRING): North, South, East, West
   - total_orders (INTEGER)
   - total_revenue (FLOAT)
   - avg_order_value (FLOAT)

5. sales_processed.revenue_by_product
   - product_id (STRING)
   - product_name (STRING)
   - category (STRING)
   - units_sold (INTEGER)
   - total_revenue (FLOAT)

6. sales_processed.monthly_trend
   - month (STRING): format YYYY-MM
   - total_orders (INTEGER)
   - total_revenue (FLOAT)

--- Query guidance ---
- Use sales_processed tables for revenue/trend questions (faster, pre-aggregated)
- Use sales_raw tables for order-level, customer-level, or status-based questions
- Always use fully qualified names:
    `project-0a33b36a-f359-40ab-93b.sales_raw.<table>`
    `project-0a33b36a-f359-40ab-93b.sales_processed.<table>`
"""


# ── Tool implementation ──────────────────────────────────────────────────────

def query_bigquery(sql: str) -> dict:
    """Execute SQL against BigQuery and return results."""
    client = bigquery.Client(project=PROJECT_ID)
    print(f"\n  [Tool] SQL: {sql}\n")
    try:
        rows = client.query(sql).result()
        results = [dict(row) for row in rows]
        return {"results": results, "row_count": len(results)}
    except Exception as e:
        return {"error": str(e)}


# ── Agent setup ──────────────────────────────────────────────────────────────

import os
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

query_bigquery_tool = types.Tool(
    function_declarations=[
        types.FunctionDeclaration(
            name="query_bigquery",
            description=(
                "Run a SQL query against the sales BigQuery dataset. "
                "Use this to answer any questions about revenue, orders, products, or trends."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "sql": types.Schema(
                        type=types.Type.STRING,
                        description="The BigQuery StandardSQL query to execute",
                    )
                },
                required=["sql"],
            ),
        )
    ]
)

config = types.GenerateContentConfig(
    system_instruction=f"""You are a helpful sales data analyst assistant.
You answer questions about sales performance using BigQuery data.
{SCHEMA_CONTEXT}
When answering:
- Always call query_bigquery to get real data
- Include specific numbers in your response
- Highlight interesting insights or comparisons
- Keep answers concise and clear
""",
    tools=[query_bigquery_tool],
)


# ── Agent loop ───────────────────────────────────────────────────────────────

def run_agent():
    history = []

    print("=" * 60)
    print("Sales Data Agent  (powered by Gemini)")
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

        history.append(types.Content(role="user", parts=[types.Part(text=user_input)]))

        # Agentic loop — keep going until no more tool calls
        while True:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=history,
                config=config,
            )
            candidate = response.candidates[0].content
            history.append(candidate)

            # Check if model wants to call a tool
            tool_calls = [p for p in candidate.parts if p.function_call]
            if not tool_calls:
                # No tool call — final answer
                text = next((p.text for p in candidate.parts if p.text), "")
                print(f"\nAgent: {text}")
                break

            # Execute all tool calls and feed results back
            tool_results = []
            for part in tool_calls:
                fc = part.function_call
                if fc.name == "query_bigquery":
                    result = query_bigquery(fc.args["sql"])
                else:
                    result = {"error": f"Unknown tool: {fc.name}"}

                tool_results.append(
                    types.Part(
                        function_response=types.FunctionResponse(
                            name=fc.name,
                            response=result,
                        )
                    )
                )

            history.append(types.Content(role="tool", parts=tool_results))


if __name__ == "__main__":
    run_agent()
