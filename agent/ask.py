""""Ask your shop": question -> schemas.AskPlan -> one parameterised query (phase 5.2).

TODO:
- prompt Gemini with the QueryName catalogue and the question; parse the reply into AskPlan
- map each QueryName to ONE fixed SQL statement with bind parameters; the model never writes SQL
- return {"answer": str, "rows": [...], "chart": "bar" | "line" | "table"}
"""
