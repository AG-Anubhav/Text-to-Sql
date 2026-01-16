import os
import re
import sqlite3
import pandas as pd
from typing import TypedDict, Dict
from dotenv import load_dotenv

from langchain_groq import ChatGroq
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END

# Load Env
load_dotenv()
GROQ_API_KEY="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"

# 1. Setup Database & LLM
db = SQLDatabase.from_uri("sqlite:///sales_data.db")
llm = ChatGroq(
    temperature=0,
    model_name="llama-3.3-70b-versatile",
    groq_api_key=GROQ_API_KEY
)

# 2. State Definition (Added pii_map to store secrets)
class AgentState(TypedDict):
    question: str
    sanitized_question: str
    sql_query: str
    query_result: str
    final_answer: str
    chart_data: dict
    error: str
    retry_count: int
    pii_map: Dict[str, str] # Secret storage for {placeholder: real_value}

# 3. PII Filter: Placeholder Strategy
def pii_filter_input(text):
    """
    Replaces emails with unique placeholders like __EMAIL_1__
    Returns the sanitized text and a dictionary map of secrets.
    """
    email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
    emails = re.findall(email_pattern, text)
   
    pii_map = {}
    sanitized_text = text
   
    for i, email in enumerate(emails):
        placeholder = f"__EMAIL_{i+1}__"
        pii_map[placeholder] = email
        sanitized_text = sanitized_text.replace(email, placeholder)
       
    return sanitized_text, pii_map

def redact_result_data(data_str, pii_map):
    """
    Ensures no real PII leaks into the final context from the database result.
    """
    if not pii_map:
        return data_str
   
    # Replace real values back with generic [REDACTED] tag for the LLM
    for placeholder, real_value in pii_map.items():
        data_str = data_str.replace(real_value, "[EMAIL_REDACTED]")
    return data_str

# 4. Node: SQL Generator
def generate_sql_node(state: AgentState):
    question = state['question']
   
    # 1. Sanitize Input and generate Secret Map
    sanitized, pii_map = pii_filter_input(question)
   
    schema = db.get_table_info()
   
    # We tell the LLM that the database contains the placeholders (Lie to the LLM)
    # This ensures it writes the query using '__EMAIL_1__' instead of guessing
    system_prompt = f"""You are an expert SQL Data Analyst.
    1. Schema: {schema}
    2. IMPORTANT: The user query uses placeholders like '__EMAIL_1__'.
       Treat these as LITERAL strings to search for in the database.
       DO NOT try to remove the underscores.
    3. Generate a valid SQLITE query.
    4. Output ONLY the SQL query.
    """
   
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"User Question: {sanitized}")
    ]
   
    response = llm.invoke(messages)
    sql = response.content.replace("```sql", "").replace("```", "").strip()
   
    return {
        "sql_query": sql,
        "sanitized_question": sanitized,
        "pii_map": pii_map,
        "retry_count": state.get("retry_count", 0)
    }

# 5. Node: SQL Checker
def check_sql_node(state: AgentState):
    sql = state['sql_query']
    schema = db.get_table_info()
   
    if "DROP" in sql.upper() or "DELETE" in sql.upper():
        return {"error": "Unsafe query detected.", "sql_query": ""}
       
    system_prompt = f"""Verify this SQL query for SQLite schema: {schema}
    Query: {sql}
    If correct, output "CORRECT". If incorrect, output ONLY the fixed SQL.
    """
   
    messages = [SystemMessage(content=system_prompt)]
    response = llm.invoke(messages)
    res_text = response.content.strip()
   
    if "CORRECT" in res_text.upper():
        return {"error": None}
   
    if "```sql" in res_text:
        import re
        match = re.search(r"```sql(.*?)```", res_text, re.DOTALL)
        if match:
            return {"sql_query": match.group(1).strip(), "error": None}
           
    return {"sql_query": res_text.replace("```sql", "").replace("```", "").strip(), "error": None}

# 6. Node: Execute SQL (The Magic Swap)
def execute_sql_node(state: AgentState):
    try:
        sql = state['sql_query']
        pii_map = state.get('pii_map', {})
       
        # --- THE MAGIC SWAP ---
        # We temporarily put the REAL email back into the SQL query so SQLite can find it.
        executable_sql = sql
        for placeholder, real_value in pii_map.items():
            executable_sql = executable_sql.replace(placeholder, real_value)
        # ----------------------

        # Execute the query with the real values
        conn = pd.read_sql_query(executable_sql, db._engine)
       
        # --- REDACT RESULTS ---
        # Now we convert the result to string, but we must hide the real email again
        # before showing it to the LLM or storing it in logs.
        result_str = str(conn)
        safe_result_str = redact_result_data(result_str, pii_map)
       
        chart_data = {}
        if not conn.empty and len(conn.columns) >= 2:
            chart_data = {
                "labels": conn.iloc[:, 0].tolist(),
                "values": conn.iloc[:, 1].tolist(),
                "columns": list(conn.columns)
            }
           
        return {"query_result": safe_result_str, "chart_data": chart_data}
    except Exception as e:
        return {"error": str(e), "retry_count": state["retry_count"] + 1}

# 7. Node: Final Answer
def generate_answer_node(state: AgentState):
    if state.get("error"):
        return {"final_answer": f"Error: {state['error']}"}

    # The LLM sees only the sanitized question and the redacted result
    prompt = f"""
    User Question: {state['sanitized_question']}
    SQL Query: {state['sql_query']}
    Data Retrieved: {state['query_result']}
   
    Answer the question.
    If the data shows the email is present, confirm it using '[EMAIL_REDACTED]'.
    If the data is empty, don't say anything about emails or so.
    """
    response = llm.invoke([HumanMessage(content=prompt)])
   
    # Final cleanup just in case LLM hallucinates the placeholder
    final_text = response.content
    for placeholder in state.get('pii_map', {}):
        final_text = final_text.replace(placeholder, "[EMAIL_REDACTED]")
       
    return {"final_answer": final_text}

# 8. Graph Construction
workflow = StateGraph(AgentState)
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("check_sql", check_sql_node)
workflow.add_node("execute_sql", execute_sql_node)
workflow.add_node("generate_answer", generate_answer_node)

workflow.set_entry_point("generate_sql")
workflow.add_edge("generate_sql", "check_sql")
workflow.add_edge("check_sql", "execute_sql")

def should_retry(state):
    if state.get("error") and state["retry_count"] < 3:
        return "generate_sql"
    return "generate_answer"

workflow.add_conditional_edges("execute_sql", should_retry, {"generate_sql": "generate_sql", "generate_answer": "generate_answer"})
workflow.add_edge("generate_answer", END)

app_graph = workflow.compile()