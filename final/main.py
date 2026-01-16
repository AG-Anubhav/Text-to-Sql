import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from backend import app_graph

# Page Configuration
st.set_page_config(page_title="AI SQL Agent", layout="wide")

# --- DATABASE & SCHEMA FUNCTIONS ---
def get_schema_diagram():
    """Generates a Graphviz diagram of the database schema with relationships"""
    conn = sqlite3.connect("sales_data.db")
    cursor = conn.cursor()
   
    # Graphviz DOT format definition
    dot = 'digraph Database {\n  rankdir=LR;\n  node [shape=plaintext];\n  bgcolor="transparent";\n\n'
   
    # Get all tables
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = [t[0] for t in tables]
   
    relationships = ""
   
    for table in tables:
        # Fetch Columns to build the table node
        columns = cursor.execute(f"PRAGMA table_info({table})").fetchall()
        # Format: "ColumnName (PK)"
        col_names = [f"{col[1]} {'(PK)' if col[5] else ''}" for col in columns]
       
        # HTML-like label for the table node
        col_html = "".join([f'<tr><td align="left" port="{c.split()[0]}">{c}</td></tr>' for c in col_names])
        dot += f'  {table} [label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="4">\n'
        dot += f'    <tr><td bgcolor="lightblue"><b>{table.upper()}</b></td></tr>\n'
        dot += f'{col_html}  </table>>];\n\n'
       
        # Fetch Foreign Keys to draw arrows
        fks = cursor.execute(f"PRAGMA foreign_key_list({table})").fetchall()
        for fk in fks:
            # fk[2] = target table, fk[3] = source col, fk[4] = target col
            relationships += f'  {table}:{fk[3]} -> {fk[2]}:{fk[4]} [dir=forward];\n'

    dot += relationships + "}"
    conn.close()
    return dot

def get_table_data(table_name):
    """Fetches all rows from a specific table"""
    conn = sqlite3.connect("sales_data.db")
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        return df
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})
    finally:
        conn.close()

def get_all_table_names():
    """Gets a list of all table names"""
    conn = sqlite3.connect("sales_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables
# -----------------------------------

# --- SIDEBAR UI ---
with st.sidebar:
    st.title("🎛️ Control Panel")
   
    st.markdown("### 🗄️ Database Explorer")
   
    # Tabs for Data vs Schema
    tab_view, tab_schema = st.tabs(["📄 Data", "🕸️ Schema"])
   
    with tab_view:
        table_options = get_all_table_names()
        selected_table = st.selectbox("Select Table:", table_options)
        if selected_table:
            st.caption(f"Showing top 50 rows of **{selected_table}**")
            df_preview = get_table_data(selected_table)
            st.dataframe(df_preview.head(50), hide_index=True)
       
    with tab_schema:
        st.caption("Entity Relationship Diagram")
        try:
            graph = get_schema_diagram()
            st.graphviz_chart(graph)
        except Exception as e:
            st.error(f"Could not load schema: {e}")

    st.divider()
   
    st.markdown("### ⚙️ Settings")
    pii_enabled = st.toggle("Enable PII Filtering", value=True)
    show_sql = st.toggle("Show Generated SQL", value=True)
   
# --- MAIN CHAT UI ---
st.title("📊 Enterprise Text-to-SQL Agent")
st.markdown("Ask questions about your sales data (2023-2024). *Example: 'What is the total sales for Laptop Pro?'*")

# Initialize Chat History
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display Chat History
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        # If there is chart data attached to this message, display it
        if "chart" in message and message["chart"]:
            chart_dict = message["chart"]
            st.bar_chart(pd.DataFrame(chart_dict["values"], index=chart_dict["labels"]))

# Input Field
if prompt := st.chat_input("Ask a question..."):
    # 1. Display User Message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 2. Process with AI Agent
    with st.spinner("Agent is thinking..."):
        try:
            inputs = {"question": prompt, "retry_count": 0}
            result = app_graph.invoke(inputs)
           
            answer = result.get('final_answer', "I couldn't generate an answer.")
            sql_used = result.get('sql_query', 'N/A')
            chart_data = result.get('chart_data', None)

            # Construct Response Text
            response_content = answer
            if show_sql:
                response_content += f"\n\n**SQL Executed:**\n```sql\n{sql_used}\n```"
           
            # 3. Display Assistant Response
            st.session_state.messages.append({
                "role": "assistant",
                "content": response_content,
                "chart": chart_data
            })
           
            with st.chat_message("assistant"):
                st.markdown(response_content)
               
                # Check if we have data to plot
                if chart_data and len(chart_data.get('values', [])) > 0:
                    st.subheader("Visual Analysis")
                    tab_chart, tab_raw = st.tabs(["📊 Bar Chart", "🔢 Raw Data"])
                   
                    df_chart = pd.DataFrame({
                        chart_data['columns'][0]: chart_data['labels'],
                        chart_data['columns'][1]: chart_data['values']
                    }).set_index(chart_data['columns'][0])
                   
                    with tab_chart:
                        st.bar_chart(df_chart)
                    with tab_raw:
                        st.dataframe(df_chart)

        except Exception as e:
            st.error(f"An error occurred: {e}")