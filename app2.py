# app.py
import os
import json
import pandas as pd
import streamlit as st
import requests
from databricks import sql
from io import BytesIO
from datetime import datetime

# ---------- Config ----------
def get_secret(name: str) -> str:
    return st.secrets.get(name) or os.getenv(name) or ""

DATABRICKS_SERVER_HOST = get_secret("DATABRICKS_SERVER_HOST")
DATABRICKS_HTTP_PATH = get_secret("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = get_secret("DATABRICKS_ACCESS_TOKEN")
GROQ_API_KEY = get_secret("GROQ_API_KEY")
WORKSPACE_NAME = get_secret("WORKSPACE_NAME") or None

MODEL_NAME = "llama-3.3-70b-versatile"
SCHEMA_JSON_DIR = "schemas"
os.makedirs(SCHEMA_JSON_DIR, exist_ok=True)

# --- ⚠️ EDIT THIS VALUE ---
# You MUST still set the report focus
HARDCODED_REPORT_FOCUS = "Sales Pacing Report" # e.g., "Weekly Sales Trends"
# --- END OF VALUES TO EDIT ---


# ---------- Low-level query helper (cursor usage) ----------
def run_query_raw(query: str, use_catalog: str = None, use_schema: str = None, timeout: int = 120):
    """Execute query and return (rows, cols). Uses cursor.execute on databricks-sql connector."""
    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cur:
            if use_catalog:
                try: cur.execute(f"USE CATALOG {use_catalog}")
                except Exception: pass
            if use_schema:
                try: cur.execute(f"USE SCHEMA {use_catalog}.{use_schema}" if use_catalog else f"USE SCHEMA {use_schema}")
                except Exception:
                    try:
                        if use_catalog: cur.execute(f"USE {use_catalog}.{use_schema}")
                        else: cur.execute(f"USE {use_schema}")
                    except Exception: pass
            cur.execute(query)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
    return rows, cols

# ---------- Helpers to detect active workspace ----------
def fetch_current_catalog():
    """Return current_catalog() for the SQL session if available."""
    try:
        rows, cols = run_query_raw("SELECT current_catalog()")
        if rows and rows[0] and rows[0][0]:
            return str(rows[0][0])
    except Exception: pass
    try:
        rows, cols = run_query_raw("SELECT current_schema()")
        if rows and rows[0] and rows[0][0]:
            return str(rows[0][0])
    except Exception: pass
    return None

# ---------- Cached listing helpers ----------
@st.cache_data(ttl=300)
def cached_list_databases(workspace: str):
    try:
        rows, cols = run_query_raw(f"SHOW SCHEMAS IN CATALOG {workspace}", use_catalog=workspace)
    except Exception:
        rows, cols = run_query_raw("SHOW SCHEMAS", use_catalog=workspace)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

@st.cache_data(ttl=300)
def cached_list_table_names(workspace: str, database: str):
    try:
        rows, cols = run_query_raw(f"SHOW TABLES IN {workspace}.{database}", use_catalog=workspace, use_schema=database)
    except Exception:
        rows, cols = run_query_raw(f"SHOW TABLES IN {database}", use_catalog=workspace, use_schema=database)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=cols)
    possible = [c for c in df.columns if c.lower() in ("table_name", "tablename", "name", "table")]
    name_col = possible[0] if possible else df.columns[0]
    return df[[name_col]].rename(columns={name_col: "table_name"})

@st.cache_data(ttl=600)
def cached_describe_table(workspace: str, database: str, table: str):
    try:
        rows, cols = run_query_raw(f"DESCRIBE TABLE {workspace}.{database}.{table}", use_catalog=workspace, use_schema=database)
    except Exception:
        rows, cols = run_query_raw(f"DESCRIBE {database}.{table}", use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

# ---------- Utilities ----------
def sample_table_rows(
    workspace: str, 
    database: str, 
    table: str, 
    limit: int = 500000, 
    date_col: str = None, 
    start_dt: datetime.date = None, 
    end_dt: datetime.date = None
) -> pd.DataFrame:
    where_clauses = []
    if date_col and (start_dt or end_dt):
        try:
            if start_dt:
                where_clauses.append(f"{date_col} >= '{start_dt.strftime('%Y-%m-%d')}'")
            if end_dt:
                where_clauses.append(f"{date_col} <= '{end_dt.strftime('%Y-%m-%d')}'")
        except Exception as e:
            st.warning(f"Could not apply date filter (is column name correct?): {e}")

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)
        
    q = f"SELECT * FROM {workspace}.{database}.{table}{where_sql} LIMIT {limit}"
    
    rows, cols = run_query_raw(q, use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

def describe_to_column_meta(describe_df: pd.DataFrame):
    if describe_df.empty:
        return []
    name_col = next((c for c in describe_df.columns if c.lower() in ("col_name", "column", "name")), None)
    type_col = next((c for c in describe_df.columns if c.lower() in ("data_type", "type")), None)
    comment_col = next((c for c in describe_df.columns if c.lower() == "comment"), None)
    cols = []
    for _, r in describe_df.iterrows():
        cname = r.get(name_col) if name_col else None
        ctype = r.get(type_col) if type_col else None
        ccomment = r.get(comment_col) if comment_col else None
        if cname and ctype:
            cols.append({"name": cname, "type": ctype, "comment": ccomment})
    return cols

def to_excel(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data = output.getvalue()
    return processed_data

def get_llm_date_column(cols_meta: list) -> str:
    """
    Calls the LLM to identify the most likely primary date column from a schema.
    """
    if not cols_meta:
        return None

    schema_block = json.dumps({"columns": cols_meta}, indent=2)
    
    prompt = (
        f"Given the following table schema:\n{schema_block}\n\n"
        "Which column *best* represents the primary date or timestamp for business analysis (e.g., transaction_date, order_date, event_date)? "
        "Return *only* the single, exact column name and nothing else. If no clear date column exists, return 'None'."
    )
    
    system = "You are a schema-parsing assistant. You return only a single column name or the word 'None'."
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL_NAME,
        "temperature": 0.0,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]
    }
    
    try:
        resp = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        col_name = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        
        col_name = col_name.replace('"', '').replace("`", "").replace(".", "")
        
        if col_name.lower() == 'none' or col_name == "":
            return None
        
        all_col_names = [c['name'] for c in cols_meta]
        if col_name in all_col_names:
            return col_name
        else:
            st.warning(f"LLM suggested date column '{col_name}' which was not found in the schema.")
            return None
            
    except Exception as e:
        st.error(f"Failed to ask LLM for date column: {e}")
        return None


# ---------- UI and behavior ----------
st.set_page_config(page_title="Reporting Agent", layout="wide")
st.title("Reporting Agent")

# Validate configuration
missing = [k for k,v in {
    "DATABRICKS_SERVER_HOST": DATABRICKS_SERVER_HOST,
    "DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH,
    "DATABRICKS_ACCESS_TOKEN": DATABRICKS_TOKEN,
    "GROQ_API_KEY": GROQ_API_KEY,
}.items() if not v]
if missing:
    st.error(f"Missing configuration: {', '.join(missing)}")
    st.stop()

# Session state initialization
if "workspace" not in st.session_state:
    st.session_state.workspace = WORKSPACE_NAME
if "databases" not in st.session_state:
    st.session_state.databases = []
if "tables" not in st.session_state:
    st.session_state.tables = []
if "selected_database" not in st.session_state:
    st.session_state.selected_database = None
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None
if "last_report" not in st.session_state:
    st.session_state.last_report = ""
if "last_report_df" not in st.session_state:
    st.session_state.last_report_df = pd.DataFrame()
if "start_date_filter" not in st.session_state:
    st.session_state.start_date_filter = None
if "end_date_filter" not in st.session_state:
    st.session_state.end_date_filter = None
# --- ADDED SESSION STATE FOR DATE COLUMN ---
if "date_column_name" not in st.session_state:
    st.session_state.date_column_name = ""


st.sidebar.header("Controls")

# --- NEW: Function to auto-detect date column when table changes ---
def on_table_change():
    ws = st.session_state.workspace
    db = st.session_state.selected_database
    tbl = st.session_state.selected_table
    
    if not (ws and db and tbl):
        st.session_state.date_column_name = ""
        return

    try:
        # Get schema and ask LLM for date col
        describe_df = cached_describe_table(ws, db, tbl)
        cols_meta = describe_to_column_meta(describe_df)
        if cols_meta:
            with st.spinner("AI is guessing the date column..."):
                ai_guessed_col = get_llm_date_column(cols_meta)
                st.session_state.date_column_name = ai_guessed_col or ""
        else:
            st.session_state.date_column_name = ""
    except Exception as e:
        st.warning(f"Could not auto-detect date column: {e}")
        st.session_state.date_column_name = ""
# --- END NEW FUNCTION ---

# Refresh workspace
def refresh_workspace():
    cached_list_databases.clear()
    cached_list_table_names.clear()
    cached_describe_table.clear()

    if WORKSPACE_NAME:
        workspace = WORKSPACE_NAME
    else:
        workspace = fetch_current_catalog()
        if not workspace:
            st.error("Unable to determine current catalog. Set WORKSPACE_NAME.")
            st.session_state.workspace = None
            st.session_state.databases = []
            st.session_state.tables = []
            return

    st.session_state.workspace = workspace

    try:
        dbs_df = cached_list_databases(workspace)
        if dbs_df.empty:
            st.session_state.databases = []
            st.session_state.selected_database = None
            st.session_state.tables = []
            st.session_state.selected_table = None
            return
        db_col = dbs_df.columns[0]
        st.session_state.databases = dbs_df[db_col].astype(str).tolist()
        st.session_state.selected_database = st.session_state.databases[0] if st.session_state.databases else None

        if st.session_state.selected_database:
            tdf = cached_list_table_names(workspace, st.session_state.selected_database)
            if tdf.empty:
                st.session_state.tables = []
                st.session_state.selected_table = None
            else:
                st.session_state.tables = tdf["table_name"].astype(str).tolist()
                st.session_state.selected_table = st.session_state.tables[0] if st.session_state.tables else None
            
            # --- ADDED CALL ---
            # When workspace refreshes, auto-select first DB/table and guess date col
            if st.session_state.selected_table:
                on_table_change() 
            else:
                st.session_state.date_column_name = ""
            # --- END ADDED CALL ---

    except Exception as e:
        st.error(f"Failed to refresh workspace metadata: {e}")
        st.session_state.databases = []
        st.session_state.tables = []
        st.session_state.date_column_name = ""

# Sidebar UI
if st.sidebar.button("Refresh workspace"):
    refresh_workspace()

st.sidebar.markdown("### Workspace (warehouse)")
st.sidebar.write(st.session_state.workspace or "— (click Refresh workspace)")

# Database dropdown
if st.session_state.databases:
    def on_db_change():
        ws = st.session_state.workspace
        db = st.session_state.selected_database
        st.session_state.selected_table = None
        st.session_state.tables = []
        if not (ws and db):
            return
        try:
            tdf = cached_list_table_names(ws, db)
            if tdf.empty:
                st.session_state.tables = []
                st.session_state.selected_table = None
            else:
                st.session_state.tables = tdf["table_name"].astype(str).tolist()
                st.session_state.selected_table = st.session_state.tables[0] if st.session_state.tables else None
            
            # --- ADDED CALL ---
            # When DB changes, auto-select first table and guess date col
            if st.session_state.selected_table:
                on_table_change()
            else:
                st.session_state.date_column_name = ""
            # --- END ADDED CALL ---

        except Exception as e:
            st.error(f"Failed to list table names for {ws}.{db}: {e}")
            st.session_state.tables = []

    st.sidebar.selectbox(
        "Database",
        options=st.session_state.databases,
        key="selected_database",
        on_change=on_db_change
    )
else:
    st.sidebar.info("Databases will appear here after clicking Refresh workspace")

# Table dropdown
if st.session_state.tables:
    # --- MODIFIED: Added on_change to the table selectbox ---
    st.sidebar.selectbox(
        "Table", 
        options=st.session_state.tables, 
        key="selected_table",
        on_change=on_table_change # <-- When user picks a new table, re-guess date col
    )
else:
    st.sidebar.info("Tables will appear here after selecting a Database")

# ---------- Main area ----------
st.header("Selected source")
st.markdown(
    f"**Workspace (warehouse)**: {st.session_state.workspace or '—'}  \n"
    f"**Database**: {st.session_state.selected_database or '—'}  \n"
    f"**Table**: {st.session_state.selected_table or '—'}"
)

st.subheader("Generate Report")

# --- MODIFIED: Added text input for date col, pre-filled by session state ---
st.info("The AI will *try* to guess the main date column. Please verify or enter the correct column name for filtering.")
c1, c2, c3 = st.columns(3)
with c1:
    st.text_input("Date Column for Filtering", key="date_column_name")
with c2:
    st.date_input("Start Date", value=None, key="start_date_filter")
with c3:
    st.date_input("End Date", value=None, key="end_date_filter")
# --- END MODIFICATION ---


if st.button("Generate report", type="primary", use_container_width=True):
    ws = st.session_state.workspace
    db = st.session_state.selected_database
    tbl = st.session_state.selected_table
    
    report_focus = HARDCODED_REPORT_FOCUS

    # --- READ VALUES FROM UI / SESSION STATE ---
    start_dt = st.session_state.start_date_filter
    end_dt = st.session_state.end_date_filter
    date_col = st.session_state.date_column_name # <-- Reads from the text box
    # --- END READ ---

    if not (ws and db and tbl):
        st.error("Click Refresh workspace then select a Database and Table.")
    elif not (start_dt and end_dt):
        st.error("Please select both a Start Date and an End Date.")
    elif not date_col: # <-- Check if date col is empty
        st.error("Please provide a Date Column for filtering. The AI may not have been able to guess one.")
    else:
        try:
            # --- MODIFIED: Removed Phase 1, simplified spinner ---
            with st.spinner(f"Generating report... (using '{date_col}' for date filtering)"):
                
                # Fetch schema for the report prompt (already cached)
                describe_df = cached_describe_table(ws, db, tbl)
                cols_meta = describe_to_column_meta(describe_df)
                if not cols_meta:
                    st.error("Could not fetch table schema.")
                    st.stop()
                
                # Sample data using the date_col from the text box
                df = sample_table_rows(
                    ws, db, tbl, 
                    limit=2000, 
                    date_col=date_col, 
                    start_dt=start_dt, 
                    end_dt=end_dt
                )
                
                st.session_state.last_report_df = df
                st.session_state.last_report = ""

                meta = {
                    "workspace": ws, 
                    "database": db, 
                    "table": tbl, 
                    "row_sample_count": len(df),
                    "filter_date_column": date_col, # From text box
                    "filter_start_date": str(start_dt),
                    "filter_end_date": str(end_dt)
                }

                schema_block = json.dumps({"columns": cols_meta}, indent=2)
                preview = df.head(20).to_markdown(index=False) if not df.empty else "No rows available"
                try:
                    stats = df.describe(include="all").to_markdown() if not df.empty else "No stats available"
                except Exception:
                    stats = "Stats unavailable"
                
                # --- This is the enhanced prompt for better insights ---
                prompt = (
                    f"Workspace context:\n{json.dumps(meta, indent=2)}\n\n"
                    f"Table schema:\n{schema_block}\n\n"
                    f"Sample preview (top 20 rows):\n{preview}\n\n"
                    f"Descriptive stats:\n{stats}\n\n"
                    f"Task:\nGenerate a deep, quantitative analysis focused on: **{report_focus}**\n"
                    "- **Prioritize hard numbers:** Use the provided data to calculate key metrics, totals, averages, and percentage changes.\n"
                    "- **Identify meaningful insights:** Go beyond simple observations. Explain *why* trends are happening and what their business impact is. Look for correlations or anomalies.\n"
                    "- **Structure the report:** Start with a high-level **Executive Summary** (with 3-4 key numbers). Then, create sections for **Detailed Analysis & Insights**, **Key Trends**, and **Potential Data Quality Issues**.\n"
                    "- **Be quantitative:** All insights must be supported by specific numbers, metrics, or statistics from the data.\n"
                    "- Base all analysis *only* on the schema and data sample provided for the specified date range.\n"
                    "- Suggest next analyses or metrics that would add more value.\n"
                    "- Use clear section headings (like 'Executive Summary') and bullet points.\n"
                    "- Avoid reprinting raw data.\n"
                )

                # --- This is the enhanced system message ---
                system = (
                    "You are a principal data scientist. Your audience is a business executive. "
                    "They need a deep, quantitative report, not a simple description. "
                    "Focus on metrics, financial insights, and the 'so what' of the data. "
    
                    "All analysis must be for the specified date range."
                )
                
                headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
                payload = {"model": MODEL_NAME, "temperature": 0.1, "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]}
                resp = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=120)
                resp.raise_for_status()
                data = resp.json()
                report = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                
                if report:
                    st.session_state.last_report = report
                else:
                    st.error("Groq returned an empty report.")
        except Exception as e:
            st.error(f"Report generation failed: {e}")
            st.session_state.last_report = ""
            st.session_state.last_report_df = pd.DataFrame()

# --- Report and Export Area ---
if st.session_state.last_report:
    st.divider()
    st.subheader("Generated Report")
    st.markdown(st.session_state.last_report)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.download_button(
            label="📥 Download Report as Text (.txt)",
            data=st.session_state.last_report.encode("utf-8"),
            file_name=f"{st.session_state.workspace}_{st.session_state.selected_database}_{st.session_state.selected_table}_report.txt",
            mime="text/plain",
        )
        
    with col2:
        if not st.session_state.last_report_df.empty:
            excel_data = to_excel(st.session_state.last_report_df)
            st.download_button(
                label="📥 Download Data Sample as Excel (.xlsx)",
                data=excel_data,
                file_name=f"{st.session_state.workspace}_{st.session_state.selected_database}_{st.session_state.selected_table}_sample.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

st.caption("Refresh workspace -> shows Database dropdown and Table names only. DESCRIBE is executed per-table on demand.")