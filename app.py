# app.py
import os
import json
import pandas as pd
import streamlit as st
import requests
from databricks import sql

# ---------- Config ----------
def get_secret(name: str) -> str:
    return st.secrets.get(name) or os.getenv(name) or ""

DATABRICKS_SERVER_HOST = get_secret("DATABRICKS_SERVER_HOST")
DATABRICKS_HTTP_PATH = get_secret("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = get_secret("DATABRICKS_ACCESS_TOKEN")
GROQ_API_KEY = get_secret("GROQ_API_KEY")
WORKSPACE_NAME = get_secret("WORKSPACE_NAME") or None  # optional override

MODEL_NAME = "llama-3.3-70b-versatile"
SCHEMA_JSON_DIR = "schemas"
os.makedirs(SCHEMA_JSON_DIR, exist_ok=True)

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
                try:
                    cur.execute(f"USE CATALOG {use_catalog}")
                except Exception:
                    pass
            if use_schema:
                try:
                    cur.execute(f"USE SCHEMA {use_catalog}.{use_schema}" if use_catalog else f"USE SCHEMA {use_schema}")
                except Exception:
                    try:
                        if use_catalog:
                            cur.execute(f"USE {use_catalog}.{use_schema}")
                        else:
                            cur.execute(f"USE {use_schema}")
                    except Exception:
                        pass
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
    except Exception:
        pass
    try:
        rows, cols = run_query_raw("SELECT current_schema()")
        if rows and rows[0] and rows[0][0]:
            return str(rows[0][0])
    except Exception:
        pass
    return None

# ---------- Cached listing helpers ----------
@st.cache_data(ttl=300)
def cached_list_databases(workspace: str):
    """Return DataFrame of databases (schemas) inside the workspace/catalog."""
    try:
        rows, cols = run_query_raw(f"SHOW SCHEMAS IN CATALOG {workspace}", use_catalog=workspace)
    except Exception:
        rows, cols = run_query_raw("SHOW SCHEMAS", use_catalog=workspace)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

@st.cache_data(ttl=300)
def cached_list_table_names(workspace: str, database: str):
    """Return a DataFrame with a single column 'table_name' holding table names (fast)."""
    try:
        rows, cols = run_query_raw(f"SHOW TABLES IN {workspace}.{database}", use_catalog=workspace, use_schema=database)
    except Exception:
        rows, cols = run_query_raw(f"SHOW TABLES IN {database}", use_catalog=workspace, use_schema=database)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=cols)
    # Heuristic: find likely column holding the table name
    possible = [c for c in df.columns if c.lower() in ("table_name", "tablename", "name", "table")]
    name_col = possible[0] if possible else df.columns[0]
    return df[[name_col]].rename(columns={name_col: "table_name"})

@st.cache_data(ttl=600)
def cached_describe_table(workspace: str, database: str, table: str):
    """DESCRIBE a single table and cache result per (workspace, database, table)."""
    try:
        rows, cols = run_query_raw(f"DESCRIBE TABLE {workspace}.{database}.{table}", use_catalog=workspace, use_schema=database)
    except Exception:
        rows, cols = run_query_raw(f"DESCRIBE {database}.{table}", use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

# ---------- Utilities ----------
def save_schema_json(name: str, payload: dict) -> str:
    path = os.path.join(SCHEMA_JSON_DIR, f"{name}_schema.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return path

def sample_table_rows(workspace: str, database: str, table: str, limit: int = 5000) -> pd.DataFrame:
    q = f"SELECT * FROM {workspace}.{database}.{table} LIMIT {limit}"
    rows, cols = run_query_raw(q, use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

def describe_to_column_meta(describe_df: pd.DataFrame):
    """Normalize DESCRIBE output to a list of {name,type,comment}."""
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

st.sidebar.header("Controls")

# Refresh workspace: only populate DB names and table NAMES for the first DB
def refresh_workspace():
    # Clear cached listing functions so we fetch fresh data
    cached_list_databases.clear()
    cached_list_table_names.clear()
    cached_describe_table.clear()

    # Determine workspace to inspect
    if WORKSPACE_NAME:
        workspace = WORKSPACE_NAME
    else:
        workspace = fetch_current_catalog()
        if not workspace:
            st.error("Unable to determine current catalog for this SQL session. Set WORKSPACE_NAME or ensure current_catalog() is available.")
            st.session_state.workspace = None
            st.session_state.databases = []
            st.session_state.tables = []
            st.session_state.selected_database = None
            st.session_state.selected_table = None
            return

    st.session_state.workspace = workspace

    # List databases (schemas)
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
        # auto-select first database (change if you want manual selection)
        st.session_state.selected_database = st.session_state.databases[0] if st.session_state.databases else None

        # List table NAMES for selected_database (fast)
        if st.session_state.selected_database:
            tdf = cached_list_table_names(workspace, st.session_state.selected_database)
            if tdf.empty:
                st.session_state.tables = []
                st.session_state.selected_table = None
            else:
                st.session_state.tables = tdf["table_name"].astype(str).tolist()
                st.session_state.selected_table = st.session_state.tables[0] if st.session_state.tables else None
    except Exception as e:
        st.error(f"Failed to refresh workspace metadata: {e}")
        st.session_state.databases = []
        st.session_state.tables = []
        st.session_state.selected_database = None
        st.session_state.selected_table = None

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
        except Exception as e:
            st.error(f"Failed to list table names for {ws}.{db}: {e}")
            st.session_state.tables = []
            st.session_state.selected_table = None

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
    st.sidebar.selectbox("Table", options=st.session_state.tables, key="selected_table")
else:
    st.sidebar.info("Tables will appear here after selecting a Database")

# ---------- Main area ----------
st.header("Selected source")
st.markdown(
    f"**Workspace (warehouse)**: {st.session_state.workspace or '—'}  \n"
    f"**Database**: {st.session_state.selected_database or '—'}  \n"
    f"**Table**: {st.session_state.selected_table or '—'}"
)

st.subheader("Table sample, schema and report")
limit = st.number_input("Sample size (rows)", min_value=100, max_value=50000, value=5000, step=1000)
task_hint = st.text_input("Optional: report focus (e.g., 'sales trends by region, anomaly detection')")

col_preview, col_actions = st.columns([2, 1])

with col_preview:
    if st.button("Preview sample"):
        ws = st.session_state.workspace
        db = st.session_state.selected_database
        tbl = st.session_state.selected_table
        if not (ws and db and tbl):
            st.error("Click Refresh workspace then select a Database and Table.")
        else:
            try:
                df = sample_table_rows(ws, db, tbl, limit=limit)
                st.write(f"Sample of {ws}.{db}.{tbl} (showing up to {limit} rows)")
                st.dataframe(df.head(2000), use_container_width=True)
            except Exception as e:
                st.error(f"Failed to fetch sample: {e}")

with col_actions:
    if st.button("Fetch table schema (DESCRIBE)"):
        ws = st.session_state.workspace
        db = st.session_state.selected_database
        tbl = st.session_state.selected_table
        if not (ws and db and tbl):
            st.error("Select workspace, database and table first.")
        else:
            try:
                describe_df = cached_describe_table(ws, db, tbl)
                if describe_df.empty:
                    st.warning("DESCRIBE returned no rows.")
                else:
                    st.write("Table schema (DESCRIBE) — cached")
                    st.dataframe(describe_df, use_container_width=True)
                    cols_meta = describe_to_column_meta(describe_df)
                    schema_payload = {"workspace": ws, "database": db, "table": tbl, "columns": cols_meta}
                    save_path = save_schema_json(f"{ws}_{db}_{tbl}", schema_payload)
                    st.success(f"Schema saved to {save_path}")
            except Exception as e:
                st.error(f"Failed to DESCRIBE table: {e}")

    if st.button("Generate report"):
        ws = st.session_state.workspace
        db = st.session_state.selected_database
        tbl = st.session_state.selected_table
        if not (ws and db and tbl):
            st.error("Select workspace, database and table first.")
        else:
            try:
                # fetch schema (cached) for the selected table only
                describe_df = cached_describe_table(ws, db, tbl)
                cols_meta = describe_to_column_meta(describe_df)

                # sample rows (bounded)
                df = sample_table_rows(ws, db, tbl, limit=2000)

                # build prompt (small preview + stats + schema)
                meta = {"workspace": ws, "database": db, "table": tbl, "row_sample_count": len(df)}
                schema_block = json.dumps({"columns": cols_meta}, indent=2)
                preview = df.head(20).to_markdown(index=False) if not df.empty else "No rows available"
                try:
                    stats = df.describe(include="all").to_markdown() if not df.empty else "No stats available"
                except Exception:
                    stats = "Stats unavailable"

                prompt = (
                    f"Workspace context:\n{json.dumps(meta, indent=2)}\n\n"
                    f"Table schema:\n{schema_block}\n\n"
                    f"Sample preview (top 20 rows):\n{preview}\n\n"
                    f"Descriptive stats:\n{stats}\n\n"
                    "Task:\nGenerate a concise, meaningful report that:\n"
                    "- Identifies key trends, anomalies, and business-relevant insights\n"
                    "- Highlights potential data quality issues\n"
                    "- Suggests next analyses or metrics\n"
                    "- Uses clear section headings and bullet points\n"
                    "- Avoids reprinting raw data\n"
                )
                if task_hint:
                    prompt += f"\nAdditional hint: {task_hint}\n"

                system = "You are a senior data analyst. Provide accurate, concise, action-oriented insights. If data is limited, state assumptions."
                headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
                payload = {"model": MODEL_NAME, "temperature": 0.2, "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}]}
                resp = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload, timeout=120)
                resp.raise_for_status()
                data = resp.json()
                report = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                if report:
                    st.success("Report generated.")
                    st.markdown(report)
                else:
                    st.error("Groq returned an empty report.")
            except Exception as e:
                st.error(f"Report generation failed: {e}")

st.caption("Refresh workspace -> shows Database dropdown and Table names only. DESCRIBE is executed per-table on demand.")
