import os
import pandas as pd
import numpy as np
import streamlit as st
from databricks import sql
from io import BytesIO
from datetime import datetime

# ---------- Config ----------
def get_secret(name: str) -> str:
    return st.secrets.get(name) or os.getenv(name) or ""

DATABRICKS_SERVER_HOST = get_secret("DATABRICKS_SERVER_HOST")
DATABRICKS_HTTP_PATH = get_secret("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = get_secret("DATABRICKS_ACCESS_TOKEN")
WORKSPACE_NAME = get_secret("WORKSPACE_NAME") or None 

# ---------- Low-level query helper ----------
def run_query_raw(query: str, use_catalog: str = None, use_schema: str = None):
    """Execute query and return (rows, cols)."""
    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cur:
            # Only attempt to USE CATALOG if we actually have a name
            if use_catalog and use_catalog != "None":
                try: cur.execute(f"USE CATALOG {use_catalog}")
                except Exception: pass
            
            if use_schema:
                try: 
                    cur.execute(f"USE SCHEMA {use_catalog}.{use_schema}" if (use_catalog and use_catalog != "None") else f"USE SCHEMA {use_schema}")
                except Exception: 
                    try: cur.execute(f"USE {use_schema}")
                    except Exception: pass
            
            cur.execute(query)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
    return rows, cols

def get_sql_path(workspace, database, table):
    """Prevents 'None.database.table' errors."""
    if workspace and workspace != "None":
        return f"{workspace}.{database}.{table}"
    return f"{database}.{table}"

def fetch_current_catalog():
    """Auto-detects catalog if not provided in secrets."""
    try:
        rows, _ = run_query_raw("SELECT current_catalog()")
        return rows[0][0] if rows else None
    except Exception:
        return None

def run_aggregation_query(workspace, database, table, col_def, agg_type="SUM"):
    """
    Helper to get totals.
    col_def: Can be a column name 'Spends' or expression 'DISTINCT Week'
    """
    table_path = get_sql_path(workspace, database, table)
    # Constructs: SELECT SUM(Spends) or SELECT COUNT(DISTINCT Week)
    q = f"SELECT {agg_type}({col_def}) FROM {table_path}"
    rows, _ = run_query_raw(q, use_catalog=workspace, use_schema=database)
    return rows[0][0] if rows and rows[0][0] is not None else 0

# ---------- Caching Helpers ----------
@st.cache_data(ttl=300)
def cached_list_databases(workspace: str):
    try:
        if workspace:
            rows, cols = run_query_raw(f"SHOW SCHEMAS IN CATALOG {workspace}", use_catalog=workspace)
        else:
            rows, cols = run_query_raw("SHOW SCHEMAS")
    except Exception:
        rows, cols = run_query_raw("SHOW SCHEMAS")
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

@st.cache_data(ttl=300)
def cached_list_table_names(workspace: str, database: str):
    try:
        if workspace:
            rows, cols = run_query_raw(f"SHOW TABLES IN {workspace}.{database}", use_catalog=workspace, use_schema=database)
        else:
            rows, cols = run_query_raw(f"SHOW TABLES IN {database}", use_schema=database)
    except Exception:
        rows, cols = run_query_raw(f"SHOW TABLES IN {database}", use_schema=database)
        
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows, columns=cols)
    possible = [c for c in df.columns if c.lower() in ("table_name", "tablename", "name", "table")]
    name_col = possible[0] if possible else df.columns[0]
    return df[[name_col]].rename(columns={name_col: "table_name"})

@st.cache_data(ttl=600)
def cached_describe_table(workspace: str, database: str, table: str):
    table_path = get_sql_path(workspace, database, table)
    try:
        rows, cols = run_query_raw(f"DESCRIBE TABLE {table_path}", use_catalog=workspace, use_schema=database)
    except Exception:
        rows, cols = run_query_raw(f"DESCRIBE {database}.{table}", use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

# ---------- Data Fetching ----------
def fetch_filtered_data(workspace, database, table, date_col, start_dt, end_dt, limit=500000):
    table_path = get_sql_path(workspace, database, table)
    where_clause = f"WHERE {date_col} >= '{start_dt}' AND {date_col} <= '{end_dt}'"
    q = f"SELECT * FROM {table_path} {where_clause} LIMIT {limit}"
    rows, cols = run_query_raw(q, use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

def to_excel(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Pacing Report')
    return output.getvalue()

# ---------- PACING LOGIC ----------
def calculate_kpis(df_filtered, total_budget_global, total_days_global, cols_map):
    spend_col = cols_map['spend']
    rev_col = cols_map['revenue']
    conv_col = cols_map['conversion']
    date_col = cols_map['date']

    # Ensure numeric
    for c in [spend_col, rev_col, conv_col]:
        df_filtered[c] = pd.to_numeric(df_filtered[c], errors='coerce').fillna(0)

    # 1. Current Metrics (Filtered Data)
    Total_Spend_Till_Date = df_filtered[spend_col].sum()
    Total_Revenue_Till_Date = df_filtered[rev_col].sum()
    total_conversions_till_date = df_filtered[conv_col].sum()
    
    # Calculations
    roas = round(Total_Revenue_Till_Date / Total_Spend_Till_Date, 2) if Total_Spend_Till_Date > 0 else 0
    cpa = round((Total_Spend_Till_Date / total_conversions_till_date), 2) if total_conversions_till_date > 0 else 0
    
    # 2. Budget & Pacing (Global Context)
    Total_Approved_Budget = float(total_budget_global)
    
    Spend_Pacing = round((Total_Spend_Till_Date / Total_Approved_Budget) * 100, 2) if Total_Approved_Budget > 0 else 0
    
    # Days
    Days_Elapsed = df_filtered[date_col].nunique()
    Days_Remaining = total_days_global - Days_Elapsed
    
    # Pacing Math
    if Days_Remaining + Days_Elapsed > 0:
        Expected_Pacing = round((Days_Elapsed / Days_Remaining) * 100, 2)
    else:
        Expected_Pacing = 100.0

    Pacing_Variance = round(Spend_Pacing - Expected_Pacing, 2)
    Remaining_Budget = Total_Approved_Budget - Total_Spend_Till_Date
    
    # Status Labels
    Pacing_Status = "UNDER PACING" if Spend_Pacing < Expected_Pacing else "PACING WELL"
    Pacing_Status_color = "Red" if Pacing_Status == "UNDER PACING" else "Green"
    roas_status = "Poor" if roas <= 1 else "Good"
    cpa_status = "Poor" if cpa > 100 else "Good"

    data = {
        "Metric": [
            "Days Elapsed", "Days Remaining", "Total Spend Till Date", 
            "Total Revenue Till Date", "Current ROAS", "Current CPA", 
            "Spend Pacing % (Actual)", "Expected Pacing % (by calendar)", 
            "Pacing Variance", "Pacing Status", "Remaining Budget"
        ],
        "Value": [
            f"{int(Days_Elapsed)} Days", f"{int(Days_Remaining)} Days", 
            f"${Total_Spend_Till_Date:,.2f}", f"${Total_Revenue_Till_Date:,.2f}", 
            f"{roas}x", f"${cpa}", 
            f"{Spend_Pacing}%", f"{Expected_Pacing}%", 
            f"{Pacing_Variance}%", Pacing_Status, f"${Remaining_Budget:,.2f}"
        ],
        "Status": [
            np.nan, np.nan, np.nan, np.nan, 
            roas_status, cpa_status, np.nan, np.nan, 
            Pacing_Status, Pacing_Status_color, np.nan
        ]
    }
    return pd.DataFrame(data)

# ---------- UI Setup ----------
st.set_page_config(page_title="Pacing Report", layout="wide")
st.title("📊 Automated Pacing Report")

# Session State
if "workspace" not in st.session_state: st.session_state.workspace = WORKSPACE_NAME
if "databases" not in st.session_state: st.session_state.databases = []
if "tables" not in st.session_state: st.session_state.tables = []
if "selected_database" not in st.session_state: st.session_state.selected_database = None
if "selected_table" not in st.session_state: st.session_state.selected_table = None

# Sidebar
def refresh_workspace():
    cached_list_databases.clear()
    cached_list_table_names.clear()
    
    # Try to detect workspace if None
    ws = WORKSPACE_NAME
    if not ws:
        detected = fetch_current_catalog()
        if detected:
            ws = detected
            st.session_state.workspace = ws
            st.sidebar.success(f"Detected Catalog: {ws}")
    
    # Get databases
    dbs = cached_list_databases(ws)
    if not dbs.empty:
        col_name = dbs.columns[0]
        st.session_state.databases = dbs[col_name].astype(str).tolist()
        st.session_state.selected_database = st.session_state.databases[0]

if st.sidebar.button("Refresh Workspace"):
    refresh_workspace()

st.sidebar.markdown(f"**Catalog:** {st.session_state.workspace or 'Not Set (using default)'}")

if st.session_state.databases:
    st.session_state.selected_database = st.sidebar.selectbox("Database", st.session_state.databases)
    if st.session_state.selected_database:
        tbls = cached_list_table_names(st.session_state.workspace, st.session_state.selected_database)
        st.session_state.tables = tbls["table_name"].astype(str).tolist() if not tbls.empty else []

if st.session_state.tables:
    st.session_state.selected_table = st.sidebar.selectbox("Table", st.session_state.tables)

# Main UI
if st.session_state.selected_table:
    ws = st.session_state.workspace
    db = st.session_state.selected_database
    tbl = st.session_state.selected_table
    
    schema_df = cached_describe_table(ws, db, tbl)
    col_names = []
    if not schema_df.empty:
        # find column name column (usually col_name or name)
        c_col = [c for c in schema_df.columns if 'name' in c.lower()][0]
        col_names = schema_df[c_col].tolist()

    st.subheader("1. Map Data Columns")
    c1, c2, c3, c4 = st.columns(4)
    
    def find_col(options, keyword):
        found = [o for o in options if keyword in o.lower()]
        return found[0] if found else options[0]

    if col_names:
        with c1:
            date_col = st.selectbox("Date Column", col_names, index=col_names.index(find_col(col_names, "date")))
        with c2:
            spend_col = st.selectbox("Spends Column", col_names, index=col_names.index(find_col(col_names, "spend")))
        with c3:
            rev_col = st.selectbox("Revenue Column", col_names, index=col_names.index(find_col(col_names, "rev")))
        with c4:
            conv_col = st.selectbox("Conversion Column", col_names, index=col_names.index(find_col(col_names, "conv")))

        st.subheader("2. Select Reporting Period")
        c_d1, c_d2 = st.columns(2)
        start_date = c_d1.date_input("Start Date")
        end_date = c_d2.date_input("End Date")

        if st.button("Generate Pacing Report", type="primary"):
            if start_date and end_date:
                with st.spinner("Fetching data and calculating KPIs..."):
                    try:
                        # 1. Get Filtered Data
                        df_filtered = fetch_filtered_data(ws, db, tbl, date_col, start_date, end_date)
                        
                        if df_filtered.empty:
                            st.warning("No data found for this date range.")
                        else:
                            # 2. Get Global Totals
                            # --- FIXED LINES HERE ---
                            total_budget_global = run_aggregation_query(ws, db, tbl, spend_col, "SUM")
                            # We pass "DISTINCT {date_col}" as the column, and "COUNT" as the aggregator
                            # This generates SELECT COUNT(DISTINCT Week) FROM ...
                            total_days_global = run_aggregation_query(ws, db, tbl, f"DISTINCT {date_col}", "COUNT") 
                            
                            # 3. Calculate Report
                            col_map = {
                                "date": date_col,
                                "spend": spend_col,
                                "revenue": rev_col,
                                "conversion": conv_col
                            }
                            
                            report_df = calculate_kpis(df_filtered, total_budget_global, total_days_global, col_map)
                            
                            st.success("Report Generated Successfully")
                            
                            # Apply color to the 'Status' column in Streamlit view
                            def style_df(row):
                                color = row['Status'] if row.name == 9 else '' # Only coloring Pacing Status row based on Status value? 
                                # Actually, let's just display clean
                                return [''] * len(row)

                            st.dataframe(report_df, use_container_width=True)
                            
                            excel_data = to_excel(report_df)
                            st.download_button(
                                label="📥 Download Pacing Report (.xlsx)",
                                data=excel_data,
                                file_name="pacing_report.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )

                    except Exception as e:
                        st.error(f"Error generating report: {e}")
            else:
                st.error("Please select dates.")
    else:
        st.error("Could not read table schema. Please check permissions.")