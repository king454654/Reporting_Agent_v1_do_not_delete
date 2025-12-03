import os
import pandas as pd
import numpy as np
import streamlit as st
from databricks import sql
from io import BytesIO
from datetime import datetime, timedelta

# Email Imports
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# ---------- Config ----------
def get_secret(name: str) -> str:
    # Checks Streamlit secrets first, then OS environment variables
    return st.secrets.get(name) or os.getenv(name) or ""

DATABRICKS_SERVER_HOST = get_secret("DATABRICKS_SERVER_HOST")
DATABRICKS_HTTP_PATH = get_secret("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = get_secret("DATABRICKS_ACCESS_TOKEN")
WORKSPACE_NAME = get_secret("WORKSPACE_NAME") or None 

# ---------- Low-level query helper ----------
def run_query_raw(query: str, use_catalog: str = None, use_schema: str = None):
    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cur:
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
    if workspace and workspace != "None":
        return f"{workspace}.{database}.{table}"
    return f"{database}.{table}"

def fetch_current_catalog():
    try:
        rows, _ = run_query_raw("SELECT current_catalog()")
        return rows[0][0] if rows else None
    except Exception:
        return None

def run_aggregation_query(workspace, database, table, col_def, agg_type="SUM"):
    table_path = get_sql_path(workspace, database, table)
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

# ---------- Data Fetching & Export (UPDATED) ----------

def fetch_filtered_data(workspace, database, table, col_map, start_dt, end_dt, limit=500000):
    # Optimization: Only select the columns we actually need
    needed_cols = list(col_map.values())
    selected_cols_str = ", ".join(needed_cols)
    date_col = col_map['date']

    table_path = get_sql_path(workspace, database, table)
    where_clause = f"WHERE {date_col} >= '{start_dt}' AND {date_col} <= '{end_dt}'"
    q = f"SELECT {selected_cols_str} FROM {table_path} {where_clause} LIMIT {limit}"
    
    rows, cols = run_query_raw(q, use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

def to_excel_separate_sheets(dfs: dict) -> bytes:
    """
    Writes multiple dataframes to a SINGLE Excel file, but on SEPARATE SHEETS (TABS).
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in dfs.items():
            # Ensure sheet name is valid (Excel limits to 31 chars)
            clean_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=clean_name, index=False)
    return output.getvalue()

# ---------- Email Functionality ----------
def send_email_with_attachment(recipient_email, excel_bytes, filename="pacing_report.xlsx"):
    smtp_server = get_secret("SMTP_SERVER")
    smtp_port_str = get_secret("SMTP_PORT")
    sender_email = get_secret("SENDER_EMAIL")
    sender_password = get_secret("SENDER_PASSWORD")
    
    if not all([smtp_server, sender_email, sender_password]):
        return False, "Missing SMTP configuration in secrets.toml"

    try:
        smtp_port = int(smtp_port_str) if smtp_port_str else 587
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = f"Pacing Report - {datetime.now().strftime('%Y-%m-%d')}"

        body = "Hello,\n\nPlease find the attached Pacing Report.\n\nBest Regards,\nAnalytics Team"
        msg.attach(MIMEText(body, 'plain'))

        part = MIMEBase('application', 'octet-stream')
        part.set_payload(excel_bytes)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        msg.attach(part)

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        
        return True, "Email sent successfully!"
    except Exception as e:
        return False, str(e)

# ---------- LOGIC: KPI & Breakdowns ----------

def determine_status(spend_pct):
    if spend_pct < 95: return "Under Pacing"
    elif spend_pct > 105: return "Over Pacing"
    return "On Track"

def process_granular_data(df, cols_map, freq='W'):
    df = df.copy()
    date_col = cols_map['date']
    spend_col = cols_map['spend']
    rev_col = cols_map['revenue']
    budget_col = cols_map['budget']
    
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Pre-aggregate to Daily
    daily_df = df.groupby(pd.Grouper(key=date_col, freq='D')).agg({
        spend_col: 'sum',
        rev_col: 'sum',
        budget_col: 'sum' 
    }).reset_index().fillna(0)

    # Resample
    if freq == 'D':
        grouped = daily_df.copy()
    else:
        resample_rule = 'W-MON' if freq == 'W' else 'MS'
        grouped = daily_df.set_index(date_col).resample(resample_rule, closed='left', label='left').agg({
            spend_col: 'sum',
            rev_col: 'sum',
            budget_col: 'sum' 
        }).reset_index()

    results = []
    week_counter = 1
    
    for _, row in grouped.iterrows():
        d = row[date_col]
        spend = row[spend_col]
        rev = row[rev_col]
        budget = row[budget_col]
        
        if budget == 0 and spend == 0: continue

        roas = round(rev / spend, 2) if spend > 0 else 0.0
        pct_spent = round((spend / budget) * 100, 2) if budget > 0 else 0.0
        status = determine_status(pct_spent)
        
        if freq == 'W':
            end_d = d + timedelta(days=6)
            period_label = f"Week {week_counter}"
            date_range = f"{d.strftime('%d-%b')} to {end_d.strftime('%d-%b')}"
            week_counter += 1
            results.append({
                "Week Label": period_label, "Period": date_range, "Budget": budget,
                "Spend": spend, "% Spent": f"{pct_spent}%", "Revenue": rev,
                "ROAS": roas, "Status": status
            })
        elif freq == 'M':
            label = d.strftime('%b %Y')
            results.append({
                "Month Label": label, "Budget": budget, "Spend MTD": spend,
                "Revenue MTD": rev, "ROAS": roas, "% Spent": f"{pct_spent}%", "Status": status
            })
        elif freq == 'D':
            label = d.strftime('%Y-%m-%d')
            day_name = d.strftime('%A')
            results.append({
                "Date": label, "Day": day_name, "Budget": budget, "Spend": spend,
                "% Spent": f"{pct_spent}%", "Revenue": rev, "ROAS": roas, "Status": status
            })
            
    return pd.DataFrame(results)

def calculate_summary_kpis(df_filtered, total_budget_global, total_days_global, cols_map):
    spend_col = cols_map['spend']
    rev_col = cols_map['revenue']
    conv_col = cols_map['conversion']
    date_col = cols_map['date']

    for c in [spend_col, rev_col, conv_col]:
        df_filtered[c] = pd.to_numeric(df_filtered[c], errors='coerce').fillna(0)

    Total_Spend_Till_Date = df_filtered[spend_col].sum()
    Total_Revenue_Till_Date = df_filtered[rev_col].sum()
    total_conversions_till_date = df_filtered[conv_col].sum()
    
    roas = round(Total_Revenue_Till_Date / Total_Spend_Till_Date, 2) if Total_Spend_Till_Date > 0 else 0
    cpa = round((Total_Spend_Till_Date / total_conversions_till_date), 2) if total_conversions_till_date > 0 else 0
    Total_Approved_Budget = float(total_budget_global)
    Spend_Pacing = round((Total_Spend_Till_Date / Total_Approved_Budget) * 100, 2) if Total_Approved_Budget > 0 else 0
    
    Days_Elapsed = df_filtered[date_col].nunique()
    Days_Remaining = total_days_global - Days_Elapsed
    
    if Days_Remaining + Days_Elapsed > 0:
        Expected_Pacing = round((Days_Elapsed / Days_Remaining) * 100, 2)
    else:
        Expected_Pacing = 100.0

    Pacing_Variance = round(Spend_Pacing - Expected_Pacing, 2)
    Remaining_Budget = Total_Approved_Budget - Total_Spend_Till_Date
    Pacing_Status = "UNDER PACING" if Spend_Pacing < Expected_Pacing else "PACING WELL"
    Pacing_Status_color = "Red" if Pacing_Status == "UNDER PACING" else "Green"

    data = {
        "Metric": ["Days Elapsed", "Days Remaining", "Total Spend Till Date", "Total Revenue Till Date", "Current ROAS", "Current CPA", "Spend Pacing % (Actual)", "Expected Pacing %", "Pacing Variance", "Pacing Status", "Remaining Budget"],
        "Value": [f"{int(Days_Elapsed)} Days", f"{int(Days_Remaining)} Days", f"${Total_Spend_Till_Date:,.2f}", f"${Total_Revenue_Till_Date:,.2f}", f"{roas}x", f"${cpa}", f"{Spend_Pacing}%", f"{Expected_Pacing}%", f"{Pacing_Variance}%", Pacing_Status, f"${Remaining_Budget:,.2f}"],
        "Status": [np.nan, np.nan, np.nan, np.nan, ("Poor" if roas <= 1 else "Good"), ("Poor" if cpa > 100 else "Good"), np.nan, np.nan, Pacing_Status, Pacing_Status_color, np.nan]
    }
    return pd.DataFrame(data)

# ---------- UI Setup ----------
st.set_page_config(page_title="Pacing Report", layout="wide")
st.title("📊 Automated Pacing Report")

# Session State Initialization
if "workspace" not in st.session_state: st.session_state.workspace = WORKSPACE_NAME
if "databases" not in st.session_state: st.session_state.databases = []
if "tables" not in st.session_state: st.session_state.tables = []
if "selected_database" not in st.session_state: st.session_state.selected_database = None
if "selected_table" not in st.session_state: st.session_state.selected_table = None

# Result Storage
if "summary_df" not in st.session_state: st.session_state.summary_df = None
if "daily_df" not in st.session_state: st.session_state.daily_df = None
if "weekly_df" not in st.session_state: st.session_state.weekly_df = None
if "monthly_df" not in st.session_state: st.session_state.monthly_df = None
if "excel_bytes" not in st.session_state: st.session_state.excel_bytes = None

def refresh_workspace():
    cached_list_databases.clear()
    cached_list_table_names.clear()
    ws = WORKSPACE_NAME
    if not ws:
        detected = fetch_current_catalog()
        if detected:
            ws = detected
            st.session_state.workspace = ws
            st.sidebar.success(f"Detected Catalog: {ws}")
    dbs = cached_list_databases(ws)
    if not dbs.empty:
        col_name = dbs.columns[0]
        st.session_state.databases = dbs[col_name].astype(str).tolist()
        st.session_state.selected_database = st.session_state.databases[0]

if st.sidebar.button("Refresh Workspace"):
    refresh_workspace()

st.sidebar.markdown(f"**Catalog:** {st.session_state.workspace or 'Not Set'}")
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
        c_col = [c for c in schema_df.columns if 'name' in c.lower()][0]
        col_names = schema_df[c_col].tolist()

    st.subheader("1. Map Data Columns")
    c1, c2, c3, c4, c5 = st.columns(5)
    
    def find_col(options, keyword):
        found = [o for o in options if keyword in o.lower()]
        return found[0] if found else options[0]

    if col_names:
        with c1:
            date_col = st.selectbox("Date Col", col_names, index=col_names.index(find_col(col_names, "date")))
        with c2:
            spend_col = st.selectbox("Spend Col", col_names, index=col_names.index(find_col(col_names, "spend")))
        with c3:
            budget_idx = col_names.index(find_col(col_names, "budget")) if find_col(col_names, "budget") else 0
            budget_col = st.selectbox("Budget Col", col_names, index=budget_idx)
        with c4:
            rev_col = st.selectbox("Revenue Col", col_names, index=col_names.index(find_col(col_names, "rev")))
        with c5:
            conv_col = st.selectbox("Conv Col", col_names, index=col_names.index(find_col(col_names, "conv")))

        st.subheader("2. Select Reporting Period")
        c_d1, c_d2 = st.columns(2)
        start_date = c_d1.date_input("Start Date")
        end_date = c_d2.date_input("End Date")

        # GENERATE REPORT BUTTON
        if st.button("Generate Reports", type="primary"):
            if start_date and end_date:
                with st.spinner("Crunching numbers..."):
                    try:
                        col_map = {
                            "date": date_col, "spend": spend_col, 
                            "revenue": rev_col, "conversion": conv_col, 
                            "budget": budget_col
                        }
                        
                        # Fetch Data
                        df_filtered = fetch_filtered_data(ws, db, tbl, col_map, start_date, end_date)
                        
                        if df_filtered.empty:
                            st.warning("No data found for this date range.")
                            st.session_state.excel_bytes = None
                        else:
                            # Make numeric
                            for c in [spend_col, rev_col, conv_col, budget_col]:
                                df_filtered[c] = pd.to_numeric(df_filtered[c], errors='coerce').fillna(0)

                            # 1. Overall Summary Report
                            total_budget_global = run_aggregation_query(ws, db, tbl, spend_col, "SUM")
                            total_days_global = run_aggregation_query(ws, db, tbl, f"DISTINCT {date_col}", "COUNT") 
                            
                            # Logic
                            summary_df = calculate_summary_kpis(df_filtered, total_budget_global, total_days_global, col_map)
                            daily_df = process_granular_data(df_filtered, col_map, freq='D')
                            weekly_df = process_granular_data(df_filtered, col_map, freq='W')
                            monthly_df = process_granular_data(df_filtered, col_map, freq='M')

                            # SAVE TO SESSION STATE
                            st.session_state.summary_df = summary_df
                            st.session_state.daily_df = daily_df
                            st.session_state.weekly_df = weekly_df
                            st.session_state.monthly_df = monthly_df
                            
                            # Generate Excel Bytes (SEPARATE SHEETS)
                            dfs_to_save = {
                                'Summary': summary_df,
                                'Daily': daily_df,
                                'Weekly': weekly_df,
                                'Monthly': monthly_df
                            }
                            st.session_state.excel_bytes = to_excel_separate_sheets(dfs_to_save)
                            st.success("Reports generated successfully!")

                    except Exception as e:
                        st.error(f"Error generating report: {e}")
            else:
                st.error("Please select dates.")

        # DISPLAY RESULTS
        if st.session_state.excel_bytes is not None:
            st.divider()
            
            # Show Tabs in UI for easier viewing
            tab1, tab2, tab3, tab4 = st.tabs(["📋 Executive Summary", "📅 Daily Breakdown", "📅 Weekly Breakdown", "🗓️ Monthly Breakdown"])
            
            with tab1:
                st.dataframe(st.session_state.summary_df, use_container_width=True)
            with tab2:
                st.dataframe(st.session_state.daily_df.style.format({"Budget": "${:,.2f}", "Spend": "${:,.2f}", "Revenue": "${:,.2f}", "ROAS": "{:.2f}"}), use_container_width=True)
            with tab3:
                st.dataframe(st.session_state.weekly_df.style.format({"Budget": "${:,.2f}", "Spend": "${:,.2f}", "Revenue": "${:,.2f}", "ROAS": "{:.2f}"}), use_container_width=True)
            with tab4:
                st.dataframe(st.session_state.monthly_df.style.format({"Budget": "${:,.2f}", "Spend MTD": "${:,.2f}", "Revenue MTD": "${:,.2f}", "ROAS": "{:.2f}"}), use_container_width=True)

            st.divider()
            st.subheader("📤 Export & Share")
            
            col_download, col_email = st.columns([1, 2])

            # 1. Download Button
            with col_download:
                st.download_button(
                    label="📥 Download Excel Report",
                    data=st.session_state.excel_bytes,
                    file_name="pacing_report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

            # 2. Email Form
            with col_email:
                with st.form(key="email_form"):
                    recipient = st.text_input("Recipient Email Address", placeholder="manager@company.com")
                    submit_email = st.form_submit_button("✉️ Send via Email", type="secondary")
                    
                    if submit_email:
                        if recipient:
                            with st.spinner("Sending email..."):
                                success, msg = send_email_with_attachment(recipient, st.session_state.excel_bytes)
                                if success:
                                    st.success(msg)
                                else:
                                    st.error(f"Failed: {msg}")
                        else:
                            st.warning("Please enter an email address.")

    else:
        if not col_names:
            st.error("Could not read table schema. Please check permissions.")