import os
import pandas as pd
import numpy as np
import streamlit as st
from io import BytesIO
from datetime import datetime, timedelta, date
import xlsxwriter

# Email Imports
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# --- 1. SAFE IMPORT OF CONNECTOR LIBRARIES ---
try: from databricks import sql as databricks_sql
except ImportError: databricks_sql = None

try: import snowflake.connector
except ImportError: snowflake_connector = None

try: import boto3
except ImportError: boto3 = None

try: from google.cloud import bigquery
except ImportError: bigquery = None

try: from google.cloud import storage as gcs_storage
except ImportError: gcs_storage = None

try: import pymysql
except ImportError: pymysql = None

try: import pyodbc
except ImportError: pyodbc = None

try: import psycopg2
except ImportError: psycopg2 = None

# ---------- Config ----------
def get_secret(name: str) -> str:
    return st.secrets.get(name) or os.getenv(name) or ""

# ---------- 2. CONNECTOR HELPER FUNCTIONS ----------

def fetch_database_list(source_type, config):
    """Discovers available databases/schemas."""
    dbs = []
    try:
        if source_type == "Databricks":
            if not databricks_sql: return []
            with databricks_sql.connect(server_hostname=config['host'], http_path=config['http_path'], access_token=config['token']) as conn:
                with conn.cursor() as cur:
                    cur.execute("SHOW SCHEMAS")
                    dbs = [row[0] for row in cur.fetchall()]

        elif source_type == "Snowflake":
            if not snowflake_connector: return []
            ctx = snowflake_connector.connect(user=config['user'], password=config['password'], account=config['account'], warehouse=config['warehouse'])
            cur = ctx.cursor()
            cur.execute("SHOW DATABASES")
            dbs = [row[1] for row in cur.fetchall()]
            cur.close()

        elif source_type == "BigQuery":
            if not bigquery: return []
            client = bigquery.Client()
            dbs = [d.dataset_id for d in list(client.list_datasets())]

        elif source_type == "MySQL":
            if not pymysql: return []
            conn = pymysql.connect(host=config['host'], user=config['user'], password=config['password'])
            with conn.cursor() as cur:
                cur.execute("SHOW DATABASES")
                dbs = [row[0] for row in cur.fetchall()]
            conn.close()

        elif source_type == "SQL Server":
            if not pyodbc: return []
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};UID={config['user']};PWD={config['password']}"
            conn = pyodbc.connect(conn_str)
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM sys.databases")
                dbs = [row[0] for row in cur.fetchall()]
            conn.close()

        elif source_type == "Redshift":
            if not psycopg2: return []
            conn = psycopg2.connect(host=config['host'], port=config.get('port',5439), user=config['user'], password=config['password'], dbname=config['database'])
            with conn.cursor() as cur:
                cur.execute("SELECT schema_name FROM information_schema.schemata")
                dbs = [row[0] for row in cur.fetchall()]
            conn.close()

    except Exception as e:
        st.error(f"Error fetching databases: {e}")
    return sorted(dbs)

def fetch_table_list(source_type, config, database_name):
    """Discovers tables within a database."""
    tbls = []
    try:
        if source_type == "Databricks":
            with databricks_sql.connect(server_hostname=config['host'], http_path=config['http_path'], access_token=config['token']) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SHOW TABLES IN `{database_name}`")
                    tbls = [row[1] for row in cur.fetchall()]

        elif source_type == "Snowflake":
            ctx = snowflake_connector.connect(user=config['user'], password=config['password'], account=config['account'], warehouse=config['warehouse'], database=database_name)
            cur = ctx.cursor()
            cur.execute(f"SHOW TABLES IN DATABASE {database_name}")
            tbls = [row[1] for row in cur.fetchall()]
            cur.close()

        elif source_type == "BigQuery":
            client = bigquery.Client()
            tbls = [t.table_id for t in list(client.list_tables(database_name))]

        elif source_type == "MySQL":
            conn = pymysql.connect(host=config['host'], user=config['user'], password=config['password'], database=database_name)
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                tbls = [row[0] for row in cur.fetchall()]
            conn.close()

        elif source_type == "SQL Server":
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};DATABASE={database_name};UID={config['user']};PWD={config['password']}"
            conn = pyodbc.connect(conn_str)
            with conn.cursor() as cur:
                cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
                tbls = [row[0] for row in cur.fetchall()]
            conn.close()

        elif source_type == "Redshift":
            conn = psycopg2.connect(host=config['host'], port=config.get('port',5439), user=config['user'], password=config['password'], dbname=config['database'])
            with conn.cursor() as cur:
                cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{database_name}'")
                tbls = [row[0] for row in cur.fetchall()]
            conn.close()

    except Exception as e:
        st.error(f"Error fetching tables: {e}")
    return sorted(tbls)

def fetch_raw_data(source_type, config, database, table, limit=50000):
    """Fetches data rows."""
    try:
        if source_type == "Databricks":
            with databricks_sql.connect(server_hostname=config['host'], http_path=config['http_path'], access_token=config['token']) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM `{database}`.`{table}` LIMIT {limit}")
                    rows = cur.fetchall()
                    cols = [d[0] for d in cur.description]
                    return pd.DataFrame(rows, columns=cols)
        
        elif source_type == "Snowflake":
            ctx = snowflake_connector.connect(user=config['user'], password=config['password'], account=config['account'], warehouse=config['warehouse'], database=database)
            cur = ctx.cursor()
            cur.execute(f"SELECT * FROM {database}.{config.get('schema','PUBLIC')}.{table} LIMIT {limit}")
            df = cur.fetch_pandas_all()
            cur.close()
            return df

        elif source_type == "BigQuery":
            client = bigquery.Client()
            return client.query(f"SELECT * FROM `{database}.{table}` LIMIT {limit}").to_dataframe()

        elif source_type == "MySQL":
            conn = pymysql.connect(host=config['host'], user=config['user'], password=config['password'], database=database)
            df = pd.read_sql(f"SELECT * FROM `{table}` LIMIT {limit}", conn)
            conn.close()
            return df

        elif source_type == "SQL Server":
            conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};DATABASE={database};UID={config['user']};PWD={config['password']}"
            conn = pyodbc.connect(conn_str)
            df = pd.read_sql(f"SELECT TOP {limit} * FROM {table}", conn)
            conn.close()
            return df
        
        elif source_type == "AWS S3":
            if not boto3: return pd.DataFrame()
            s3 = boto3.client('s3', aws_access_key_id=config['key'], aws_secret_access_key=config['secret'])
            obj = s3.get_object(Bucket=config['bucket'], Key=config['file_path'])
            if config['file_path'].endswith('.parquet'): return pd.read_parquet(BytesIO(obj['Body'].read()))
            return pd.read_csv(obj['Body'])

        elif source_type == "GCS":
            if not gcs_storage: return pd.DataFrame()
            client = gcs_storage.Client()
            bucket = client.get_bucket(config['bucket'])
            blob = bucket.blob(config['file_path'])
            data = blob.download_as_bytes()
            if config['file_path'].endswith('.parquet'): return pd.read_parquet(BytesIO(data))
            return pd.read_csv(BytesIO(data))

    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()
    
    return pd.DataFrame()

# ---------- 3. REPORTING LOGIC (UPDATED WITH FIX) ----------

def get_dynamic_insights(df, period_type):
    if df.empty: return ["No data available."]
    insights = []
    col_spend = 'Spend MTD' if 'Spend MTD' in df.columns else 'Spend'
    col_label = 'Month Label' if 'Month Label' in df.columns else ('Week Label' if 'Week Label' in df.columns else 'Date')

    if 'ROAS' in df.columns and df['ROAS'].sum() > 0:
        top_roas = df.loc[df['ROAS'].idxmax()]
        insights.append(f"Best Efficiency: {top_roas[col_label]} had highest ROAS of {top_roas['ROAS']}x.")
    
    if col_spend in df.columns:
        top_spend = df.loc[df[col_spend].idxmax()]
        insights.append(f"Highest Spend: {top_spend[col_label]} with ${top_spend[col_spend]:,.2f}.")

    return insights[:4]

def determine_status(spend_pct):
    if spend_pct < 95: return "Under Pacing"
    elif spend_pct > 105: return "Over Pacing"
    return "On Track"

def process_granular_data(df, cols_map, freq='W'):
    # Fix: Create copy to prevent memory view errors
    df = df.copy()
    
    date_col = cols_map['date']
    spend_col = cols_map['spend']
    rev_col = cols_map['revenue']
    budget_col = cols_map['budget']
    
    # --- ERROR PREVENTION: CHECK FOR COLUMN OVERLAP ---
    numeric_inputs = [spend_col, rev_col, budget_col]
    if date_col in numeric_inputs:
        st.error(f"⚠️ Configuration Error: The column '{date_col}' is selected as both DATE and a METRIC (Spend/Budget). Please fix the dropdowns.")
        return pd.DataFrame()

    # --- TYPE SAFETY: CONVERT TO NUMERIC ---
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    for c in numeric_inputs:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    # Aggregation
    try:
        daily_df = df.groupby(pd.Grouper(key=date_col, freq='D')).agg({
            spend_col: 'sum', rev_col: 'sum', budget_col: 'sum'
        }).reset_index().fillna(0)
    except TypeError as e:
        st.error(f"Data Processing Error: Could not sum columns. {e}")
        return pd.DataFrame()

    if freq == 'D': grouped = daily_df.copy()
    else:
        resample_rule = 'W-MON' if freq == 'W' else 'MS'
        grouped = daily_df.set_index(date_col).resample(resample_rule, closed='left', label='left').agg({
            spend_col: 'sum', rev_col: 'sum', budget_col: 'sum'
        }).reset_index()

    results = []
    for _, row in grouped.iterrows():
        d = row[date_col]
        spend, rev, budget = row[spend_col], row[rev_col], row[budget_col]
        
        if budget == 0 and spend == 0: continue

        roas = round(rev / spend, 2) if spend > 0 else 0.0
        pct_spent = round((spend / budget) * 100, 2) if budget > 0 else 0.0
        status = determine_status(pct_spent)
        
        if freq == 'W':
            end_d = d + timedelta(days=6)
            results.append({
                "Week Label": f"Week {d.strftime('%d-%b')}", "Period": f"{d.strftime('%d-%b')} to {end_d.strftime('%d-%b')}",
                "Budget": budget, "Spend": spend, "% Spent": f"{pct_spent}%", "Revenue": rev, "ROAS": roas, "Status": status
            })
        elif freq == 'M':
            results.append({
                "Month Label": d.strftime('%b %Y'), "Budget": budget, "Spend MTD": spend,
                "Revenue MTD": rev, "ROAS": roas, "% Spent": f"{pct_spent}%", "Status": status
            })
        elif freq == 'D':
            results.append({
                "Date": d.strftime('%Y-%m-%d'), "Budget": budget, "Spend": spend,
                "% Spent": f"{pct_spent}%", "Revenue": rev, "ROAS": roas, "Status": status
            })
            
    return pd.DataFrame(results)

def calculate_summary_kpis(df_filtered, cols_map):
    s_col, r_col = cols_map['spend'], cols_map['revenue']
    b_col = cols_map['budget']
    
    total_spend = df_filtered[s_col].sum()
    total_rev = df_filtered[r_col].sum()
    total_budget = df_filtered[b_col].sum()
    
    roas = round(total_rev / total_spend, 2) if total_spend > 0 else 0
    pacing = round((total_spend / total_budget)*100, 2) if total_budget > 0 else 0
    remaining = total_budget - total_spend
    
    data = {
        "Metric": ["Total Spend", "Total Revenue", "ROAS", "Total Budget", "Budget Remaining", "Pacing %"],
        "Value": [f"${total_spend:,.2f}", f"${total_rev:,.2f}", f"{roas}x", f"${total_budget:,.2f}", f"${remaining:,.2f}", f"{pacing}%"]
    }
    return pd.DataFrame(data)

def generate_excel_report(summary_df, report_dfs) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        for name, df in report_dfs.items():
            df.to_excel(writer, sheet_name=name, index=False)
    return output.getvalue()

def send_email_with_attachment(recipient_email, subject, body_html, excel_bytes):
    smtp_server = get_secret("SMTP_SERVER")
    smtp_port = get_secret("SMTP_PORT") or 587
    sender_email = get_secret("SENDER_EMAIL")
    sender_password = get_secret("SENDER_PASSWORD")

    if not all([smtp_server, sender_email, sender_password]):
        return False, "Missing SMTP credentials."

    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject 
        msg.attach(MIMEText(body_html, 'html'))

        part = MIMEBase('application', 'octet-stream')
        part.set_payload(excel_bytes)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="pacing_report.xlsx"')
        msg.attach(part)

        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        return True, "Email sent!"
    except Exception as e:
        return False, str(e)

# ---------- 4. APP UI SETUP ----------

st.set_page_config(page_title="Pacing Report", layout="wide")
st.title("Automated Pacing Report")

# Session State for Dropdowns
if "databases_list" not in st.session_state: st.session_state.databases_list = []
if "tables_list" not in st.session_state: st.session_state.tables_list = []
if "selected_db" not in st.session_state: st.session_state.selected_db = None
if "raw_df" not in st.session_state: st.session_state.raw_df = pd.DataFrame()

# --- SIDEBAR: CONNECTOR SETUP ---
st.sidebar.header("1. Data Connection")
connector = st.sidebar.selectbox("Source Type", ["Databricks", "Snowflake", "BigQuery", "Redshift", "MySQL", "SQL Server", "AWS S3", "GCS"])

config = {}
is_storage = connector in ["AWS S3", "GCS"]

# Connector Inputs
if connector == "Databricks":
    config['host'] = st.sidebar.text_input("Server Hostname", get_secret("DATABRICKS_SERVER_HOST"))
    config['http_path'] = st.sidebar.text_input("HTTP Path", get_secret("DATABRICKS_HTTP_PATH"))
    config['token'] = st.sidebar.text_input("Access Token", get_secret("DATABRICKS_ACCESS_TOKEN"), type="password")
elif connector == "Snowflake":
    config['user'] = st.sidebar.text_input("User", get_secret("SNOWFLAKE_USER"))
    config['password'] = st.sidebar.text_input("Password", get_secret("SNOWFLAKE_PASSWORD"), type="password")
    config['account'] = st.sidebar.text_input("Account", get_secret("SNOWFLAKE_ACCOUNT"))
    config['warehouse'] = st.sidebar.text_input("Warehouse", get_secret("SNOWFLAKE_WAREHOUSE"))
elif connector == "MySQL":
    config['host'] = st.sidebar.text_input("Host", get_secret("MYSQL_HOST"))
    config['user'] = st.sidebar.text_input("User", get_secret("MYSQL_USER"))
    config['password'] = st.sidebar.text_input("Password", get_secret("MYSQL_PASSWORD"), type="password")
elif connector == "SQL Server":
    config['server'] = st.sidebar.text_input("Server", get_secret("SQL_SERVER_HOST"))
    config['user'] = st.sidebar.text_input("User", get_secret("SQL_SERVER_USER"))
    config['password'] = st.sidebar.text_input("Password", get_secret("SQL_SERVER_PASSWORD"), type="password")
elif connector == "AWS S3":
    config['key'] = st.sidebar.text_input("AWS Key", get_secret("AWS_ACCESS_KEY"))
    config['secret'] = st.sidebar.text_input("AWS Secret", get_secret("AWS_SECRET_KEY"), type="password")
    config['bucket'] = st.sidebar.text_input("Bucket Name")
    config['file_path'] = st.sidebar.text_input("File Path (e.g. data.csv)")
elif connector == "Redshift":
    config['host'] = st.sidebar.text_input("Host", get_secret("REDSHIFT_HOST"))
    config['user'] = st.sidebar.text_input("User", get_secret("REDSHIFT_USER"))
    config['password'] = st.sidebar.text_input("Password", get_secret("REDSHIFT_PASSWORD"), type="password")
    config['database'] = st.sidebar.text_input("DB Name", get_secret("REDSHIFT_DB"))

st.sidebar.markdown("---")

if is_storage:
    if st.sidebar.button("Load File"):
        with st.spinner("Downloading file..."):
            st.session_state.raw_df = fetch_raw_data(connector, config, None, None)
else:
    # Database Flow
    if st.sidebar.button("🔌 Connect & List Databases"):
        with st.spinner("Fetching databases..."):
            st.session_state.databases_list = fetch_database_list(connector, config)
            st.session_state.tables_list = [] # Reset tables
            if st.session_state.databases_list:
                st.success(f"Found {len(st.session_state.databases_list)} databases.")
            else:
                st.warning("No databases found or connection failed.")
            
    # Database Dropdown
    if st.session_state.databases_list:
        sel_db = st.sidebar.selectbox("Select Database", st.session_state.databases_list)
        # Update tables if DB changed
        if sel_db != st.session_state.selected_db:
            st.session_state.selected_db = sel_db
            with st.spinner("Fetching tables..."):
                st.session_state.tables_list = fetch_table_list(connector, config, sel_db)
        
    # Table Dropdown
    sel_table = None
    if st.session_state.tables_list:
        sel_table = st.sidebar.selectbox("Select Table", st.session_state.tables_list)

    if sel_table and st.sidebar.button("🚀 Load Data", type="primary"):
        with st.spinner("Loading rows..."):
            st.session_state.raw_df = fetch_raw_data(connector, config, st.session_state.selected_db, sel_table)
            if not st.session_state.raw_df.empty:
                st.success("Data Loaded!")
            else:
                st.error("Table is empty or failed to load.")

# --- MAIN: REPORT GENERATION ---

if not st.session_state.raw_df.empty:
    df = st.session_state.raw_df
    st.divider()
    st.subheader("2. Configure Report")
    
    cols = df.columns.tolist()
    
    # Defaults helpers
    def get_idx(options, search):
        found = [i for i, o in enumerate(options) if search.lower() in o.lower()]
        return found[0] if found else 0
        
    c1, c2, c3, c4 = st.columns(4)
    with c1: date_col = st.selectbox("Date Column", cols, index=get_idx(cols, "date"))
    with c2: spend_col = st.selectbox("Spend Column", cols, index=get_idx(cols, "spend"))
    with c3: rev_col = st.selectbox("Revenue Column", cols, index=get_idx(cols, "rev"))
    with c4: bud_col = st.selectbox("Budget Column", cols, index=get_idx(cols, "budget"))
    
    # Optional filtering
    camp_col = st.selectbox("Campaign Name (Optional)", ["(None)"] + cols)
    
    st.subheader("3. Select Period")
    cd1, cd2 = st.columns(2)
    s_date = cd1.date_input("Start Date", value=date(2024,1,1))
    e_date = cd2.date_input("End Date", value=date(2024,1,31))
    
    if st.button("Generate Dashboard"):
        # 1. Filter Data
        mask = (pd.to_datetime(df[date_col], errors='coerce').dt.date >= s_date) & (pd.to_datetime(df[date_col], errors='coerce').dt.date <= e_date)
        df_filtered = df.loc[mask].copy()
        
        if df_filtered.empty:
            st.warning("No data found for selected dates.")
        else:
            col_map = {"date": date_col, "spend": spend_col, "revenue": rev_col, "budget": bud_col}
            
            # 2. Process Data (Function handles type safety)
            summary_kpi = calculate_summary_kpis(df_filtered, col_map)
            daily_data = process_granular_data(df_filtered, col_map, freq='D')
            
            # Only proceed if daily processing worked
            if not daily_data.empty:
                weekly_data = process_granular_data(df_filtered, col_map, freq='W')
                monthly_data = process_granular_data(df_filtered, col_map, freq='M')
                
                # 3. Save to Session
                st.session_state.summary_kpi = summary_kpi
                st.session_state.daily_data = daily_data
                st.session_state.weekly_data = weekly_data
                st.session_state.monthly_data = monthly_data
                
                # 4. Generate Excel
                dfs_dict = {'Daily': daily_data, 'Weekly': weekly_data, 'Monthly': monthly_data}
                st.session_state.excel_bytes = generate_excel_report(summary_kpi, dfs_dict)
                st.success("Report Generated!")

    # --- DISPLAY & EMAIL ---
    if "daily_data" in st.session_state and not st.session_state.daily_data.empty:
        st.divider()
        t1, t2, t3, t4 = st.tabs(["Summary", "Daily", "Weekly", "Monthly"])
        with t1: st.dataframe(st.session_state.summary_kpi, use_container_width=True)
        with t2: 
            st.dataframe(st.session_state.daily_data, use_container_width=True)
            for i in get_dynamic_insights(st.session_state.daily_data, "Day"): st.caption(f"• {i}")
        with t3: st.dataframe(st.session_state.weekly_data, use_container_width=True)
        with t4: st.dataframe(st.session_state.monthly_data, use_container_width=True)
        
        st.divider()
        c_down, c_mail = st.columns([1, 2])
        
        with c_down:
            st.download_button("Download Excel Report", st.session_state.excel_bytes, "pacing.xlsx")
            
        with c_mail:
            with st.form("email_form"):
                rec_email = st.text_input("Recipient Email")
                if st.form_submit_button("Send Email"):
                    html_body = f"""
                    <h2>Pacing Report</h2>
                    <p>Attached is the report for {s_date} to {e_date}.</p>
                    <ul>
                        <li>Total Spend: {st.session_state.summary_kpi.iloc[0]['Value']}</li>
                        <li>ROAS: {st.session_state.summary_kpi.iloc[2]['Value']}</li>
                    </ul>
                    """
                    success, msg = send_email_with_attachment(rec_email, "Pacing Report", html_body, st.session_state.excel_bytes)
                    if success: st.success(msg)
                    else: st.error(msg)