import os
import pandas as pd
import numpy as np
import streamlit as st
from io import BytesIO
from datetime import datetime, timedelta, date
import xlsxwriter
import json
import time
import re

# Email Imports
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# --- CONNECTOR LIBRARY IMPORTS ---
try:
    from databricks import sql as databricks_sql
    HAS_DATABRICKS = True
except ImportError:
    databricks_sql = None
    HAS_DATABRICKS = False

try:
    from google.cloud import bigquery
    from google.oauth2 import service_account
    HAS_BIGQUERY = True
except ImportError:
    bigquery = None
    HAS_BIGQUERY = False

try:
    import redshift_connector
    HAS_REDSHIFT = True
except ImportError:
    redshift_connector = None
    HAS_REDSHIFT = False

try:
    import boto3
    from botocore.exceptions import NoCredentialsError
    HAS_S3 = True
except ImportError:
    boto3 = None
    HAS_S3 = False

# ---------- Config & Secrets Helper ----------
def get_secret(name: str) -> str:
    if name in st.session_state.get('dynamic_creds', {}):
        return st.session_state['dynamic_creds'][name]
    return st.secrets.get(name) or os.getenv(name) or ""

LOGO_FILENAME = "logo.png"

# ==========================================
# 1. CONNECTOR ABSTRACTION LAYER
# ==========================================

class BaseConnector:
    """Template for all database connectors"""
    def connect(self, creds): pass
    def run_query(self, query): pass
    def get_schemas(self): pass
    def get_tables(self, schema): pass
    def describe_table(self, schema, table): pass
    def safe_id(self, identifier): return f"`{identifier}`"

# --- DATABRICKS CONNECTOR ---
class DatabricksConnector(BaseConnector):
    def connect(self, creds):
        if not HAS_DATABRICKS: raise ImportError("databricks-sql-connector is not installed.")
        self.host = creds.get("host")
        self.http_path = creds.get("http_path")
        self.token = creds.get("token")
        self.catalog = creds.get("catalog") 
        with databricks_sql.connect(server_hostname=self.host, http_path=self.http_path, access_token=self.token) as conn: pass

    def run_query(self, query):
        if not HAS_DATABRICKS: return [], []
        try:
            with databricks_sql.connect(server_hostname=self.host, http_path=self.http_path, access_token=self.token) as conn:
                with conn.cursor() as cur:
                    if self.catalog:
                        try: cur.execute(f"USE CATALOG {self.safe_id(self.catalog)}")
                        except: pass
                    cur.execute(query)
                    rows = cur.fetchall()
                    cols = [d[0] for d in cur.description] if cur.description else []
            return rows, cols
        except Exception as e:
            raise e

    def get_schemas(self):
        rows, cols = self.run_query("SHOW SCHEMAS")
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

    def get_tables(self, schema):
        rows, cols = self.run_query(f"SHOW TABLES IN {self.safe_id(schema)}")
        if not rows: return pd.DataFrame()
        df = pd.DataFrame(rows, columns=cols)
        possible = [c for c in df.columns if c.lower() in ("table_name", "tablename", "name")]
        name_col = possible[0] if possible else df.columns[0]
        return df[[name_col]].rename(columns={name_col: "table_name"})

    def describe_table(self, schema, table):
        rows, cols = self.run_query(f"DESCRIBE TABLE {self.safe_id(schema)}.{self.safe_id(table)}")
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

# --- BIGQUERY CONNECTOR ---
class BigQueryConnector(BaseConnector):
    def __init__(self):
        self.client = None
        self.project_id = None

    def connect(self, creds):
        if not HAS_BIGQUERY: raise ImportError("google-cloud-bigquery library not installed")
        service_account_info = creds.get("service_account_json")
        self.project_id = creds.get("project_id")
        if service_account_info:
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
        else:
            self.client = bigquery.Client(project=self.project_id)

    def run_query(self, query):
        if not self.client: return [], []
        query_job = self.client.query(query)
        result = query_job.result()
        rows = [list(row.values()) for row in result]
        cols = [field.name for field in result.schema] if result.schema else []
        return rows, cols

    def get_schemas(self):
        datasets = list(self.client.list_datasets())
        rows = [[d.dataset_id] for d in datasets]
        return pd.DataFrame(rows, columns=["database_name"])

    def get_tables(self, schema):
        tables = list(self.client.list_tables(schema))
        rows = [[t.table_id] for t in tables]
        return pd.DataFrame(rows, columns=["table_name"])

    def describe_table(self, schema, table):
        table_ref = self.client.dataset(schema).table(table)
        table_obj = self.client.get_table(table_ref)
        rows = [[s.name, s.field_type] for s in table_obj.schema]
        return pd.DataFrame(rows, columns=["col_name", "data_type"])

# --- REDSHIFT CONNECTOR ---
class RedshiftConnector(BaseConnector):
    def connect(self, creds):
        if not HAS_REDSHIFT: raise ImportError("redshift_connector not installed")
        self.conn_params = {
            'host': creds.get('host'), 'database': creds.get('databasen'),
            'user': creds.get('user'), 'password': creds.get('password'),
            'port': int(creds.get('port', 5439))
        }
        with redshift_connector.connect(**self.conn_params) as conn: pass

    def safe_id(self, identifier): return f'"{identifier}"'

    def run_query(self, query):
        with redshift_connector.connect(**self.conn_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                if cursor.description:
                    rows = cursor.fetchall()
                    cols = [d[0] for d in cursor.description]
                    return rows, cols
                return [], []

    def get_schemas(self):
        q = "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema'"
        rows, cols = self.run_query(q)
        return pd.DataFrame(rows, columns=cols)

    def get_tables(self, schema):
        q = f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '{schema}'"
        rows, cols = self.run_query(q)
        return pd.DataFrame(rows, columns=['table_name'])

    def describe_table(self, schema, table):
        q = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}'"
        rows, cols = self.run_query(q)
        return pd.DataFrame(rows, columns=cols)

# --- S3 CONNECTOR (REGEX FIXED) ---
class S3Connector(BaseConnector):
    def __init__(self):
        self.s3_client = None
        self.aws_access_key = None
        self.aws_secret_key = None
        self.region = None

    def connect(self, creds):
        if not HAS_S3: raise ImportError("boto3 is not installed.")
        self.aws_access_key = creds.get('aws_access_key')
        self.aws_secret_key = creds.get('aws_secret_key')
        self.region = creds.get('region', 'us-east-1')
        
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region
        )
        self.s3_client = session.client('s3')
        self.s3_client.list_buckets()

    def safe_id(self, identifier):
        # CRITICAL: Wrap keys in backticks to handle spaces/dots in Regex
        return f"`{identifier}`"

    def _get_file_extension(self, key):
        if key.lower().endswith('.csv'): return 'csv'
        if key.lower().endswith('.xlsx'): return 'xlsx'
        if key.lower().endswith('.xls'): return 'xls'
        if key.lower().endswith('.parquet'): return 'parquet'
        if key.lower().endswith('.json'): return 'json'
        return None

    def _load_data_into_df(self, bucket, key):
        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
            body = obj['Body'].read()
            file_stream = BytesIO(body)
            ext = self._get_file_extension(key)
            
            if ext == 'csv': return pd.read_csv(file_stream)
            elif ext in ['xlsx', 'xls']: return pd.read_excel(file_stream, engine='openpyxl')
            elif ext == 'parquet': return pd.read_parquet(file_stream, engine='pyarrow')
            elif ext == 'json': return pd.read_json(file_stream)
            else: return pd.DataFrame()
        except Exception as e:
            st.error(f"S3 Read Error: {str(e)}")
            return pd.DataFrame()

    def run_query(self, query):
        try:
            # 1. PARSE BUCKET & KEY WITH REGEX
            # This captures: FROM `bucket`.`path/to/my file.csv`
            # Ignores dots and spaces inside the backticks.
            match = re.search(r"FROM\s+`([^`]+)`\.`([^`]+)`", query, re.IGNORECASE)
            
            if not match:
                st.error("Query Parse Error. S3 Connector expects backticks.")
                return [], []
            
            bucket = match.group(1)
            key = match.group(2) # Correctly captures full key with spaces

            # Load Data
            df = self._load_data_into_df(bucket, key)
            if df.empty: return [], []

            # 2. EMULATE SQL LOGIC
            clean_query = query.replace("`", "")

            # Aggregations
            if "SELECT SUM(" in clean_query.upper():
                col_match = re.search(r"SUM\((.*?)\)", clean_query, re.IGNORECASE)
                if col_match:
                    col = col_match.group(1).strip().replace('"', '').replace("'", "")
                    if col in df.columns:
                        val = pd.to_numeric(df[col], errors='coerce').sum()
                        return [[val]], ["sum"]
                    return [[0]], ["sum"]
            
            if "COUNT(DISTINCT" in clean_query.upper():
                col_match = re.search(r"DISTINCT\s+(.*?)\)", clean_query, re.IGNORECASE)
                if col_match:
                    col = col_match.group(1).strip().replace('"', '').replace("'", "")
                    if col in df.columns:
                        val = df[col].nunique()
                        return [[val]], ["count"]
                    return [[0]], ["count"]

            # Filtering (WHERE)
            if "WHERE" in clean_query.upper():
                where_part = clean_query.split("WHERE")[1]
                conditions = re.findall(r"(\w+)\s*(>=|<=)\s*'([^']+)'", where_part)
                for col, op, val in conditions:
                    col = col.strip()
                    if col in df.columns:
                        if not pd.api.types.is_datetime64_any_dtype(df[col]):
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        target_val = pd.to_datetime(val)
                        if op == ">=": df = df[df[col] >= target_val]
                        elif op == "<=": df = df[df[col] <= target_val]

            # Select Columns
            select_match = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.IGNORECASE)
            if select_match:
                select_part = select_match.group(1)
                if "*" not in select_part:
                    req_cols = [c.strip().replace('`', '').replace('"', '').replace("'", "") for c in select_part.split(",")]
                    valid_cols = [c for c in req_cols if c in df.columns]
                    if valid_cols: df = df[valid_cols]

            df = df.fillna(0)
            return df.values.tolist(), df.columns.tolist()

        except Exception as e:
            st.error(f"S3 Processing Error: {str(e)}")
            return [], []

    def get_schemas(self):
        try:
            response = self.s3_client.list_buckets()
            return pd.DataFrame([b['Name'] for b in response.get('Buckets', [])], columns=['database_name'])
        except: return pd.DataFrame()

    def get_tables(self, schema):
        try:
            response = self.s3_client.list_objects_v2(Bucket=schema)
            if 'Contents' not in response: return pd.DataFrame()
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].lower().endswith(('.csv', '.xlsx', '.parquet', '.json'))]
            return pd.DataFrame(files, columns=['table_name'])
        except: return pd.DataFrame()

    def describe_table(self, schema, table):
        try:
            df = self._load_data_into_df(schema, table)
            if df.empty: return pd.DataFrame()
            df_head = df.head(5)
            return pd.DataFrame([[c, str(df_head[c].dtype)] for c in df_head.columns], columns=["col_name", "data_type"])
        except: return pd.DataFrame()

# ==========================================
# 2. MAIN APP & LOGIC
# ==========================================

st.set_page_config(page_title="Pacing Report", layout="wide")

if "connector" not in st.session_state: st.session_state.connector = None
if "is_connected" not in st.session_state: st.session_state.is_connected = False
if "databases" not in st.session_state: st.session_state.databases = []
if "excel_bytes" not in st.session_state: st.session_state.excel_bytes = None
if "campaign_email_name" not in st.session_state: st.session_state.campaign_email_name = "General"
if "dynamic_creds" not in st.session_state: st.session_state.dynamic_creds = {}

if os.path.exists(LOGO_FILENAME):
    st.sidebar.image(LOGO_FILENAME, width=200)

st.title("Automated Pacing Report Multi-Connect")

# --- SIDEBAR: CONNECTION MANAGER ---
st.sidebar.title("1. Connection")
connector_type = st.sidebar.selectbox("Select Connector", ["Databricks", "BigQuery", "Redshift", "S3 Bucket"])

with st.sidebar.form("connection_form"):
    creds_input = {}

    if connector_type == "Databricks":
        def_host = st.secrets.get("DATABRICKS_SERVER_HOST", "")
        def_path = st.secrets.get("DATABRICKS_HTTP_PATH", "")
        def_token = st.secrets.get("DATABRICKS_ACCESS_TOKEN", "")
        creds_input['host'] = st.text_input("Server Hostname", value=def_host)
        creds_input['http_path'] = st.text_input("HTTP Path", value=def_path)
        creds_input['token'] = st.text_input("Access Token", value=def_token, type="password")
        creds_input['catalog'] = st.text_input("Catalog (Optional)", value=st.secrets.get("WORKSPACE_NAME", ""))

    elif connector_type == "BigQuery":
        st.info("Upload Service Account JSON")
        uploaded_file = st.file_uploader("Service Account JSON", type=['json'])
        creds_input['project_id'] = st.text_input("Project ID (Optional if in JSON)")
        if uploaded_file:
            creds_input['service_account_json'] = json.load(uploaded_file)
            if not creds_input['project_id']:
                creds_input['project_id'] = creds_input['service_account_json'].get('project_id')

    elif connector_type == "Redshift":
        creds_input['host'] = st.text_input("Host")
        creds_input['port'] = st.text_input("Port", value="5439")
        creds_input['database'] = st.text_input("Database Name")
        creds_input['user'] = st.text_input("User")
        creds_input['password'] = st.text_input("Password", type="password")
    
    elif connector_type == "S3 Bucket":
        st.info("Reads CSV, Excel, Parquet from S3")
        creds_input['aws_access_key'] = st.text_input("AWS Access Key ID")
        creds_input['aws_secret_key'] = st.text_input("AWS Secret Access Key", type="password")
        creds_input['region'] = st.text_input("Region", value="us-east-1")

    connect_btn = st.form_submit_button("Connect & Refresh")

if connect_btn:
    try:
        conn = None
        if connector_type == "Databricks": conn = DatabricksConnector()
        elif connector_type == "BigQuery": conn = BigQueryConnector()
        elif connector_type == "Redshift": conn = RedshiftConnector()
        elif connector_type == "S3 Bucket": conn = S3Connector()
        
        conn.connect(creds_input)
        st.session_state.connector = conn
        st.session_state.is_connected = True
        st.session_state.dynamic_creds = creds_input
        st.sidebar.success(f"Connected to {connector_type}!")
        
        dbs_df = st.session_state.connector.get_schemas()
        st.session_state.databases = dbs_df[dbs_df.columns[0]].astype(str).tolist() if not dbs_df.empty else []
            
    except Exception as e:
        st.session_state.is_connected = False
        st.sidebar.error(f"Connection Failed: {str(e)}")

# --- SIDEBAR: SCHEMA SELECTION ---
selected_database = None
selected_table = None

@st.cache_data(ttl=600)
def get_tables_cached(_connector, schema):
    return _connector.get_tables(schema)

@st.cache_data(ttl=600)
def describe_table_cached(_connector, schema, table):
    return _connector.describe_table(schema, table)

if st.session_state.is_connected and st.session_state.databases:
    st.sidebar.divider()
    label = "Select Bucket" if connector_type == "S3 Bucket" else "Select Schema/Dataset"
    selected_database = st.sidebar.selectbox(label, st.session_state.databases)
    
    if selected_database:
        tables_df = get_tables_cached(st.session_state.connector, selected_database)
        if not tables_df.empty:
            label_tbl = "Select File" if connector_type == "S3 Bucket" else "Select Table"
            table_list = tables_df['table_name'].astype(str).tolist()
            selected_table = st.sidebar.selectbox(label_tbl, table_list)
        else:
            st.sidebar.warning("No tables/files found.")

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

def run_aggregation_query(connector, database, table, col_def_sql, agg_type="SUM"):
    try:
        # safe_id ensures backticks for S3 regex compatibility
        table_path = f"{connector.safe_id(database)}.{connector.safe_id(table)}"
        q = f"SELECT {agg_type}({col_def_sql}) FROM {table_path}"
        rows, _ = connector.run_query(q)
        return rows[0][0] if rows and rows[0][0] is not None else 0
    except: return 0

@st.cache_data(ttl=300, show_spinner=False)
def fetch_filtered_data(_connector, database, table, col_map, start_dt, end_dt):
    raw_cols = list(col_map.values())
    needed_cols = list(set([c for c in raw_cols if c != "(None)"]))
    
    escaped_cols = [_connector.safe_id(c) for c in needed_cols]
    selected_cols_str = ", ".join(escaped_cols)
    
    date_col_safe = _connector.safe_id(col_map['date'])
    table_path = f"{_connector.safe_id(database)}.{_connector.safe_id(table)}"
    
    where_clause = f"WHERE {date_col_safe} >= '{start_dt}' AND {date_col_safe} <= '{end_dt}'"
    q = f"SELECT {selected_cols_str} FROM {table_path} {where_clause}"
    
    try:
        rows, cols = _connector.run_query(q)
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame()

# ---------- INSIGHTS & EXCEL ----------
def get_dynamic_insights(df, period_type):
    if df.empty: return ["No data available."]
    insights = []
    col_spend = 'Spend MTD' if 'Spend MTD' in df.columns else 'Spend'
    col_label = 'Month Label' if 'Month Label' in df.columns else ('Week Label' if 'Week Label' in df.columns else 'Date')
    
    if col_spend in df.columns:
        df[col_spend] = pd.to_numeric(df[col_spend], errors='coerce').fillna(0)
    if col_spend in df.columns and not df.empty:
        top_spend = df.loc[df[col_spend].idxmax()]
        insights.append(f"Highest Spend: {top_spend[col_label]} with ${top_spend[col_spend]:,.2f}.")
    return insights[:5]

def generate_excel_report(raw_df, summary_kpi_df, report_dfs, col_map, global_budget_val) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        title_fmt = workbook.add_format({'bold': True, 'font_size': 16, 'bg_color': "#FF6B15", 'font_color': 'white', 'align': 'center', 'border': 1})
        ws = workbook.add_worksheet('Summary')
        ws.merge_range('A1:B2', 'Campaign Pacing Dashboard', title_fmt)
        summary_kpi_df.to_excel(writer, sheet_name='Summary', startrow=6, index=False)
        for sheet_name, df in report_dfs.items():
            if sheet_name == 'Summary': continue
            clean_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=clean_name, index=False)
    return output.getvalue()

def process_granular_data(df, cols_map, freq='W'):
    df = df.copy()
    date_col = cols_map['date']
    spend_col = cols_map['spend']
    rev_col = cols_map['revenue']
    budget_col = cols_map['budget']
    
    df[date_col] = pd.to_datetime(df[date_col])
    daily_df = df.groupby(pd.Grouper(key=date_col, freq='D')).agg({
        spend_col: 'sum', rev_col: 'sum', budget_col: 'sum'
    }).reset_index().fillna(0)

    if freq == 'D': grouped = daily_df.copy()
    else:
        resample_rule = 'W-MON' if freq == 'W' else 'MS'
        grouped = daily_df.set_index(date_col).resample(resample_rule, closed='left', label='left').agg({
            spend_col: 'sum', rev_col: 'sum', budget_col: 'sum'
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
        
        status = "On Track"
        if pct_spent < 95: status = "Under Pacing"
        elif pct_spent > 105: status = "Over Pacing"
        
        if freq == 'W':
            end_d = d + timedelta(days=6)
            results.append({
                "Week Label": f"Week {week_counter}", "Period": f"{d.strftime('%d-%b')} to {end_d.strftime('%d-%b')}",
                "Budget": budget, "Spend": spend, "% Spent": f"{pct_spent}%", "Revenue": rev, "ROAS": roas, "Status": status
            })
            week_counter += 1
        elif freq == 'M':
            results.append({
                "Month Label": d.strftime('%b %Y'), "Budget": budget, "Spend MTD": spend,
                "Revenue MTD": rev, "ROAS": roas, "% Spent": f"{pct_spent}%", "Status": status
            })
        elif freq == 'D':
            results.append({
                "Date": d.strftime('%Y-%m-%d'), "Day": d.strftime('%A'), "Budget": budget, "Spend": spend,
                "% Spent": f"{pct_spent}%", "Revenue": rev, "ROAS": roas, "Status": status
            })
    return pd.DataFrame(results)

def calculate_summary_kpis(df_filtered, total_budget_global, total_days_global, cols_map):
    spend_col, rev_col, conv_col, date_col = cols_map['spend'], cols_map['revenue'], cols_map['conversion'], cols_map['date']
    for c in [spend_col, rev_col, conv_col]:
        df_filtered[c] = pd.to_numeric(df_filtered[c], errors='coerce').fillna(0)

    Total_Spend_Till_Date = df_filtered[spend_col].sum()
    Total_Revenue_Till_Date = df_filtered[rev_col].sum()
    total_conv = df_filtered[conv_col].sum()
    roas = round(Total_Revenue_Till_Date / Total_Spend_Till_Date, 2) if Total_Spend_Till_Date > 0 else 0
    cpa = round((Total_Spend_Till_Date / total_conv), 2) if total_conv > 0 else 0
    Total_Approved_Budget = float(total_budget_global)
    Spend_Pacing = round((Total_Spend_Till_Date / Total_Approved_Budget) * 100, 2) if Total_Approved_Budget > 0 else 0
    
    Days_Elapsed = df_filtered[date_col].nunique()
    Days_Remaining = total_days_global - Days_Elapsed
    Expected_Pacing = round((Days_Elapsed / Days_Remaining) * 100, 2) if (Days_Remaining + Days_Elapsed) > 0 else 100.0
    Pacing_Variance = round(Spend_Pacing - Expected_Pacing, 2)
    Pacing_Status = "UNDER PACING" if Spend_Pacing < Expected_Pacing else "PACING WELL"
    
    data = {
        "Metric": ["Days Elapsed", "Days Remaining", "Total Spend Till Date", "Total Revenue Till Date", "Current ROAS", "Current CPA", "Spend Pacing % (Actual)", "Expected Pacing %", "Pacing Variance", "Pacing Status", "Remaining Budget"],
        "Value": [f"{int(Days_Elapsed)} Days", f"{int(Days_Remaining)} Days", f"${Total_Spend_Till_Date:,.2f}", f"${Total_Revenue_Till_Date:,.2f}", f"{roas}x", f"${cpa}", f"{Spend_Pacing}%", f"{Expected_Pacing}%", f"{Pacing_Variance}%", Pacing_Status, f"${(Total_Approved_Budget - Total_Spend_Till_Date):,.2f}"]
    }
    return pd.DataFrame(data)

def send_email_with_attachment(recipient_email, subject, body_html, excel_bytes, filename="pacing_report.xlsx"):
    smtp_server = get_secret("SMTP_SERVER")
    sender_email = get_secret("SENDER_EMAIL")
    sender_password = get_secret("SENDER_PASSWORD")
    if not all([smtp_server, sender_email, sender_password]): return False, "Missing SMTP secrets."
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject 
        msg.attach(MIMEText(body_html, 'html'))
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(excel_bytes)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        server = smtplib.SMTP(smtp_server, int(get_secret("SMTP_PORT") or 587))
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        return True, "Email sent!"
    except Exception as e: return False, str(e)

# ==========================================
# 4. APP WORKFLOW
# ==========================================

if selected_table and st.session_state.is_connected:
    connector = st.session_state.connector
    schema_df = describe_table_cached(connector, selected_database, selected_table)
    col_names = []
    if not schema_df.empty:
        possible_cols = [c for c in schema_df.columns if 'col' in c.lower()]
        col_names = schema_df[possible_cols[0] if possible_cols else schema_df.columns[0]].tolist()

    st.subheader("2. Map Data Columns")
    def find_col(options, keyword):
        found = [o for o in options if keyword in o.lower()]
        return found[0] if found else options[0]
    
    col_options_optional = ["(None)"] + col_names

    if col_names:
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: date_col = st.selectbox("Date Col", col_names, index=col_names.index(find_col(col_names, "date")))
        with c2: spend_col = st.selectbox("Spend Col", col_names, index=col_names.index(find_col(col_names, "spend")))
        with c3: budget_col = st.selectbox("Budget Col", col_names, index=col_names.index(find_col(col_names, "budget")) if find_col(col_names, "budget") else 0)
        with c4: rev_col = st.selectbox("Revenue Col", col_names, index=col_names.index(find_col(col_names, "rev")))
        with c5: conv_col = st.selectbox("Conv Col", col_names, index=col_names.index(find_col(col_names, "conv")))
        
        st.caption("Optional Headers:")
        d1, d2, d3 = st.columns(3)
        with d1: camp_col = st.selectbox("Campaign Name", col_options_optional)
        with d2: brand_col = st.selectbox("Brand", col_options_optional)
        with d3: plat_col = st.selectbox("Platform", col_options_optional)

        st.subheader("3. Select Reporting Period")
        c_d1, c_d2 = st.columns(2)
        start_date = c_d1.date_input("Start Date",value=date(2024,1,1))
        end_date = c_d2.date_input("End Date",value=date(2024,3,31))

        if st.button("Generate Reports", type="primary"):
            if start_date and end_date:
                with st.spinner("Processing..."):
                    try:
                        col_map = {"date": date_col, "spend": spend_col, "revenue": rev_col, "conversion": conv_col, "budget": budget_col, "campaign": camp_col, "brand": brand_col, "platform": plat_col}
                        df_filtered = fetch_filtered_data(connector, selected_database, selected_table, col_map, start_date, end_date)
                        
                        if df_filtered.empty:
                            st.warning("No data found.")
                            st.session_state.excel_bytes = None
                        else:
                            camp_context = "General"
                            if camp_col and camp_col != "(None)" and camp_col in df_filtered.columns:
                                unique_camps = df_filtered[camp_col].dropna().unique()
                                if len(unique_camps) > 0: camp_context = str(unique_camps[0]) 
                            st.session_state.campaign_email_name = camp_context
                            
                            total_budget = run_aggregation_query(connector, selected_database, selected_table, connector.safe_id(budget_col), "SUM") 
                            total_days = run_aggregation_query(connector, selected_database, selected_table, f"DISTINCT {connector.safe_id(date_col)}", "COUNT") 
                            
                            summary_df = calculate_summary_kpis(df_filtered, total_budget, total_days, col_map)
                            daily_df = process_granular_data(df_filtered, col_map, freq='D')
                            weekly_df = process_granular_data(df_filtered, col_map, freq='W')
                            monthly_df = process_granular_data(df_filtered, col_map, freq='M')

                            st.session_state.summary_df = summary_df
                            st.session_state.daily_df = daily_df
                            st.session_state.weekly_df = weekly_df
                            st.session_state.monthly_df = monthly_df
                            
                            dfs = {'Summary': summary_df, 'Daily': daily_df, 'Weekly': weekly_df, 'Monthly': monthly_df}
                            st.session_state.excel_bytes = generate_excel_report(df_filtered, summary_df, dfs, col_map, total_budget)
                            st.success("Reports generated successfully!")
                    except Exception as e: st.error(f"Error: {e}")

        if st.session_state.excel_bytes:
            st.divider()
            t1, t2, t3, t4 = st.tabs(["Summary", "Daily", "Weekly", "Monthly"])
            with t1: st.dataframe(st.session_state.summary_df, use_container_width=True)
            with t2: st.dataframe(st.session_state.daily_df, use_container_width=True)
            with t3: st.dataframe(st.session_state.weekly_df, use_container_width=True)
            with t4: st.dataframe(st.session_state.monthly_df, use_container_width=True)
            st.divider()
            c1, c2 = st.columns([1, 2])
            with c1: st.download_button("Download Excel", st.session_state.excel_bytes, "pacing.xlsx")
            with c2:
                with st.form("email_form"):
                    rec = st.text_input("Recipient Email")
                    if st.form_submit_button("Send Email") and rec:
                        s, m = send_email_with_attachment(rec, f"{st.session_state.campaign_email_name} Report", "<h1>Report Attached</h1>", st.session_state.excel_bytes)
                        if s: st.success(m)
                        else: st.error(m)

elif not st.session_state.is_connected:
    st.info("Please select a Connector and Login in the Sidebar.")