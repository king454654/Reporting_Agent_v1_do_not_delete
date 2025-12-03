import os
import pandas as pd
import numpy as np
import streamlit as st
from databricks import sql
from io import BytesIO
from datetime import datetime, timedelta
import xlsxwriter 

# Email Imports
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# ---------- Config ----------
def get_secret(name: str) -> str:
    return st.secrets.get(name) or os.getenv(name) or ""

DATABRICKS_SERVER_HOST = get_secret("DATABRICKS_SERVER_HOST")
DATABRICKS_HTTP_PATH = get_secret("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = get_secret("DATABRICKS_ACCESS_TOKEN")
WORKSPACE_NAME = get_secret("WORKSPACE_NAME") or None 

# ---------- Low-level query helper ----------
def run_query_raw(query: str, use_catalog: str = None, use_schema: str = None):
    try:
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
                        target = f"{use_catalog}.{use_schema}" if (use_catalog and use_catalog != "None") else use_schema
                        cur.execute(f"USE SCHEMA {target}")
                    except Exception: 
                        try: cur.execute(f"USE {use_schema}")
                        except Exception: pass
                
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
        return rows, cols
    except Exception as e:
        st.sidebar.error(f"⚠️ Query Error: {e}")
        return [], []

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
    try:
        table_path = get_sql_path(workspace, database, table)
        q = f"SELECT {agg_type}({col_def}) FROM {table_path}"
        rows, _ = run_query_raw(q, use_catalog=workspace, use_schema=database)
        return rows[0][0] if rows and rows[0][0] is not None else 0
    except:
        return 0

# ---------- Caching Helpers ----------
@st.cache_data(ttl=300)
def cached_list_databases(workspace: str):
    try:
        if workspace:
            rows, cols = run_query_raw("SHOW SCHEMAS", use_catalog=workspace)
        else:
            rows, cols = run_query_raw("SHOW SCHEMAS")
    except Exception:
        rows, cols = run_query_raw("SHOW SCHEMAS")
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

@st.cache_data(ttl=300)
def cached_list_table_names(workspace: str, database: str):
    try:
        if workspace:
            rows, cols = run_query_raw(f"SHOW TABLES IN {database}", use_catalog=workspace, use_schema=database)
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
    try:
        table_path = get_sql_path(workspace, database, table)
        rows, cols = run_query_raw(f"DESCRIBE TABLE {table_path}", use_catalog=workspace, use_schema=database)
    except Exception:
        try:
            rows, cols = run_query_raw(f"DESCRIBE {database}.{table}", use_catalog=workspace, use_schema=database)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

# ---------- Data Fetching ----------

def fetch_filtered_data(workspace, database, table, col_map, start_dt, end_dt, limit=500000):
    raw_cols = list(col_map.values())
    needed_cols = list(set([c for c in raw_cols if c != "(None)"]))
    selected_cols_str = ", ".join(needed_cols)
    date_col = col_map['date']

    table_path = get_sql_path(workspace, database, table)
    where_clause = f"WHERE {date_col} >= '{start_dt}' AND {date_col} <= '{end_dt}'"
    q = f"SELECT {selected_cols_str} FROM {table_path} {where_clause} LIMIT {limit}"
    
    rows, cols = run_query_raw(q, use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

# ---------- EXCEL GENERATION (WITH CHARTS) ----------

def generate_excel_report(raw_df, summary_kpi_df, report_dfs, col_map, global_budget_val) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # --- Formats ---
        title_fmt = workbook.add_format({'bold': True, 'font_size': 16, 'bg_color': "#FF6B15", 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        label_fmt = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'left', 'valign': 'vcenter'})
        value_fmt = workbook.add_format({'text_wrap': True, 'border': 1, 'valign': 'top', 'align': 'left'})
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1})
        label_currency_fmt = workbook.add_format({'num_format': '$ #,##0.00', 'bold': True, 'border': 1, 'bg_color': '#E2EFDA', 'font_color': '#385723', 'valign': 'vcenter'})

        # ==========================================
        # 1. CREATE COMBINED "SUMMARY" SHEET
        # ==========================================
        worksheet = workbook.add_worksheet('Summary')
        
        # ... (Same Summary Logic as before) ...
        def get_unique(col_key):
            actual_col = col_map.get(col_key)
            if actual_col and actual_col != "(None)" and actual_col in raw_df.columns:
                return ", ".join(raw_df[actual_col].dropna().unique().astype(str))
            return "N/A"

        campaign_val = get_unique('campaign')
        brand_val = get_unique('brand')
        platform_val = get_unique('platform')
        
        date_col = col_map['date']
        if date_col in raw_df.columns:
            dates = pd.to_datetime(raw_df[date_col], errors='coerce').dropna()
            period_val = f"{dates.min().strftime('%d-%b-%Y')} to {dates.max().strftime('%d-%b-%Y')}" if not dates.empty else "No Dates"
        else:
            period_val = "N/A"

        worksheet.set_column('A:A', 30)
        worksheet.set_column('B:B', 60)
        worksheet.merge_range('A1:B2', 'Campaign Pacing Dashboard', title_fmt)

        data_rows = [
            ('Campaign Name:', campaign_val),
            ('Brand:', brand_val),
            ('Platform:', platform_val),
            ('Objective:', 'Conversion(Purchases)'),
            ('Campaign Period:', period_val)
        ]

        row = 3
        for label, val in data_rows:
            worksheet.write(row, 0, label, label_fmt)
            worksheet.write(row, 1, val, value_fmt)
            row += 1
            
        worksheet.write(row, 0, 'Total Approved Budget:', label_fmt)
        worksheet.write(row, 1, global_budget_val, label_currency_fmt)
        
        start_row_kpi = row + 3 
        worksheet.write(start_row_kpi - 1, 0, "KPI Summary", label_fmt)
        
        summary_kpi_df.to_excel(writer, sheet_name='Summary', startrow=start_row_kpi, index=False)
        for col_num, value in enumerate(summary_kpi_df.columns.values):
            worksheet.write(start_row_kpi, col_num, value, header_fmt)

        # ==========================================
        # 2. CREATE GRANULAR SHEETS WITH CHARTS
        # ==========================================
        for sheet_name, df in report_dfs.items():
            if sheet_name == 'Summary': continue
            clean_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=clean_name, index=False)
            
            ws = writer.sheets[clean_name]
            # Column widths
            for idx, col in enumerate(df.columns):
                ws.set_column(idx, idx, 15)

            # --- ADDING CHARTS ---
            if not df.empty:
                max_row = len(df)
                # Helper to find column index (0-based)
                def get_col_idx(name, cols):
                    try: return [i for i, c in enumerate(cols) if name.lower() in c.lower()][0]
                    except: return -1

                col_idx_date = 0  # Assuming Date/Label is first
                col_idx_spend = get_col_idx("Spend", df.columns)
                col_idx_budget = get_col_idx("Budget", df.columns)
                col_idx_rev = get_col_idx("Revenue", df.columns)
                col_idx_roas = get_col_idx("ROAS", df.columns)

                # CHART 1: SPEND vs BUDGET (Clustered Column)
                if col_idx_spend >= 0 and col_idx_budget >= 0:
                    chart1 = workbook.add_chart({'type': 'column'})
                    # Budget Series
                    chart1.add_series({
                        'name':       [clean_name, 0, col_idx_budget],
                        'categories': [clean_name, 1, col_idx_date, max_row, col_idx_date],
                        'values':     [clean_name, 1, col_idx_budget, max_row, col_idx_budget],
                        'color':      '#D9D9D9' # Grey
                    })
                    # Spend Series
                    chart1.add_series({
                        'name':       [clean_name, 0, col_idx_spend],
                        'categories': [clean_name, 1, col_idx_date, max_row, col_idx_date],
                        'values':     [clean_name, 1, col_idx_spend, max_row, col_idx_spend],
                        'color':      '#FF6B15' # Brand Orange
                    })
                    chart1.set_title({'name': 'Budget vs Spend'})
                    chart1.set_size({'width': 600, 'height': 300})
                    ws.insert_chart('J2', chart1) # Place at column J

                # CHART 2: REVENUE vs ROAS (Line/Column Combo)
                if col_idx_rev >= 0 and col_idx_roas >= 0:
                    chart2 = workbook.add_chart({'type': 'column'})
                    
                    # Revenue (Columns)
                    chart2.add_series({
                        'name':       [clean_name, 0, col_idx_rev],
                        'categories': [clean_name, 1, col_idx_date, max_row, col_idx_date],
                        'values':     [clean_name, 1, col_idx_rev, max_row, col_idx_rev],
                        'color':      '#4472C4' # Blue
                    })
                    
                    # ROAS (Line on Secondary Axis) - Requires creating a separate chart object and combining
                    # For simplicity in 'xlsxwriter' basic, we'll do just Revenue trend here
                    # or strictly secondary axis logic:
                    
                    line_chart = workbook.add_chart({'type': 'line'})
                    line_chart.add_series({
                        'name':       [clean_name, 0, col_idx_roas],
                        'categories': [clean_name, 1, col_idx_date, max_row, col_idx_date],
                        'values':     [clean_name, 1, col_idx_roas, max_row, col_idx_roas],
                        'y2_axis':    True,
                        'color':      '#70AD47' # Green
                    })
                    
                    chart2.combine(line_chart)
                    chart2.set_title({'name': 'Revenue & ROAS Trend'})
                    chart2.set_y2_axis({'name': 'ROAS'})
                    chart2.set_size({'width': 600, 'height': 300})
                    
                    ws.insert_chart('J18', chart2) # Place below the first chart
                
    return output.getvalue()

# ---------- Email Functionality ----------
def send_email_with_attachment(recipient_email, excel_bytes, filename="pacing_report.xlsx"):
    smtp_server = get_secret("SMTP_SERVER")
    smtp_port_str = get_secret("SMTP_PORT")
    sender_email = get_secret("SENDER_EMAIL")
    sender_password = get_secret("SENDER_PASSWORD")
    
    if not all([smtp_server, sender_email, sender_password]):
        return False, "Missing SMTP configuration in secrets."

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
    
    daily_df = df.groupby(pd.Grouper(key=date_col, freq='D')).agg({
        spend_col: 'sum', rev_col: 'sum', budget_col: 'sum'
    }).reset_index().fillna(0)

    if freq == 'D':
        grouped = daily_df.copy()
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
        status = determine_status(pct_spent)
        
        if freq == 'W':
            end_d = d + timedelta(days=6)
            results.append({
                "Week Label": f"Week {week_counter}", "Period": f"{d.strftime('%d-%b')} to {end_d.strftime('%d-%b')}",
                "Budget": budget, "Spend": spend, "% Spent": f"{pct_spent}%", 
                "Revenue": rev, "ROAS": roas, "Status": status
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
st.set_page_config(page_title="Pacing Report", layout="wide", page_icon="📊")
st.title("📊 Automated Pacing Report")

# Initialization
if "workspace" not in st.session_state: st.session_state.workspace = WORKSPACE_NAME
if "databases" not in st.session_state: st.session_state.databases = []
if "tables" not in st.session_state: st.session_state.tables = []
if "selected_database" not in st.session_state: st.session_state.selected_database = None
if "selected_table" not in st.session_state: st.session_state.selected_table = None

# Sidebar Logic
def refresh_workspace():
    cached_list_databases.clear()
    cached_list_table_names.clear()
    
    ws = WORKSPACE_NAME or fetch_current_catalog()
    if ws: 
        st.session_state.workspace = ws
        st.sidebar.success(f"Connected to Catalog: {ws}")
    
    dbs = cached_list_databases(ws)
    if not dbs.empty:
        st.session_state.databases = dbs[dbs.columns[0]].astype(str).tolist()
        st.session_state.selected_database = st.session_state.databases[0]
    else:
        st.sidebar.warning("No databases found. Check permissions or catalog name.")

# Auto-load on startup
if not st.session_state.databases:
    refresh_workspace()

if st.sidebar.button("🔄 Refresh Workspace"): refresh_workspace()

st.sidebar.markdown(f"**Catalog:** `{st.session_state.workspace or 'Not Set'}`")

if st.session_state.databases:
    st.session_state.selected_database = st.sidebar.selectbox("Database", st.session_state.databases)
    if st.session_state.selected_database:
        tbls = cached_list_table_names(st.session_state.workspace, st.session_state.selected_database)
        st.session_state.tables = tbls["table_name"].astype(str).tolist() if not tbls.empty else []

if st.session_state.tables:
    st.session_state.selected_table = st.sidebar.selectbox("Table", st.session_state.tables)

# Main App
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
    
    def find_col(options, keyword):
        found = [o for o in options if keyword in o.lower()]
        return found[0] if found else options[0]
    
    def get_opt_index(opts, keyword):
        found = [i for i, o in enumerate(opts) if keyword in o.lower() and o != "(None)"]
        return found[0] if found else 0

    col_options_optional = ["(None)"] + col_names

    if col_names:
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: date_col = st.selectbox("Date Col", col_names, index=col_names.index(find_col(col_names, "date")))
        with c2: spend_col = st.selectbox("Spend Col", col_names, index=col_names.index(find_col(col_names, "spend")))
        with c3: budget_col = st.selectbox("Budget Col", col_names, index=col_names.index(find_col(col_names, "budget")) if find_col(col_names, "budget") else 0)
        with c4: rev_col = st.selectbox("Revenue Col", col_names, index=col_names.index(find_col(col_names, "rev")))
        with c5: conv_col = st.selectbox("Conv Col", col_names, index=col_names.index(find_col(col_names, "conv")))
        
        st.caption("File Summary Headers:")
        d1, d2, d3 = st.columns(3)
        with d1: camp_col = st.selectbox("Campaign Name", col_options_optional, index=get_opt_index(col_options_optional, "campaign"))
        with d2: brand_col = st.selectbox("Brand", col_options_optional, index=get_opt_index(col_options_optional, "brand"))
        with d3: plat_col = st.selectbox("Platform", col_options_optional, index=get_opt_index(col_options_optional, "platform"))

        st.subheader("2. Select Reporting Period")
        c_d1, c_d2 = st.columns(2)
        start_date = c_d1.date_input("Start Date")
        end_date = c_d2.date_input("End Date")

        if st.button("Generate Reports", type="primary"):
            if start_date and end_date:
                with st.spinner("Processing..."):
                    try:
                        col_map = {
                            "date": date_col, "spend": spend_col, "revenue": rev_col, 
                            "conversion": conv_col, "budget": budget_col,
                            "campaign": camp_col, "brand": brand_col, "platform": plat_col
                        }
                        
                        df_filtered = fetch_filtered_data(ws, db, tbl, col_map, start_date, end_date)
                        
                        if df_filtered.empty:
                            st.warning("No data found.")
                            st.session_state.excel_bytes = None
                        else:
                            for c in [spend_col, rev_col, conv_col, budget_col]:
                                df_filtered[c] = pd.to_numeric(df_filtered[c], errors='coerce').fillna(0)

                            # --- CHANGED: Use SPEND COLUMN from WHOLE DATA for Global Budget ---
                            total_budget_global = run_aggregation_query(ws, db, tbl, spend_col, "SUM") 
                            
                            # Count unique days
                            total_days_global = run_aggregation_query(ws, db, tbl, f"DISTINCT {date_col}", "COUNT") 
                            
                            summary_df = calculate_summary_kpis(df_filtered, total_budget_global, total_days_global, col_map)
                            daily_df = process_granular_data(df_filtered, col_map, freq='D')
                            weekly_df = process_granular_data(df_filtered, col_map, freq='W')
                            monthly_df = process_granular_data(df_filtered, col_map, freq='M')

                            st.session_state.summary_df = summary_df
                            st.session_state.daily_df = daily_df
                            st.session_state.weekly_df = weekly_df
                            st.session_state.monthly_df = monthly_df
                            
                            # Excel Generation
                            dfs_to_save = {'Summary': summary_df, 'Daily': daily_df, 'Weekly': weekly_df, 'Monthly': monthly_df}
                            st.session_state.excel_bytes = generate_excel_report(df_filtered, summary_df, dfs_to_save, col_map, total_budget_global)
                            st.success("Reports generated successfully!")

                    except Exception as e:
                        st.error(f"Error: {e}")
            else:
                st.error("Select dates.")

        if st.session_state.excel_bytes:
            st.divider()
            t1, t2, t3, t4 = st.tabs(["Summary", "Daily", "Weekly", "Monthly"])
            with t1: st.dataframe(st.session_state.summary_df, use_container_width=True)
            with t2: st.dataframe(st.session_state.daily_df, use_container_width=True)
            with t3: st.dataframe(st.session_state.weekly_df, use_container_width=True)
            with t4: st.dataframe(st.session_state.monthly_df, use_container_width=True)
            
            st.divider()
            c1, c2 = st.columns([1, 2])
            with c1:
                st.download_button("📥 Download Excel", st.session_state.excel_bytes, "pacing.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with c2:
                with st.form("email"):
                    rec = st.text_input("Recipient")
                    if st.form_submit_button("Send Email") and rec:
                        s, m = send_email_with_attachment(rec, st.session_state.excel_bytes)
                        if s: st.success(m)
                        else: st.error(m)