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
LOGO_FILENAME = "logo.png" 

# ---------- SQL Helper Functions ----------
def safe_identifier(col_name):
    """Wraps column names in backticks to handle spaces and special chars"""
    if not col_name or col_name == "(None)":
        return col_name
    return f"`{col_name}`"

def run_query_raw(query: str, use_catalog: str = None, use_schema: str = None):
    try:
        with sql.connect(
            server_hostname=DATABRICKS_SERVER_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        ) as conn:
            with conn.cursor() as cur:
                if use_catalog and use_catalog != "None":
                    try: cur.execute(f"USE CATALOG {safe_identifier(use_catalog)}")
                    except Exception: pass
                
                if use_schema:
                    try: 
                        target = f"{safe_identifier(use_catalog)}.{safe_identifier(use_schema)}" if (use_catalog and use_catalog != "None") else safe_identifier(use_schema)
                        cur.execute(f"USE SCHEMA {target}")
                    except Exception: 
                        try: cur.execute(f"USE {safe_identifier(use_schema)}")
                        except Exception: pass
                
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
        return rows, cols
    except Exception as e:
        st.sidebar.error(f"Query Error: {e}")
        return [], []

def get_sql_path(workspace, database, table):
    if workspace and workspace != "None":
        return f"{safe_identifier(workspace)}.{safe_identifier(database)}.{safe_identifier(table)}"
    return f"{safe_identifier(database)}.{safe_identifier(table)}"

def fetch_current_catalog():
    try:
        rows, _ = run_query_raw("SELECT current_catalog()")
        return rows[0][0] if rows else None
    except Exception:
        return None

def run_aggregation_query(workspace, database, table, col_def_sql, agg_type="SUM"):
    try:
        table_path = get_sql_path(workspace, database, table)
        q = f"SELECT {agg_type}({col_def_sql}) FROM {table_path}"
        rows, _ = run_query_raw(q, use_catalog=workspace, use_schema=database)
        return rows[0][0] if rows and rows[0][0] is not None else 0
    except:
        return 0

# ---------- Caching Helpers ----------
@st.cache_data(ttl=300)
def cached_list_databases(workspace: str):
    try:
        rows, cols = run_query_raw("SHOW SCHEMAS", use_catalog=workspace)
    except Exception:
        rows, cols = run_query_raw("SHOW SCHEMAS")
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

@st.cache_data(ttl=300)
def cached_list_table_names(workspace: str, database: str):
    try:
        if workspace:
            rows, cols = run_query_raw(f"SHOW TABLES IN {safe_identifier(database)}", use_catalog=workspace, use_schema=database)
        else:
            rows, cols = run_query_raw(f"SHOW TABLES IN {safe_identifier(database)}", use_schema=database)
    except Exception:
        rows, cols = run_query_raw(f"SHOW TABLES IN {safe_identifier(database)}", use_schema=database)
        
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
            rows, cols = run_query_raw(f"DESCRIBE {safe_identifier(database)}.{safe_identifier(table)}", use_catalog=workspace, use_schema=database)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

# ---------- Data Fetching ----------
def fetch_filtered_data(workspace, database, table, col_map, start_dt, end_dt, limit=500000):
    raw_cols = list(col_map.values())
    needed_cols = list(set([c for c in raw_cols if c != "(None)"]))
    
    escaped_cols = [f"`{c}`" for c in needed_cols]
    selected_cols_str = ", ".join(escaped_cols)
    
    date_col = col_map['date']
    table_path = get_sql_path(workspace, database, table)
    
    where_clause = f"WHERE `{date_col}` >= '{start_dt}' AND `{date_col}` <= '{end_dt}'"
    q = f"SELECT {selected_cols_str} FROM {table_path} {where_clause} LIMIT {limit}"
    
    rows, cols = run_query_raw(q, use_catalog=workspace, use_schema=database)
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()

# ---------- INSIGHT GENERATION LOGIC ----------
def get_dynamic_insights(df, period_type):
    if df.empty:
        return ["No data available to generate insights."]
    
    insights = []
    col_spend = 'Spend MTD' if 'Spend MTD' in df.columns else 'Spend'
    col_label = 'Month Label' if 'Month Label' in df.columns else ('Week Label' if 'Week Label' in df.columns else 'Date')

    if 'ROAS' in df.columns and df['ROAS'].sum() > 0:
        top_roas = df.loc[df['ROAS'].idxmax()]
        insights.append(f"Best Efficiency: {top_roas[col_label]} had the highest ROAS of {top_roas['ROAS']}x.")

    if col_spend in df.columns:
        top_spend = df.loc[df[col_spend].idxmax()]
        insights.append(f"Highest Spend: {top_spend[col_label]} with ${top_spend[col_spend]:,.2f}.")

    if 'ROAS' in df.columns:
        avg_roas = df['ROAS'].mean()
        insights.append(f"Average ROAS for this period is {avg_roas:.2f}x.")

    if len(df) > 1 and col_spend in df.columns:
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        diff = last_row[col_spend] - prev_row[col_spend]
        direction = "increased" if diff > 0 else "decreased"
        insights.append(f"Trend: Spend {direction} by ${abs(diff):,.2f} compared to the previous {period_type.lower()}.")

    if 'ROAS' in df.columns:
        low_perf = df[df['ROAS'] < 1.0]
        if not low_perf.empty:
            count = len(low_perf)
            insights.append(f"Warning: {count} {period_type.lower()}(s) had a ROAS below 1.0.")
        else:
            insights.append("Stability: No periods dropped below 1.0 ROAS.")

    return insights[:5]

# ---------- EXCEL GENERATION ----------
def generate_excel_report(raw_df, summary_kpi_df, report_dfs, col_map, global_budget_val) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Formats
        title_fmt = workbook.add_format({'bold': True, 'font_size': 16, 'bg_color': "#FF6B15", 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        label_fmt = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'left', 'valign': 'vcenter'})
        value_fmt = workbook.add_format({'text_wrap': True, 'border': 1, 'valign': 'top', 'align': 'left'})
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1})
        label_currency_fmt = workbook.add_format({'num_format': '$ #,##0.00', 'bold': True, 'border': 1, 'bg_color': '#E2EFDA', 'font_color': '#385723', 'valign': 'vcenter'})
        currency_data_fmt = workbook.add_format({'num_format': '$#,##0.00', 'text_wrap': True, 'valign': 'top', 'align': 'left'})
        insight_header_fmt = workbook.add_format({'bold': True, 'font_color': '#FF0000', 'font_size': 12, 'underline': True, 'align': 'left'})
        insight_text_fmt = workbook.add_format({'italic': True, 'font_color': '#333333', 'align': 'left', 'text_wrap': True})

        # Summary Sheet
        worksheet = workbook.add_worksheet('Summary')
        worksheet.hide_gridlines(2)
        
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

        # Granular Sheets
        for sheet_name, df in report_dfs.items():
            if sheet_name == 'Summary': continue
            clean_name = sheet_name[:31]
            df.to_excel(writer, sheet_name=clean_name, index=False)
            
            ws = writer.sheets[clean_name]
            ws.hide_gridlines(2)
            
            for idx, col in enumerate(df.columns):
                col_lower = col.lower()
                if any(x in col_lower for x in ['budget', 'spend', 'revenue', 'cpa']):
                    ws.set_column(idx, idx, 15, currency_data_fmt)
                else:
                    ws.set_column(idx, idx, 15)

            if not df.empty:
                max_row = len(df)
                period_type = "Day" if "Daily" in sheet_name else ("Week" if "Weekly" in sheet_name else "Month")
                sheet_insights = get_dynamic_insights(df, period_type)
                
                ws.set_column('K:K', 50) 
                ws.write('K2', f"Key Insights ({sheet_name}):", insight_header_fmt)
                
                for i, text in enumerate(sheet_insights):
                    ws.write(f'K{3+i}', f"• {text}", insight_text_fmt)

                # Charts
                def get_col_idx(name, cols):
                    try: return [i for i, c in enumerate(cols) if name.lower() in c.lower()][0]
                    except: return -1

                col_idx_date = 0 
                col_idx_spend = get_col_idx("Spend", df.columns)
                col_idx_budget = get_col_idx("Budget", df.columns)
                col_idx_rev = get_col_idx("Revenue", df.columns)
                col_idx_roas = get_col_idx("ROAS", df.columns)

                if col_idx_spend >= 0 and col_idx_budget >= 0:
                    chart1 = workbook.add_chart({'type': 'column'})
                    chart1.add_series({'name': [clean_name, 0, col_idx_budget], 'categories': [clean_name, 1, col_idx_date, max_row, col_idx_date], 'values': [clean_name, 1, col_idx_budget, max_row, col_idx_budget], 'color': '#D9D9D9'})
                    chart1.add_series({'name': [clean_name, 0, col_idx_spend], 'categories': [clean_name, 1, col_idx_date, max_row, col_idx_date], 'values': [clean_name, 1, col_idx_spend, max_row, col_idx_spend], 'color': '#FF6B15'})
                    chart1.set_title({'name': 'Budget vs Spend'})
                    chart1.set_size({'width': 600, 'height': 300})
                    ws.insert_chart('L2', chart1)

                if col_idx_rev >= 0 and col_idx_roas >= 0:
                    chart2 = workbook.add_chart({'type': 'column'})
                    chart2.add_series({'name': [clean_name, 0, col_idx_rev], 'categories': [clean_name, 1, col_idx_date, max_row, col_idx_date], 'values': [clean_name, 1, col_idx_rev, max_row, col_idx_rev], 'color': '#4472C4'})
                    line_chart = workbook.add_chart({'type': 'line'})
                    line_chart.add_series({'name': [clean_name, 0, col_idx_roas], 'categories': [clean_name, 1, col_idx_date, max_row, col_idx_date], 'values': [clean_name, 1, col_idx_roas, max_row, col_idx_roas], 'y2_axis': True, 'color': '#70AD47'})
                    chart2.combine(line_chart)
                    chart2.set_title({'name': 'Revenue & ROAS Trend'})
                    chart2.set_size({'width': 600, 'height': 300})
                    ws.insert_chart('L18', chart2)
                
    return output.getvalue()

# ---------- Email Functionality (HTML UPDATED) ----------
def send_email_with_attachment(recipient_email, subject, body_html, excel_bytes, filename="pacing_report.xlsx"):
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
        msg['Subject'] = subject 
        
        # CHANGED: 'plain' to 'html'
        msg.attach(MIMEText(body_html, 'html'))

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

# ---------- DATA PROCESSING LOGIC ----------
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
st.set_page_config(page_title="Pacing Report", layout="wide")

if os.path.exists(LOGO_FILENAME):
    st.sidebar.image(LOGO_FILENAME, width=200)

st.title("Automated Pacing Report")

# Initialization
if "workspace" not in st.session_state: st.session_state.workspace = WORKSPACE_NAME
if "databases" not in st.session_state: st.session_state.databases = []
if "tables" not in st.session_state: st.session_state.tables = []
if "selected_database" not in st.session_state: st.session_state.selected_database = None
if "selected_table" not in st.session_state: st.session_state.selected_table = None

if "excel_bytes" not in st.session_state: st.session_state.excel_bytes = None
if "summary_df" not in st.session_state: st.session_state.summary_df = None
if "daily_df" not in st.session_state: st.session_state.daily_df = None
if "weekly_df" not in st.session_state: st.session_state.weekly_df = None
if "monthly_df" not in st.session_state: st.session_state.monthly_df = None
if "campaign_email_name" not in st.session_state: st.session_state.campaign_email_name = "General"

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
        st.sidebar.warning("No databases found.")

if not st.session_state.databases:
    refresh_workspace()

if st.sidebar.button("Refresh Workspace"): refresh_workspace()

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

                            camp_context = "General"
                            if camp_col and camp_col != "(None)" and camp_col in df_filtered.columns:
                                unique_camps = df_filtered[camp_col].dropna().unique()
                                if len(unique_camps) > 0:
                                    camp_context = str(unique_camps[0]) 
                                    if len(unique_camps) > 1: camp_context += " (and others)"
                            st.session_state.campaign_email_name = camp_context

                            total_budget_global = run_aggregation_query(ws, db, tbl, f"`{budget_col}`", "SUM") 
                            total_days_global = run_aggregation_query(ws, db, tbl, f"DISTINCT `{date_col}`", "COUNT") 
                            
                            summary_df = calculate_summary_kpis(df_filtered, total_budget_global, total_days_global, col_map)
                            daily_df = process_granular_data(df_filtered, col_map, freq='D')
                            weekly_df = process_granular_data(df_filtered, col_map, freq='W')
                            monthly_df = process_granular_data(df_filtered, col_map, freq='M')

                            st.session_state.summary_df = summary_df
                            st.session_state.daily_df = daily_df
                            st.session_state.weekly_df = weekly_df
                            st.session_state.monthly_df = monthly_df
                            
                            dfs_to_save = {'Summary': summary_df, 'Daily': daily_df, 'Weekly': weekly_df, 'Monthly': monthly_df}
                            st.session_state.excel_bytes = generate_excel_report(df_filtered, summary_df, dfs_to_save, col_map, total_budget_global)
                            st.success("Reports generated successfully!")

                    except Exception as e:
                        st.error(f"Error: {e}")
            else:
                st.error("Select dates.")

        if st.session_state.excel_bytes:
            st.divider()
            
            currency_format = st.column_config.NumberColumn(format="$%.2f")
            cols_config = {
                "Budget": currency_format, "Spend": currency_format, "Spend MTD": currency_format,
                "Revenue": currency_format, "Revenue MTD": currency_format
            }

            t1, t2, t3, t4 = st.tabs(["Summary", "Daily", "Weekly", "Monthly"])
            with t1: st.dataframe(st.session_state.summary_df, use_container_width=True)
            
            with t2:
                if not st.session_state.daily_df.empty:
                    st.markdown("#### Daily Insights")
                    insights = get_dynamic_insights(st.session_state.daily_df, "Day")
                    for i in insights: st.caption(f"• {i}")
                    st.divider()
                st.dataframe(st.session_state.daily_df, column_config=cols_config, use_container_width=True)
            
            with t3:
                if not st.session_state.weekly_df.empty:
                    st.markdown("#### Weekly Insights")
                    insights = get_dynamic_insights(st.session_state.weekly_df, "Week")
                    for i in insights: st.caption(f"• {i}")
                    st.divider()
                st.dataframe(st.session_state.weekly_df, column_config=cols_config, use_container_width=True)
            
            with t4:
                if not st.session_state.monthly_df.empty:
                    st.markdown("#### Monthly Insights")
                    insights = get_dynamic_insights(st.session_state.monthly_df, "Month")
                    for i in insights: st.caption(f"• {i}")
                    st.divider()
                st.dataframe(st.session_state.monthly_df, column_config=cols_config, use_container_width=True)
            
            # --- EMAIL SECTION (HTML FORMATTED) ---
            st.divider()
            c1, c2 = st.columns([1, 2])
            with c1:
                st.download_button("Download Excel", st.session_state.excel_bytes, "pacing.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with c2:
                st.subheader("Email Report")
                with st.form("email"):
                    rec = st.text_input("Recipient Email")
                    
                    campaign_name = st.session_state.get('campaign_email_name', 'General')
                    
                    if start_date and end_date:
                        date_range_str = f"{start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}"
                    else:
                        date_range_str = "N/A"
                    
                    today_str = datetime.now().strftime('%d-%b-%Y')
                    
                    # --- Generate HTML KPI List ---
                    kpi_html_list = ""
                    if st.session_state.summary_df is not None:
                        try:
                            df_sum = st.session_state.summary_df
                            val_spend = df_sum[df_sum['Metric'] == 'Total Spend Till Date']['Value'].values[0]
                            val_roas = df_sum[df_sum['Metric'] == 'Current ROAS']['Value'].values[0]
                            val_pacing = df_sum[df_sum['Metric'] == 'Spend Pacing % (Actual)']['Value'].values[0]
                            
                            kpi_html_list = (
                                f"<li style='margin-bottom: 8px;'><strong>Total Spend:</strong> {val_spend}</li>"
                                f"<li style='margin-bottom: 8px;'><strong>Current ROAS:</strong> {val_roas}</li>"
                                f"<li style='margin-bottom: 8px;'><strong>Pacing:</strong> {val_pacing}</li>"
                            )
                        except:
                            kpi_html_list = "<li>Metrics unavailable</li>"

                    final_subject = f"{campaign_name} Campaign Pacing Report {today_str}"
                    
                    # --- HTML EMAIL TEMPLATE ---
                    final_body_html = f"""
                    <html>
                      <body style="font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px;">
                        <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; padding: 30px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
                            <h2 style="color: #2c3e50; border-bottom: 2px solid #FF6B15; padding-bottom: 10px; margin-top: 0;">Pacing Report</h2>
                            
                            <p style="color: #555; font-size: 16px;">Hello Team,</p>
                            <p style="color: #555; line-height: 1.5;">Please find the attached <strong>{campaign_name}</strong> campaign pacing report for your review.</p>
                            
                            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; border-left: 4px solid #FF6B15; margin: 20px 0;">
                                <h4 style="margin-top: 0; margin-bottom: 10px; color: #2c3e50;">Executive Summary</h4>
                                <ul style="color: #444; padding-left: 20px; margin-bottom: 10px;">
                                    {kpi_html_list}
                                </ul>
                                <p style="margin: 0; font-size: 14px; color: #666;"><strong>Reporting Period:</strong> {date_range_str}</p>
                            </div>

                            <p style="color: #555; line-height: 1.5;">If you have any questions regarding the data or insights, please feel free to reach out by mail.</p>
                            
                            <hr style="border: 0; border-top: 1px solid #eee; margin: 30px 0;">
                            
                            <p style="color: #888; font-size: 14px;">Best Regards,<br><strong>Solutions and Consulting	 Team</strong></p>
                        </div>
                      </body>
                    </html>
                    """

                    if st.form_submit_button("Send Email") and rec:
                        s, m = send_email_with_attachment(rec, final_subject, final_body_html, st.session_state.excel_bytes)
                        if s: st.success(m)
                        else: st.error(m)