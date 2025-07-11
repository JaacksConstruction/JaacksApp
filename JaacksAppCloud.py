import streamlit as st
import pandas as pd
import numpy as np
import datetime
import plotly.express as px
import uuid
import re
import hashlib
import secrets
import io
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from fpdf import FPDF
from pathlib import Path

# --- Page Config ---
st.set_page_config(page_title='JC Construction Tracker', layout='wide')

# --- Custom CSS ---
st.markdown("""
<style>
    /* Make Streamlit buttons less prominent if they are just for navigation */
    .stButton>button {
        background-color: #4A5568; color: white; border-radius: 5px;
        padding: 5px 10px; font-size: 0.8em; width: 100%;
        text-align: left; margin-bottom: 5px; border: none;
    }
    .stButton>button:hover { background-color: #2D3748; color: white; }
    .stButton>button:focus { background-color: #2D3748; color: white; box-shadow: none; }
    .kpi-group-container { background-color: #1f2937; padding: 20px; border-radius: 10px; margin-bottom: 25px; box-shadow: 0 4px 12px rgba(0,0,0,0.2); }
    .kpi-group-title { font-size: 1.5em; font-weight: bold; color: #e5e7eb; margin-bottom: 20px; padding-bottom: 10px; border-bottom: 1px solid #374151; }
    .metric-box { background-color: #374151; padding: 25px; border-radius: 8px; text-align: center; color: #e5e7eb; box-shadow: 0 6px 10px rgba(0,0,0,0.15); height: 160px; display: flex; flex-direction: column; justify-content: center; align-items: center; margin-bottom: 10px; transition: transform 0.2s ease-in-out; }
    .metric-box:hover { transform: translateY(-5px); }
    .metric-box h4 { font-size: 1em; color: #9ca3af; margin-bottom: 10px; font-weight: 500; line-height: 1.3; }
    .metric-box h2 { font-size: 2.2em; font-weight: 700; color: #ffffff; line-height: 1.1; }
    .stTabs [data-baseweb="tab-list"] { gap: 2px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: #374151; border-radius: 4px 4px 0px 0px; gap: 1px; padding-top: 10px; padding-bottom: 10px; color: #9ca3af; }
    .stTabs [aria-selected="true"] { background-color: #1f2937; color: #6366f1; font-weight: bold; }
    .job-row-yellow td { background-color: #A2FF8A !important; color: black !important; }
    .job-row-light-red td { background-color: #FF0000 !important; color: black !important; }
</style>
""", unsafe_allow_html=True)

# --- Global Variables & Setup ---
file_defs = {
    'jobs.csv': ['Job Name', 'Client', 'Status', 'Start Date', 'End Date', 'Description', 'Estimated Hours', 'Estimated Materials Cost', 'UniqueID',
                 'ClientAddress', 'ClientCity', 'ClientState', 'ClientZip'],
    'job_time.csv': ['Contractor', 'Client', 'Job', 'Date', 'Start Time', 'End Time', 'Time Duration (Hours)', 'UniqueID', 'JobUniqueID'],
    'materials.csv': ['Material', 'Contractor', 'Client', 'Job', 'Date Used', 'Amount', 'Payor', 'UniqueID', 'JobUniqueID'],
    'receipts.csv': ['Contractor Name', 'Client Name', 'Job Name', 'Payor', 'Amount', 'File Name', 'File Path', 'Upload Date', 'UniqueID', 'JobUniqueID'],
    'users.csv': ['Username', 'PasswordHash', 'Salt', 'Role', 'FirstName', 'Surname', 'AssociatedClientName', 'UserUniqueID'],
    'down_payments.csv': ['DownPaymentID', 'JobUniqueID', 'DateReceived', 'Amount', 'PaymentMethod', 'Notes'],
    'job_files.csv': ['FileID', 'JobUniqueID', 'FileName', 'RelativePath', 'Category', 'UploadDate', 'UploadedByUsername']
}

# --- GOOGLE API & DATA HANDLING ---

# These will come from your Streamlit Secrets file (`.streamlit/secrets.toml`)
# IMPORTANT: You MUST create these folders in your Google Drive and get their IDs.
DRIVE_FOLDER_ID_RECEIPTS = "15Z7OLMrZLa6fdu8Pue52FIoRW6CsZc5x"
DRIVE_FOLDER_ID_JOB_FILES = "1L7Q1PpDQeg1rz6VEvO7krh9slMJwxL7T"
DRIVE_FOLDER_ID_ESTIMATES_INVOICES = "1LCEuA0WOgJH0MYNSq13FeGZUesOjVa4K"
SPREADSHEET_KEY = "1Ik_6-5NKLiJLeT_ZkT4nE1l-EjlqqX4Kdsxhd6fR5A8"
LOGO_PATH = "logo.jpg" # This remains a local path for PDF generation

# Define the scopes for both APIs
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

# This function centralizes the connection and caches it for efficiency.
@st.cache_resource
def get_google_apis():
    """Initializes and returns the Drive and Sheets service objects."""
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=creds)
        sheets_service = gspread.authorize(creds)
        return drive_service, sheets_service
    except Exception as e:
        st.error(f"Failed to connect to Google APIs. Check your secrets configuration. Error: {e}")
        st.stop()

# Initialize the services once
drive_service, sheets_service = get_google_apis()

# --- NEW DATA HANDLING FUNCTIONS ---

@st.cache_data(ttl="5m")
def load_data(worksheet_name):
    """
    Loads a worksheet from Google Sheets into a DataFrame, ensuring columns exist.
    """
    try:
        sheet_columns = {
            'jobs': ['Job Name', 'Client', 'Status', 'Start Date', 'End Date', 'Description', 'Estimated Hours', 'Estimated Materials Cost', 'UniqueID', 'ClientAddress', 'ClientCity', 'ClientState', 'ClientZip'],
            'job_time': ['Contractor', 'Client', 'Job', 'Date', 'Start Time', 'End Time', 'Time Duration (Hours)', 'UniqueID', 'JobUniqueID'],
            'materials': ['Material', 'Contractor', 'Client', 'Job', 'Date Used', 'Amount', 'Payor', 'UniqueID', 'JobUniqueID'],
            'receipts': ['Contractor Name', 'Client Name', 'Job Name', 'Payor', 'Amount', 'File Name', 'File Path', 'Upload Date', 'UniqueID', 'JobUniqueID'],
            'users': ['Username', 'PasswordHash', 'Salt', 'Role', 'FirstName', 'Surname', 'AssociatedClientName', 'UserUniqueID'],
            'down_payments': ['DownPaymentID', 'JobUniqueID', 'DateReceived', 'Amount', 'PaymentMethod', 'Notes'],
            'job_files': ['FileID', 'JobUniqueID', 'FileName', 'RelativePath', 'Category', 'UploadDate', 'UploadedByUsername'],
            'invoices': ['DocNumber', 'JobUniqueID', 'DateGenerated'],
            'estimates': ['DocNumber', 'JobUniqueID', 'DateGenerated']
        }
        
        expected_cols = sheet_columns.get(worksheet_name)
        if not expected_cols:
            st.error(f"Column definition not found for worksheet: {worksheet_name}")
            return pd.DataFrame(columns=[])

        try:
            sheet = sheets_service.open_by_key(SPREADSHEET_KEY).worksheet(worksheet_name)
            data = sheet.get_all_records()
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Worksheet '{worksheet_name}' not found. An empty table will be used.")
            return pd.DataFrame(columns=expected_cols)

        if not data:
            return pd.DataFrame(columns=expected_cols)
        
        df = pd.DataFrame(data)
        
        made_changes = False
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ''
                made_changes = True
        
        if made_changes:
            save_data(df, worksheet_name)
        
        df = df[expected_cols]

        # --- Type Conversions ---
        # Convert all date-like columns to date objects, coercing errors
        date_cols = [col for col in df.columns if "date" in col.lower()]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        # Convert other columns
        for col in df.columns:
            if col not in date_cols:
                if "cost" in col.lower() or "amount" in col.lower() or "hours" in col.lower():
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                else:
                    df[col] = df[col].astype(str).replace(['nan', 'None', 'NONE', '<NA>', 'NaT'], '')
        return df
    except Exception as e:
        st.error(f"Failed to load data from '{worksheet_name}': {e}")
        return pd.DataFrame(columns=sheet_columns.get(worksheet_name, []))

def save_data(df_to_save, worksheet_name):
    """Saves a DataFrame to a specified worksheet in Google Sheets."""
    try:
        sheet = sheets_service.open_by_key(SPREADSHEET_KEY).worksheet(worksheet_name)
        df_to_save_str = df_to_save.astype(str).replace(['nan', 'None', 'NONE', '<NA>', 'NaT'], '')
        sheet.clear()
        sheet.update([df_to_save_str.columns.values.tolist()] + df_to_save_str.values.tolist(), value_input_option='USER_ENTERED')
    except Exception as e:
        st.error(f"Failed to save data to worksheet '{worksheet_name}': {e}")

def upload_file_to_drive(file_object, file_name, parent_folder_id):
    """Uploads a file to a specific parent folder in Google Drive and returns the shareable link."""
    try:
        file_io = io.BytesIO(file_object.getvalue())
        file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
        media = MediaIoBaseUpload(file_io, mimetype=file_object.type, resumable=True)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
        drive_service.permissions().create(fileId=file.get('id'), body={'type': 'anyone', 'role': 'reader'}).execute()
        return file.get('webViewLink')
    except Exception as e:
        st.error(f"An error occurred during file upload to Google Drive: {e}")
        return None

# --- Helper Functions (Your Original Functions) ---
def sanitize_foldername(name):
    if not isinstance(name, str): name = str(name)
    name = name.strip(); name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')
    return name[:50] if len(name) > 50 else name

def generate_salt(): return secrets.token_hex(16)
def hash_password(password, salt): return hashlib.sha256((salt + password).encode('utf-8')).hexdigest()
def verify_password(stored_hash, provided_password, salt): return stored_hash == hash_password(provided_password, salt)

def get_current_user_role():
    return st.session_state.logged_in_user.get('Role') if st.session_state.authentication_status and st.session_state.logged_in_user else None
def get_current_username():
    return st.session_state.logged_in_user.get('Username') if st.session_state.authentication_status and st.session_state.logged_in_user else None
def get_current_user_fullname():
    if st.session_state.authentication_status and st.session_state.logged_in_user:
        return f"{st.session_state.logged_in_user.get('FirstName','')} {st.session_state.logged_in_user.get('Surname','')}".strip()
    return None
def get_associated_client_name():
    if st.session_state.authentication_status and st.session_state.logged_in_user and get_current_user_role() == 'Client Viewer':
        return st.session_state.logged_in_user.get('AssociatedClientName')
    return None

# --- Formatters ---
def format_currency(value):
    try: return f"${float(value):,.2f}" if pd.notna(value) and str(value).strip() != '' else "$0.00"
    except (ValueError, TypeError): return "$0.00"
def format_hours(value, dec_places=2):
    try: return f"{float(value):.{dec_places}f}" if pd.notna(value) and str(value).strip() != '' else f"{0.0:.{dec_places}f}"
    except (ValueError, TypeError): return f"{0.0:.{dec_places}f}"
def truncate_text(txt, max_len=50):
    txt_str = str(txt) if pd.notna(txt) else ""
    return (txt_str[:max_len] + '...') if len(txt_str) > max_len else txt_str

def highlight_job_deadlines(row):
    style = [''] * len(row)
    today = datetime.date.today()
    end_date_val = row.get('End Date')
    
    # This check ensures we only do math on actual date objects
    if isinstance(end_date_val, datetime.date) and row.get('Status') == 'In Progress':
        try:
            delta = (end_date_val - today).days
            if delta <= 3:
                style = ['background-color: #FF0000; color: black;'] * len(row)
            elif delta <= 7:
                style = ['background-color: #A2FF8A; color: black;'] * len(row)
        except TypeError:
            # This will catch any lingering type errors, though the new load_data should prevent them
            pass
            
    return style

def display_paginated_dataframe(df_in, page_key, page_size=10, col_config=None, trunc_map=None, styler_fn=None):
    if not isinstance(df_in, pd.DataFrame) or df_in.empty:
        st.info("No data to display.")
        return

    df_disp = df_in.copy()
    if trunc_map:
        for col, length in trunc_map.items():
            if col in df_disp.columns: df_disp[col] = df_disp[col].astype(str).apply(lambda x: truncate_text(x, length))

    total_items = len(df_disp); total_pages = max(1, (total_items - 1) // page_size + 1)
    current_page_val = st.session_state.get(page_key, 0)
    current_page_val = max(0, min(current_page_val, total_pages - 1))
    st.session_state[page_key] = current_page_val

    start_idx = current_page_val * page_size
    end_idx = start_idx + page_size
    df_to_show = df_disp.iloc[start_idx:end_idx]

    if styler_fn and not df_to_show.empty:
        st.dataframe(df_to_show.style.apply(styler_fn, axis=1), column_config=col_config, use_container_width=True, hide_index=True)
    else:
        st.dataframe(df_to_show, column_config=col_config, use_container_width=True, hide_index=True)

    if total_pages > 1:
        pc, mc, nc = st.columns([1,3,1])
        if pc.button("⬅️ Prev", disabled=(current_page_val == 0), key=f"prev_{page_key}_fp_nav",use_container_width=True):
            st.session_state[page_key] -= 1
            st.rerun()
        mc.write(f"Page {current_page_val + 1} of {total_pages}")
        if nc.button("Next ➡️", disabled=(current_page_val >= total_pages - 1), key=f"next_{page_key}_fp_nav",use_container_width=True):
            st.session_state[page_key] += 1
            st.rerun()

# --- PDF Class (No changes needed, it uses a local path) ---
class PDF(FPDF):
    def __init__(self, company_details, logo_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_details = company_details
        self.logo_path = logo_path
        self.set_auto_page_break(auto=True, margin=15)
        self.set_draw_color(200, 200, 200)
        self.font_family = "Arial"

    def header(self):
        try:
            if self.logo_path and Path(self.logo_path).is_file():
                self.image(self.logo_path, x=10, y=8, w=33)
            self.set_font(self.font_family, 'B', 16)
            self.cell(0, 10, self.company_details.get("name", ""), 0, 1, 'C')
            self.set_font(self.font_family, '', 9)
            self.cell(0, 5, self.company_details.get("address", ""), 0, 1, 'C')
            self.cell(0, 5, f"Phone: {self.company_details.get('phone','')} | Email: {self.company_details.get('email','')}", 0, 1, 'C')
            self.ln(10)
        except Exception as e:
            st.warning(f"Could not generate PDF header: {e}")

    def footer(self):
        self.set_y(-15)
        self.set_font(self.font_family, 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def document_title_section(self, title, doc_num, issue_date):
        self.set_font(self.font_family, 'B', 18)
        self.cell(0, 10, title.upper(), 0, 0, 'R'); self.ln(6)
        self.set_font(self.font_family, '', 10)
        self.cell(0, 7, f"{title} #: {doc_num}", 0, 1, 'R')
        self.cell(0, 7, f"Date: {issue_date.strftime('%B %d, %Y')}", 0, 1, 'R'); self.ln(7)

    def bill_to_job_info(self, client_data, job_data):
        x_start, y_start, line_height = self.get_x(), self.get_y(), 6
        client_address_formatted =(
            #client_address_formatted = (
                f"{client_data.get('Client', 'N/A')}\n"
                f"{client_data.get('ClientAddress', '')}\n"
                f"{client_data.get('ClientCity', '')}, {client_data.get('ClientState', '')} {client_data.get('ClientZip', '')}"
        ).strip()
        self.set_font(self.font_family, 'B', 11)
        self.multi_cell(90, line_height, "BILL TO / CLIENT:", 0, 'L')
        self.set_font(self.font_family, '', 10)
        self.set_xy(x_start, self.get_y())
        self.multi_cell(90, line_height, client_address_formatted.strip(), 0, 'L')
        bill_to_height = self.get_y() - y_start
        self.set_xy(x_start + 100, y_start)
        self.set_font(self.font_family, 'B', 11)
        self.multi_cell(90, line_height, "JOB DETAILS:", 0, 'L')
        self.set_font(self.font_family, '', 10)
        self.set_xy(x_start + 100, self.get_y())
        self.multi_cell(90, line_height, f"Job: {job_data['Job Name']}\nDesc: {truncate_text(job_data['Description'], 150)}", 0, 'L')
        job_details_height = self.get_y() - y_start
        self.set_y(y_start + max(bill_to_height, job_details_height) + 5); self.ln(5)

    def line_items_table(self, headers, data, col_widths):
        self.set_font(self.font_family, 'B', 9)
        self.set_fill_color(230, 230, 230)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, 1, 0, 'C', True)
        self.ln()
        self.set_font(self.font_family, '', 9)
        self.set_fill_color(255, 255, 255)
        for row in data:
            if self.get_y() + 15 > self.page_break_trigger:
                self.add_page()
                self.set_font(self.font_family, 'B', 9)
                self.set_fill_color(230, 230, 230)
                for i, h in enumerate(headers):
                    self.cell(col_widths[i], 7, h, 1, 0, 'C', True)
                self.ln()
                self.set_font(self.font_family, '', 9)
            y_before_row = self.get_y()
            self.multi_cell(col_widths[0], 6, str(row[0]), border='LR', align='L')
            desc_height = self.get_y() - y_before_row
            self.set_y(y_before_row)
            self.set_x(self.l_margin + col_widths[0])
            for i in range(1, len(row)):
                align = 'R'
                self.cell(col_widths[i], desc_height, str(row[i]), border='R', align=align)
            self.ln(desc_height)
        self.cell(sum(col_widths), 0, '', 'T')

    def totals_section(self, subtotal, tax_label, tax_amount, grand_total):
        self.ln(5)
        self.set_font(self.font_family, '', 10)
        self.cell(130, 7, "Subtotal:", 0, 0, 'R'); self.cell(40, 7, format_currency(subtotal), 1, 1, 'R')
        self.cell(130, 7, f"{tax_label}:", 0, 0, 'R'); self.cell(40, 7, format_currency(tax_amount), 1, 1, 'R')
        self.set_font(self.font_family, 'B', 11)
        self.set_fill_color(220, 220, 220)
        self.cell(130, 8, "GRAND TOTAL:", 0, 0, 'R'); self.cell(40, 8, format_currency(grand_total), 1, 1, 'R', True); self.ln(10)

    def notes_terms_signatures(self, notes, terms, sig_h=20):
        if self.get_y() + 70 > self.h - self.b_margin:
            self.add_page()
        
        self.set_font(self.font_family, 'B', 10)
        self.cell(0, 6, "Notes / Inscription:", 0, 1, 'L')
        self.set_font(self.font_family, '', 9)
        self.multi_cell(0, 5, notes if notes else "N/A", 0, 'L'); self.ln(3)
        
        self.set_font(self.font_family, 'B', 10)
        self.cell(0, 6, "Terms & Conditions:", 0, 1, 'L')
        self.set_font(self.font_family, '', 9)
        self.multi_cell(0, 5, terms, 0, 'L'); self.ln(10)
        
        y_for_signatures = self.h - self.b_margin - sig_h - 5
        if self.get_y() > y_for_signatures:
            self.add_page()
        self.set_y(y_for_signatures)
        
        self.set_font(self.font_family, '', 10)
        self.cell(80, sig_h, "Customer Signature:", "T", 0, 'L'); self.cell(30, sig_h, "", 0, 0); self.cell(80, sig_h, "Contractor Signature:", "T", 1, 'L')

# --- Initialize Session State ---
default_session_states = {
    "logged_in_user": None, "authentication_status": False, "current_page_users": 0,
    "dashboard_focus": "jobs_status_chart", "selected_dashboard_job": "All Jobs",
    "dashboard_client_filter": "All Clients", "current_page_jobs_dashboard": 0,
    "current_page_jobs_details_section_vFull": 0,
    "current_page_job_time": 0, "current_page_job_time_admin_edit":0,
    "current_page_materials": 0, "current_page_materials_admin_edit":0,
    "current_page_receipts": 0, "current_page_receipts_admin_edit":0,
    "current_page_down_payments":0, "current_page_job_files":0,
    "current_page_invoice_time": 0, "current_page_invoice_materials": 0,
    "selected_client_job_time": "Select a Client", "selected_client_material_usage": "Select a Client",
    "selected_client_receipt_upload": "Select a Client",
    "invoice_line_items": [],
    "invoice_terms": "Payment due upon receipt. Thank you for your business!",
    "company_name_pdf": "JC Construction",
    "company_address_pdf": "21710 478th Ave,Aurora, SD, 570032",
    "company_phone_pdf": "(605) 690-6642",
    "company_email_pdf": "Jcinc2009@gmail.com",
    "admin_edit_job_client_filter": "All Clients", "admin_edit_job_name_filter": "All Jobs",
    "edit_time_entry_contractor_filter": "", "edit_time_entry_date_filter": None,
    "selected_client_material_view": "All Clients", "selected_client_receipt_view": "All Clients",
    "inv_est_time_job_rate": 65.0,
    "inv_actual_time_total_job_rate": 65.0,
    "ig_tax_rate_val": 2.041,
}
for key, value in default_session_states.items():
    if key not in st.session_state:
        st.session_state[key] = value

if not isinstance(st.session_state.get("invoice_line_items"), list) or not st.session_state.get("invoice_line_items"):
    st.session_state.invoice_line_items = [{'description': '', 'quantity': 1.0, 'unit_price': 0.0, 'total': 0.0, 'source': 'manual'}]

# --- Global Data Loading ---
try:
    jobs_df = load_data('jobs')
    job_time_df = load_data('job_time')
    materials_df = load_data('materials')
    #st.write("Columns in job_time sheet:", job_time_df.columns)
    #st.write("Columns in materials sheet:", materials_df.columns)
    receipts_df = load_data('receipts')
    users_df = load_data('users')
    down_payments_df = load_data('down_payments')
    job_files_df = load_data('job_files')
    invoices_df = load_data('invoices')
    estimates_df = load_data('job_files')
except Exception as e:
    st.error(f"A critical error occurred during initial data loading from Google Sheets: {e}")
    st.stop()

# --- Post-load cleaning (optional with cloud data, but good practice) ---
if not jobs_df.empty:
    for col in ['Job Name', 'Client', 'Status', 'Description', 'UniqueID']:
        if col in jobs_df.columns: jobs_df[col] = jobs_df[col].astype(str).str.strip()
# ... (add similar cleaning for other dataframes if needed) ...
# === Main App Header & Authentication (No major changes here) ===
header_cols = st.columns([0.85, 0.15])
with header_cols[0]:
    st.markdown("""<div style="background-color: #004466; color: white; text-align: left; padding: 1em 1em 1em 2em; border-radius: 5px; display: flex; flex-direction: column; justify-content: center; height: 80px;"><h2 style="margin:0; padding:0; line-height:1.2; font-size: 1.8em;">JC Construction</h2><p style="margin:0; padding:0; line-height:1; font-size: 0.9em;">Building Integrity</p></div>""", unsafe_allow_html=True)
with header_cols[1]:
    if LOGO_PATH and Path(LOGO_PATH).is_file():
        try: st.image(LOGO_PATH, width=100)
        except Exception: st.caption(" ")
    else: st.markdown("<div style='height:80px; display:flex; align-items:center; justify-content:center; border: 1px dashed #ccc;'><span style='color:grey; font-style:italic;'>Logo</span></div>", unsafe_allow_html=True)
st.markdown("<hr style='margin-top:5px; margin-bottom:15px;'>", unsafe_allow_html=True)

def login_user(username, password, users_df_local):
    user_record = users_df_local[users_df_local['Username'] == username]
    if not user_record.empty:
        user_data = user_record.iloc[0]
        if verify_password(user_data['PasswordHash'], password, user_data['Salt']):
            st.session_state.authentication_status = True
            st.session_state.logged_in_user = user_data.to_dict()
            st.sidebar.success(f"Welcome {user_data.get('FirstName', 'User')}!")
            st.rerun()
        else: st.sidebar.error("Incorrect username or password.")
    else: st.sidebar.error("Incorrect username or password.")

def logout_user():
    # ... (logout function remains the same) ...
    st.session_state.authentication_status = False
    st.session_state.logged_in_user = None
    st.rerun()

st.sidebar.title("User Access")
if not st.session_state.authentication_status:
    # ... (login form remains the same) ...
    login_username = st.sidebar.text_input("Username", key="login_uname_main_app").lower()
    login_password = st.sidebar.text_input("Password", type="password", key="login_pw_main_app")
    if st.sidebar.button("Login", key="login_btn_main_app"):
        if login_username and login_password: login_user(login_username, login_password, users_df)
        else: st.sidebar.warning("Please enter both username and password.")
else:
    # ... (logout button remains the same) ...
    user = st.session_state.logged_in_user
    st.sidebar.write(f"Logged in: **{user.get('Username','N/A')}** ({user.get('Role','N/A')})")
    if st.sidebar.button("Logout", key="logout_btn_main_app"): logout_user()

st.sidebar.title("Navigation")
section = None # Initialize section to prevent NameError

if st.session_state.authentication_status:
    current_user_role_val = get_current_user_role()
    nav_options_full_list = ['Dashboard', 'Job Details', 'Job Time Tracking', 'Material Usage', 'Upload Receipt', 'Down Payments Log', 'Job File Uploads', 'Invoice Generation', 'Reports & Analytics']

    if current_user_role_val == 'Client Viewer':
        nav_options_client_viewer = ['Dashboard', 'Job Details', 'Down Payments Log', 'Job File Uploads', 'Reports & Analytics']
        section = st.sidebar.selectbox("Go to", nav_options_client_viewer, key="nav_sel_client_viewer")
    elif current_user_role_val == 'Admin':
        nav_options_admin_list = list(nav_options_full_list)
        if 'User Management' not in nav_options_admin_list: nav_options_admin_list.append('User Management')
        section = st.sidebar.selectbox("Go to", nav_options_admin_list, key="nav_sel_admin")
    else: # Contractor, Manager, etc.
        section = st.sidebar.selectbox("Go to", nav_options_full_list, key="nav_sel_other_roles")
else:
    st.warning("Please log in to access the application.")
    st.stop() # Stop further execution if not logged in

# This block ensures that if a user just logged in, they are sent to the dashboard.
if section is None and st.session_state.authentication_status:
    section = "Dashboard"
    st.rerun()

# === Main App Logic (Sections) ===
# The script now proceeds to your if/elif blocks for each section...
# === Main App Logic (Sections) ===
# NOTE: All calls to save_data('dataframe', 'filename.csv') are replaced with save_data('dataframe', 'worksheet_name')

if section == 'User Management':
    if current_user_role_val == 'Admin':
        st.header("User Management")
        users_df_manage_section = load_data('users')

        st.subheader("Create New User")
        with st.form("new_user_form_um", clear_on_submit=True):
            new_fname_um = st.text_input("First Name*")
            new_sname_um = st.text_input("Surname*")
            new_role_select_um = st.selectbox("Role*", ["Contractor", "Manager", "Admin", "Client Viewer"])
            assoc_client_input_create_um = ""
            if new_role_select_um == "Client Viewer":
                clients_list_create_um = ["Select Client"] + sorted(list(jobs_df['Client'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
                assoc_client_input_create_um = st.selectbox("Associate with Client*", clients_list_create_um)
            new_pass_create_um = st.text_input("Password*", type="password")
            confirm_pass_create_um = st.text_input("Confirm Password*", type="password")

            if st.form_submit_button("Create User"):
                if not all([new_fname_um, new_sname_um, new_pass_create_um, confirm_pass_create_um]) or \
                   (new_role_select_um == "Client Viewer" and assoc_client_input_create_um == "Select Client"):
                    st.error("All fields marked * are required.")
                elif new_pass_create_um != confirm_pass_create_um: st.error("Passwords do not match.")
                else:
                    uname_base_um = (new_fname_um[0] + new_sname_um).lower().replace(" ", "")
                    uname_final_um = uname_base_um; count = 1
                    while not users_df_manage_section[users_df_manage_section['Username'] == uname_final_um].empty:
                        uname_final_um = f"{uname_base_um}{count}"; count += 1
                    salt_create_um = generate_salt(); phash_create_um = hash_password(new_pass_create_um, salt_create_um)
                    new_user_data_um = {'Username': uname_final_um, 'PasswordHash': phash_create_um,
                                        'Salt': salt_create_um, 'Role': new_role_select_um,
                                        'FirstName': new_fname_um.strip().title(), 'Surname': new_sname_um.strip().title(),
                                        'AssociatedClientName': assoc_client_input_create_um if new_role_select_um == "Client Viewer" else '',
                                        'UserUniqueID': uuid.uuid4().hex}
                    users_df_manage_section = pd.concat([users_df_manage_section, pd.DataFrame([new_user_data_um])], ignore_index=True)
                    save_data(users_df_manage_section, 'users')
                    st.success(f"User '{uname_final_um}' created!")
                    users_df = load_data('users'); st.rerun()

        st.markdown("---"); st.subheader("Existing Users")
        display_paginated_dataframe(users_df_manage_section[['Username', 'FirstName', 'Surname', 'Role', 'AssociatedClientName']],
                                      "users_page_display_um", 10)

        # --- Edit User Details ---
        st.markdown("---"); st.subheader("Edit User Details")
        edit_user_options_list_um = ["Select User to Edit"] + \
                                    sorted(users_df_manage_section.apply(lambda r: f"{r['FirstName']} {r['Surname']} ({r['Username']})", axis=1).tolist())
        selected_user_to_edit_display_um = st.selectbox("Select user to edit:", edit_user_options_list_um, key="select_user_for_edit_um")

        if selected_user_to_edit_display_um != "Select User to Edit":
            selected_username_for_edit_um = selected_user_to_edit_display_um.split('(')[-1][:-1]
            user_to_edit_data_series_um = users_df_manage_section[users_df_manage_section['Username'] == selected_username_for_edit_um]

            if not user_to_edit_data_series_um.empty:
                user_data_for_edit_um = user_to_edit_data_series_um.iloc[0]
                user_uid_for_edit_um = user_data_for_edit_um['UserUniqueID']

                with st.form(f"edit_user_form_{user_uid_for_edit_um}", clear_on_submit=False):
                    st.write(f"Editing User: {user_data_for_edit_um['FirstName']} {user_data_for_edit_um['Surname']} ({user_data_for_edit_um['Username']})")
                    st.caption(f"Username: {user_data_for_edit_um['Username']} (Cannot be changed here)")

                    edit_fname_um_val = st.text_input("First Name*", value=user_data_for_edit_um['FirstName'], key=f"fn_{user_uid_for_edit_um}")
                    edit_sname_um_val = st.text_input("Surname*", value=user_data_for_edit_um['Surname'], key=f"sn_{user_uid_for_edit_um}")

                    roles_list_edit_um = ["Contractor", "Manager", "Admin", "Client Viewer"]
                    current_role_idx_um = roles_list_edit_um.index(user_data_for_edit_um['Role']) if user_data_for_edit_um['Role'] in roles_list_edit_um else 0
                    edit_role_um_val = st.selectbox("Role*", roles_list_edit_um, index=current_role_idx_um, key=f"role_{user_uid_for_edit_um}")

                    assoc_client_current_um = user_data_for_edit_um['AssociatedClientName'] if 'AssociatedClientName' in user_data_for_edit_um else ''
                    final_assoc_client_edit_um = assoc_client_current_um
                    if edit_role_um_val == "Client Viewer":
                        clients_list_form_um = ["Select Client"] + sorted(list(jobs_df['Client'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
                        current_assoc_idx_um = 0
                        if assoc_client_current_um and assoc_client_current_um in clients_list_form_um:
                            current_assoc_idx_um = clients_list_form_um.index(assoc_client_current_um)
                        final_assoc_client_edit_um = st.selectbox("Associate with Client*", clients_list_form_um, index=current_assoc_idx_um, key=f"ac_{user_uid_for_edit_um}")
                    else:
                        final_assoc_client_edit_um = ""

                    if st.form_submit_button("Save User Changes"):
                        if not all([edit_fname_um_val, edit_sname_um_val]) or \
                           (edit_role_um_val == "Client Viewer" and final_assoc_client_edit_um == "Select Client"):
                            st.error("First Name, Surname are required. Client association is required for Client Viewer role.")
                        else:
                            idx_update_um = users_df_manage_section[users_df_manage_section['UserUniqueID'] == user_uid_for_edit_um].index
                            if not idx_update_um.empty:
                                users_df_manage_section.loc[idx_update_um[0], 'FirstName'] = edit_fname_um_val.strip().title()
                                users_df_manage_section.loc[idx_update_um[0], 'Surname'] = edit_sname_um_val.strip().title()
                                users_df_manage_section.loc[idx_update_um[0], 'Role'] = edit_role_um_val
                                users_df_manage_section.loc[idx_update_um[0], 'AssociatedClientName'] = final_assoc_client_edit_um if edit_role_um_val == "Client Viewer" else ''
                                save_data(users_df_manage_section, 'users')
                                st.success(f"User '{user_data_for_edit_um['Username']}' updated successfully.")
                                users_df = load_data('users'); st.rerun()
                            else: st.error("Failed to find user to update. Please refresh.")
            elif selected_user_to_edit_display_um != "Select User to Edit":
                st.error("Selected user details could not be retrieved. Please refresh.")

        # --- Delete User ---
        st.markdown("---"); st.subheader("Delete User")
        delete_options_um = ["Select User to Delete"] + \
                            sorted([f"{r['FirstName']} {r['Surname']} ({r['Username']})"
                                    for _, r in users_df_manage_section.iterrows()
                                    if r['Username'] != 'admin' and r['Username'] != current_username_val])
        selected_user_to_delete_disp_um = st.selectbox("Select user to delete:", delete_options_um, key="select_user_for_delete_um")

        if selected_user_to_delete_disp_um != "Select User to Delete":
            selected_uname_delete_um = selected_user_to_delete_disp_um.split('(')[-1][:-1]
            user_to_delete_series_um = users_df_manage_section[users_df_manage_section['Username'] == selected_uname_delete_um]

            if not user_to_delete_series_um.empty:
                user_data_delete_um = user_to_delete_series_um.iloc[0]
                user_uid_delete_um = user_data_delete_um['UserUniqueID']

                if st.button(f"Request Delete User: {user_data_delete_um['FirstName']} {user_data_delete_um['Surname']}", key=f"req_del_user_btn_{user_uid_delete_um}"):
                    st.session_state[f"confirm_del_user_flag_{user_uid_delete_um}"] = True

                if st.session_state.get(f"confirm_del_user_flag_{user_uid_delete_um}", False):
                    st.warning(f"Delete user: **{user_data_delete_um['FirstName']} {user_data_delete_um['Surname']} ({user_data_delete_um['Username']})**? This is irreversible.")
                    del_c1_um, del_c2_um = st.columns(2)
                    if del_c1_um.button("YES, DELETE THIS USER", key=f"confirm_del_user_final_btn_{user_uid_delete_um}"):
                        users_df_manage_section = users_df_manage_section[users_df_manage_section['UserUniqueID'] != user_uid_delete_um]
                        save_data(users_df_manage_section, 'users')
                        st.success(f"User '{selected_uname_delete_um}' deleted.")
                        del st.session_state[f"confirm_del_user_flag_{user_uid_delete_um}"]
                        users_df = load_data('users'); st.rerun()
                    if del_c2_um.button("CANCEL DELETION", key=f"cancel_del_user_final_btn_{user_uid_delete_um}"):
                        del st.session_state[f"confirm_del_user_flag_{user_uid_delete_um}"]; st.rerun()
            elif selected_user_to_delete_disp_um != "Select User to Delete":
                st.error("Selected user for deletion not found. Refresh.")

        # --- Admin Password Reset ---
        st.markdown("---"); st.subheader("Admin Password Reset")
        if not users_df_manage_section.empty:
            user_names_reset_um = ["Select User to Reset Password"] + \
                                  sorted(users_df_manage_section.apply(lambda r: f"{r['FirstName']} {r['Surname']} ({r['Username']})", axis=1).tolist())
            selected_user_disp_reset_um = st.selectbox("Select user for password reset:", user_names_reset_um, key="select_user_pwd_reset_um")

            if selected_user_disp_reset_um != "Select User to Reset Password":
                selected_uname_reset_um = selected_user_disp_reset_um.split('(')[-1][:-1]
                user_record_reset_um_series = users_df_manage_section[users_df_manage_section['Username'] == selected_uname_reset_um]
                if not user_record_reset_um_series.empty:
                    user_data_reset_um = user_record_reset_um_series.iloc[0]
                    with st.form(f"pwd_reset_form_{user_data_reset_um['UserUniqueID']}"):
                        st.write(f"Resetting password for: **{user_data_reset_um['FirstName']} {user_data_reset_um['Surname']}**")
                        new_pwd_reset_um = st.text_input("New Password*", type="password", key=f"new_pwd_{user_data_reset_um['UserUniqueID']}")
                        confirm_pwd_reset_um = st.text_input("Confirm New Password*", type="password", key=f"confirm_pwd_{user_data_reset_um['UserUniqueID']}")

                        if st.form_submit_button("Reset Password"):
                            if not new_pwd_reset_um or not confirm_pwd_reset_um: st.error("Enter and confirm the new password.")
                            elif new_pwd_reset_um != confirm_pwd_reset_um: st.error("Passwords do not match.")
                            else:
                                new_salt_um = generate_salt(); new_phash_um = hash_password(new_pwd_reset_um, new_salt_um)
                                idx_update_pwd_um = users_df_manage_section[users_df_manage_section['UserUniqueID'] == user_data_reset_um['UserUniqueID']].index
                                if not idx_update_pwd_um.empty:
                                    users_df_manage_section.loc[idx_update_pwd_um[0], 'PasswordHash'] = new_phash_um
                                    users_df_manage_section.loc[idx_update_pwd_um[0], 'Salt'] = new_salt_um
                                    save_data(users_df_manage_section, 'users')
                                    st.success(f"Password for {user_data_reset_um['Username']} reset!")
                                    users_df = load_data('users'); st.rerun()
                                else: st.error("User not found for password reset during save. Refresh.")
                elif selected_user_disp_reset_um != "Select User to Reset Password":
                    st.error("User details for password reset not found. Refresh.")
        else: st.info("No users available to reset passwords.")
    else:
        st.error("Access restricted to Admin.")

elif section == 'Dashboard':
    st.header("Job Dashboard")

    # Determine dashboard_jobs_df based on role and filters
    dashboard_jobs_view_df = jobs_df.copy()
    client_filter_for_dashboard = st.session_state.get("dashboard_client_filter", "All Clients")
    job_filter_for_dashboard = st.session_state.get("selected_dashboard_job", "All Jobs")

    if current_user_role_val == 'Client Viewer' and associated_client_name_val:
        dashboard_jobs_view_df = dashboard_jobs_view_df[dashboard_jobs_view_df['Client'].astype(str).strip() == associated_client_name_val.strip()]
        st.selectbox("Client:", options=[associated_client_name_val], index=0, key="dash_client_viewer_fixed", disabled=True)
        client_filter_for_dashboard = associated_client_name_val
        
        job_names_client_viewer = ["All My Jobs"] + sorted(list(dashboard_jobs_view_df['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
        current_job_filter_cv = job_filter_for_dashboard if job_filter_for_dashboard in job_names_client_viewer else "All My Jobs"
        job_filter_for_dashboard = st.selectbox("Filter by Job:", options=job_names_client_viewer,
                                                index=job_names_client_viewer.index(current_job_filter_cv),
                                                key="dash_job_client_viewer")
        if job_filter_for_dashboard == "All My Jobs": job_filter_for_dashboard = "All Jobs"

    else: # Admin, Manager, Contractor
        all_clients_list_dash = ["All Clients"] + (sorted(list(jobs_df['Client'].astype(str).str.strip().replace('',np.nan).dropna().unique())) if not jobs_df.empty else [])
        current_client_idx_dash = all_clients_list_dash.index(client_filter_for_dashboard) if client_filter_for_dashboard in all_clients_list_dash else 0
        client_filter_for_dashboard = st.selectbox("Filter by Client:", options=all_clients_list_dash,
                                                   index=current_client_idx_dash, key="dash_client_filter_main")
        st.session_state.dashboard_client_filter = client_filter_for_dashboard

        jobs_for_client_filter_dash = jobs_df.copy()
        if client_filter_for_dashboard != "All Clients":
            jobs_for_client_filter_dash = jobs_for_client_filter_dash[jobs_for_client_filter_dash['Client'].astype(str).str.strip() == client_filter_for_dashboard.strip()]
        
        all_jobs_list_dash = ["All Jobs"] + (sorted(list(jobs_for_client_filter_dash['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique())) if not jobs_for_client_filter_dash.empty else [])
        current_job_idx_dash = all_jobs_list_dash.index(job_filter_for_dashboard) if job_filter_for_dashboard in all_jobs_list_dash else 0
        job_filter_for_dashboard = st.selectbox(f"Filter by Job ({client_filter_for_dashboard if client_filter_for_dashboard != 'All Clients' else 'any client'}):",
                                                options=all_jobs_list_dash, index=current_job_idx_dash, key="dash_job_filter_main")
        st.session_state.selected_dashboard_job = job_filter_for_dashboard

    # --- KPI Calculations ---
    kpi_df_filtered = jobs_df.copy()
    if client_filter_for_dashboard != "All Clients":
        kpi_df_filtered = kpi_df_filtered[kpi_df_filtered['Client'].astype(str).str.strip() == client_filter_for_dashboard.strip()]
    if job_filter_for_dashboard != "All Jobs":
        kpi_df_filtered = kpi_df_filtered[kpi_df_filtered['Job Name'].astype(str).str.strip() == job_filter_for_dashboard.strip()]

    total_jobs_kpi = len(kpi_df_filtered)
    completed_jobs_kpi = len(kpi_df_filtered[kpi_df_filtered['Status'] == 'Completed'])
    in_progress_jobs_kpi = len(kpi_df_filtered[kpi_df_filtered['Status'] == 'In Progress'])

    df_wip_kpis = kpi_df_filtered[kpi_df_filtered['Status'] == 'In Progress']
    est_hours_wip_kpi = df_wip_kpis['Estimated Hours'].sum() if not df_wip_kpis.empty else 0.0
    est_materials_wip_kpi = df_wip_kpis['Estimated Materials Cost'].sum() if not df_wip_kpis.empty else 0.0
    wip_job_uids_kpi = df_wip_kpis['UniqueID'].astype(str).str.strip().unique().tolist()

    actual_hours_wip_kpi = 0.0
    if not job_time_df.empty and wip_job_uids_kpi:
        actual_hours_wip_kpi = job_time_df[job_time_df['JobUniqueID'].isin(wip_job_uids_kpi)]['Time Duration (Hours)'].sum()

    total_actual_materials_wip_kpi = 0.0
    if wip_job_uids_kpi:
        mats_wip_cost = materials_df[materials_df['JobUniqueID'].isin(wip_job_uids_kpi)]['Amount'].sum() if not materials_df.empty else 0.0
        receipts_wip_cost = receipts_df[receipts_df['JobUniqueID'].isin(wip_job_uids_kpi)]['Amount'].sum() if not receipts_df.empty else 0.0
        total_actual_materials_wip_kpi = mats_wip_cost + receipts_wip_cost
    
    total_down_payments_wip_kpi = 0.0
    if not down_payments_df.empty and wip_job_uids_kpi:
        total_down_payments_wip_kpi = down_payments_df[down_payments_df['JobUniqueID'].isin(wip_job_uids_kpi)]['Amount'].sum()

    # --- KPI Display ---
    st.markdown("<div class='kpi-group-container'><div class='kpi-group-title'>Job Activity Overview</div>", unsafe_allow_html=True)
    kpi_cols_display1 = st.columns(3)
    kpi_cols_display1[0].markdown(f"<div class='metric-box'><h4>Total Jobs ({'Filtered' if job_filter_for_dashboard != 'All Jobs' or client_filter_for_dashboard != 'All Clients' else 'Overall'})</h4><h2>{total_jobs_kpi}</h2></div>", unsafe_allow_html=True)
    kpi_cols_display1[1].markdown(f"<div class='metric-box'><h4>Completed Jobs</h4><h2>{completed_jobs_kpi}</h2></div>", unsafe_allow_html=True)
    kpi_cols_display1[2].markdown(f"<div class='metric-box'><h4>In Progress Jobs</h4><h2>{in_progress_jobs_kpi}</h2></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='kpi-group-container'><div class='kpi-group-title'>Work In Progress (WIP) Estimates</div>", unsafe_allow_html=True)
    kpi_cols_display2 = st.columns(2)
    kpi_cols_display2[0].markdown(f"<div class='metric-box'><h4>Est. Hours (WIP)</h4><h2>{format_hours(est_hours_wip_kpi, 0)}</h2></div>", unsafe_allow_html=True)
    kpi_cols_display2[1].markdown(f"<div class='metric-box'><h4>Est. Material Cost (WIP)</h4><h2>{format_currency(est_materials_wip_kpi)}</h2></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='kpi-group-container'><div class='kpi-group-title'>Work In Progress (WIP) Actuals & Payments</div>", unsafe_allow_html=True)
    kpi_cols_display3 = st.columns(3)
    kpi_cols_display3[0].markdown(f"<div class='metric-box'><h4>Actual Hours (WIP)</h4><h2>{format_hours(actual_hours_wip_kpi, 0)}</h2></div>", unsafe_allow_html=True)
    kpi_cols_display3[1].markdown(f"<div class='metric-box'><h4>Actual Material Cost (WIP)</h4><h2>{format_currency(total_actual_materials_wip_kpi)}</h2></div>", unsafe_allow_html=True)
    kpi_cols_display3[2].markdown(f"<div class='metric-box'><h4>Total Down Payments (WIP)</h4><h2>{format_currency(total_down_payments_wip_kpi)}</h2></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # ... (rest of your dashboard chart logic) ...
    st.markdown("---")
    chart_selection_dash = st.radio(
        "Select Chart View:",
        ("Job Status Breakdown", "Hours Comparison (WIP)", "Material Cost Comparison (WIP)"),
        horizontal=True,
        key="dashboard_chart_selector_main"
    )

    if chart_selection_dash == "Job Status Breakdown":
        st.subheader("Job Status Distribution")
        if not kpi_df_filtered.empty and 'Status' in kpi_df_filtered.columns and not kpi_df_filtered['Status'].dropna().empty:
            status_counts_chart = kpi_df_filtered['Status'].astype(str).str.strip().value_counts().reset_index()
            status_counts_chart.columns = ['Status', 'Count']
            fig_status = px.bar(status_counts_chart, x='Status', y='Count', text='Count', template="plotly_dark", color='Status')
            fig_status.update_traces(textfont_size=16, textposition='outside')
            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.info("No job data with status information for chart based on current filters.")

    elif chart_selection_dash == "Hours Comparison (WIP)":
        st.subheader("Estimated vs. Actual Hours per Job (WIP Jobs Only)")
        if not df_wip_kpis.empty:
            compare_hours_df = df_wip_kpis[['Job Name', 'Estimated Hours', 'UniqueID']].copy()
            actual_hours_grouped_wip = job_time_df[job_time_df['JobUniqueID'].isin(wip_job_uids_kpi)].groupby('JobUniqueID')['Time Duration (Hours)'].sum().reset_index()
            actual_hours_grouped_wip.rename(columns={'Time Duration (Hours)': 'Actual Hours', 'JobUniqueID': 'UniqueID'}, inplace=True)
            compare_hours_df = compare_hours_df.merge(actual_hours_grouped_wip, on='UniqueID', how='left').fillna({'Actual Hours': 0.0})

            if not compare_hours_df.empty:
                melted_hours_chart = compare_hours_df.melt(id_vars='Job Name', value_vars=['Estimated Hours', 'Actual Hours'], var_name='Metric', value_name='Hours')
                fig_hours_compare = px.bar(
                    melted_hours_chart, x='Job Name', y='Hours', color='Metric', barmode='group',
                    title='Estimated vs. Actual Hours (WIP)', template="plotly_dark", text='Hours',
                    color_discrete_map={'Estimated Hours': '#FF6347', 'Actual Hours': '#1E90FF'}
                )
                fig_hours_compare.update_traces(texttemplate='%{text:.1f} hrs', textposition='outside')
                st.plotly_chart(fig_hours_compare, use_container_width=True)
            else:
                st.info("No WIP job data with hours to compare.")
        else:
            st.info("No Work In Progress jobs to display hours comparison.")

    elif chart_selection_dash == "Material Cost Comparison (WIP)":
        st.subheader("Estimated vs. Actual Material Costs per Job (WIP Jobs Only)")
        if not df_wip_kpis.empty:
            compare_mats_df = df_wip_kpis[['Job Name', 'Estimated Materials Cost', 'UniqueID']].copy()
            mats_usage_cost_wip = materials_df[materials_df['JobUniqueID'].isin(wip_job_uids_kpi)].groupby('JobUniqueID')['Amount'].sum().reset_index().rename(columns={'Amount': 'MatUsage', 'JobUniqueID': 'UniqueID'})
            receipts_cost_wip = receipts_df[receipts_df['JobUniqueID'].isin(wip_job_uids_kpi)].groupby('JobUniqueID')['Amount'].sum().reset_index().rename(columns={'Amount': 'ReceiptsCost', 'JobUniqueID': 'UniqueID'})

            compare_mats_df = compare_mats_df.merge(mats_usage_cost_wip, on='UniqueID', how='left').fillna({'MatUsage': 0.0})
            compare_mats_df = compare_mats_df.merge(receipts_cost_wip, on='UniqueID', how='left').fillna({'ReceiptsCost': 0.0})
            compare_mats_df['Actual Material Cost'] = compare_mats_df['MatUsage'] + compare_mats_df['ReceiptsCost']

            if not compare_mats_df.empty:
                melted_mats_chart = compare_mats_df.melt(id_vars='Job Name', value_vars=['Estimated Materials Cost', 'Actual Material Cost'], var_name='Metric', value_name='Cost')
                fig_mats_compare = px.bar(
                    melted_mats_chart, x='Job Name', y='Cost', color='Metric', barmode='group',
                    title='Estimated vs. Actual Material Costs (WIP)', template="plotly_dark", text='Cost',
                    color_discrete_map={'Estimated Materials Cost': '#FF6347', 'Actual Material Cost': '#1E90FF'}
                )
                fig_mats_compare.update_traces(texttemplate='$%{text:,.2f}', textposition='outside')
                st.plotly_chart(fig_mats_compare, use_container_width=True)
            else:
                st.info("No WIP job data with material costs to compare.")
        else:
            st.info("No Work In Progress jobs to display material cost comparison.")

    st.markdown("---")
    st.subheader("Job Details Overview (Filtered by Dashboard selections)")
    display_paginated_dataframe(
        kpi_df_filtered.sort_values(by="Start Date", ascending=False),
        "dash_jobs_overview_paginated",
        styler_fn=highlight_job_deadlines,
        col_config={
            "UniqueID": None,
            "Description": st.column_config.TextColumn(width="medium"),
            "Start Date": st.column_config.DateColumn(format="YYYY-MM-DD"),
            "End Date": st.column_config.DateColumn(format="YYYY-MM-DD")
        }
    )

elif section == 'Job Details':
    st.header("Job Details Management")

    if current_user_role_val == 'Admin':
        with st.expander("Add New Job", expanded=False):
            with st.form("new_job_form_jd", clear_on_submit=True):
                st.subheader("Add New Job Details")
                job_name_jd_new = st.text_input("Job Name*", key="jd_new_name")
                client_jd_new = st.text_input("Client*", key="jd_new_client")
                
                # Address Fields
                st.write("Client Address")
                c1, c2 = st.columns(2)
                address_jd_new = c1.text_input("Street Address", key="jd_new_address")
                city_jd_new = c2.text_input("City", key="jd_new_city")
                s1, s2 = st.columns(2)
                state_jd_new = s1.text_input("State", key="jd_new_state")
                zip_jd_new = s2.text_input("Zip Code", key="jd_new_zip")

                status_options_jd_new = ["Planning", "In Progress", "On Hold", "Completed", "Cancelled"]
                status_jd_new = st.selectbox("Status*", status_options_jd_new, key="jd_new_status", index=0)
                start_date_jd_new = st.date_input("Start Date", value=None, key="jd_new_start_date")
                end_date_jd_new = st.date_input("End Date", value=None, key="jd_new_end_date")
                description_jd_new = st.text_area("Description", key="jd_new_desc")
                est_hours_jd_new = st.number_input("Estimated Hours", min_value=0.0, step=0.5, format="%.1f", key="jd_new_est_hours")
                est_mat_cost_jd_new = st.number_input("Estimated Materials Cost ($)", min_value=0.0, step=0.01, format="%.2f", key="jd_new_est_mat_cost")

                if st.form_submit_button("Add Job"):
                    if not job_name_jd_new or not client_jd_new or not status_jd_new:
                        st.error("Job Name, Client, and Status are required.")
                    else:
                        new_job_rec = {
                            'Job Name': job_name_jd_new.strip(), 
                            'Client': client_jd_new.strip(), 
                            'Status': status_jd_new,
                            'Start Date': pd.to_datetime(start_date_jd_new, errors='coerce').date() if start_date_jd_new else None,
                            'End Date': pd.to_datetime(end_date_jd_new, errors='coerce').date() if end_date_jd_new else None,
                            'Description': description_jd_new.strip(), 
                            'Estimated Hours': est_hours_jd_new,
                            'Estimated Materials Cost': est_mat_cost_jd_new, 
                            'UniqueID': uuid.uuid4().hex,
    
                            # This part was missing the correct values
                            'ClientAddress': address_jd_new, 
                            'ClientCity': city_jd_new,
                            'ClientState': state_jd_new, 
                            'ClientZip': zip_jd_new
                        }

                        updated_jobs_df = pd.concat([jobs_df, pd.DataFrame([new_job_rec])], ignore_index=True)
                        save_data(updated_jobs_df, 'jobs')
                        st.success(f"Job '{job_name_jd_new}' added!")
                        st.cache_data.clear()
                        st.rerun()
        st.markdown("---")

    st.subheader("Existing Jobs")

    # --- Search Bar ---
    search_query = st.text_input("Search by Job Name or Client Name:", placeholder="Type here to search...")
    jobs_display_jd = jobs_df.copy()

    if search_query:
        search_query_lower = search_query.lower()
        jobs_display_jd = jobs_df[
            jobs_df['Job Name'].str.lower().str.contains(search_query_lower) |
            jobs_df['Client'].str.lower().str.contains(search_query_lower)
        ]

    if current_user_role_val == 'Client Viewer' and associated_client_name_val:
        jobs_display_jd = jobs_display_jd[jobs_display_jd['Client'].astype(str).str.strip() == associated_client_name_val.strip()]

    display_paginated_dataframe(jobs_display_jd.sort_values(by="Start Date", ascending=False),
                                "jd_page_display", styler_fn=highlight_job_deadlines,
                                col_config={"UniqueID": None, "Description": st.column_config.TextColumn(width="large"),
                                            "Start Date": st.column_config.DateColumn(format="YYYY-MM-DD"),
                                            "End Date": st.column_config.DateColumn(format="YYYY-MM-DD"),
                                            "ClientAddress": None, "ClientCity": None, "ClientState": None, "ClientZip": None})

    if current_user_role_val == 'Admin':
        st.markdown("---"); st.subheader("Edit or Delete Job")
        job_to_edit_options = ["Select..."] + sorted(list(jobs_display_jd['Job Name'].unique()))
        job_to_edit_select_admin_jd = st.selectbox("Select Job from filtered list to Edit/Delete:",
                                                   options=job_to_edit_options, key="jd_admin_job_select")

        if job_to_edit_select_admin_jd != "Select...":
            job_data_series_admin_jd = jobs_display_jd[jobs_display_jd['Job Name'] == job_to_edit_select_admin_jd]
            if not job_data_series_admin_jd.empty:
                job_data_edit_admin_jd = job_data_series_admin_jd.iloc[0]
                job_uid_edit_admin_jd = job_data_edit_admin_jd['UniqueID']
                with st.form(f"edit_job_form_{job_uid_edit_admin_jd}"):
                    st.write(f"Editing Job: {job_data_edit_admin_jd['Job Name']}")
                    edit_name_val = st.text_input("Job Name", value=job_data_edit_admin_jd['Job Name'], key=f"ej_name_{job_uid_edit_admin_jd}")
                    edit_client_val = st.text_input("Client", value=job_data_edit_admin_jd['Client'], key=f"ej_client_{job_uid_edit_admin_jd}")
                    
                    st.write("Edit Client Address")
                    c1_edit, c2_edit = st.columns(2)
                    address_jd_edit = c1_edit.text_input("Street Address", value=job_data_edit_admin_jd['ClientAddress'], key=f"ej_addr_{job_uid_edit_admin_jd}")
                    city_jd_edit = c2_edit.text_input("City", value=job_data_edit_admin_jd['ClientCity'], key=f"ej_city_{job_uid_edit_admin_jd}")
                    s1_edit, s2_edit = st.columns(2)
                    state_jd_edit = s1_edit.text_input("State", value=job_data_edit_admin_jd['ClientState'], key=f"ej_state_{job_uid_edit_admin_jd}")
                    zip_jd_edit = s2_edit.text_input("Zip Code", value=job_data_edit_admin_jd['ClientZip'], key=f"ej_zip_{job_uid_edit_admin_jd}")

                    status_opts_ej = ["Planning", "In Progress", "On Hold", "Completed", "Cancelled"]
                    status_idx_ej = status_opts_ej.index(job_data_edit_admin_jd['Status']) if job_data_edit_admin_jd['Status'] in status_opts_ej else 0
                    edit_status_val = st.selectbox("Status", status_opts_ej, index=status_idx_ej, key=f"ej_status_{job_uid_edit_admin_jd}")

                    sdate_ej_val = pd.to_datetime(job_data_edit_admin_jd['Start Date'], errors='coerce').date() if pd.notna(job_data_edit_admin_jd['Start Date']) else None
                    edate_ej_val = pd.to_datetime(job_data_edit_admin_jd['End Date'], errors='coerce').date() if pd.notna(job_data_edit_admin_jd['End Date']) else None
                    edit_sdate_val = st.date_input("Start Date", value=sdate_ej_val, key=f"ej_sdate_{job_uid_edit_admin_jd}")
                    edit_edate_val = st.date_input("End Date", value=edate_ej_val, key=f"ej_edate_{job_uid_edit_admin_jd}")

                    edit_desc_val = st.text_area("Description", value=job_data_edit_admin_jd['Description'], key=f"ej_desc_{job_uid_edit_admin_jd}")
                    edit_eh_val = st.number_input("Est. Hours", value=float(job_data_edit_admin_jd['Estimated Hours']), format="%.1f", key=f"ej_eh_{job_uid_edit_admin_jd}")
                    edit_emc_val = st.number_input("Est. Mat. Cost ($)", value=float(job_data_edit_admin_jd['Estimated Materials Cost']), format="%.2f", key=f"ej_emc_{job_uid_edit_admin_jd}")

                    save_col_ej_btn, del_col_ej_btn = st.columns(2)
                    if save_col_ej_btn.form_submit_button("Save Changes"):
                        if not edit_name_val or not edit_client_val: st.error("Job Name and Client are required.")
                        else:
                            idx_update_ej_q = jobs_df[jobs_df['UniqueID'] == job_uid_edit_admin_jd].index
                            if not idx_update_ej_q.empty:
                                jobs_df.loc[idx_update_ej_q[0], 'Job Name'] = edit_name_val.strip()
                                jobs_df.loc[idx_update_ej_q[0], 'Client'] = edit_client_val.strip()
                                jobs_df.loc[idx_update_ej_q[0], 'ClientAddress'] = address_jd_edit
                                jobs_df.loc[idx_update_ej_q[0], 'ClientCity'] = city_jd_edit
                                jobs_df.loc[idx_update_ej_q[0], 'ClientState'] = state_jd_edit
                                jobs_df.loc[idx_update_ej_q[0], 'ClientZip'] = zip_jd_edit
                                jobs_df.loc[idx_update_ej_q[0], 'Status'] = edit_status_val
                                jobs_df.loc[idx_update_ej_q[0], 'Start Date'] = pd.to_datetime(edit_sdate_val,errors='coerce').date() if edit_sdate_val else None
                                jobs_df.loc[idx_update_ej_q[0], 'End Date'] = pd.to_datetime(edit_edate_val,errors='coerce').date() if edit_edate_val else None
                                jobs_df.loc[idx_update_ej_q[0], 'Description'] = edit_desc_val.strip()
                                jobs_df.loc[idx_update_ej_q[0], 'Estimated Hours'] = edit_eh_val
                                jobs_df.loc[idx_update_ej_q[0], 'Estimated Materials Cost'] = edit_emc_val
                                save_data(jobs_df, 'jobs')
                                st.success(f"Job '{edit_name_val}' updated!")
                                st.cache_data.clear()
                                st.rerun()
                            else: st.error("Job not found for update. Refresh.")
                    if del_col_ej_btn.form_submit_button("Delete Job", type="primary"):
                        st.session_state[f"confirm_del_job_f_{job_uid_edit_admin_jd}"] = True

                if st.session_state.get(f"confirm_del_job_f_{job_uid_edit_admin_jd}", False):
                    st.warning(f"Delete job: **{job_data_edit_admin_jd['Job Name']}** and ALL its associated data? This cannot be undone.")
                    cd1, cd2 = st.columns(2)
                    if cd1.button("YES, DELETE JOB AND ALL DATA", key=f"del_job_yes_btn_{job_uid_edit_admin_jd}"):
                        job_time_df_new = job_time_df[job_time_df['JobUniqueID'] != job_uid_edit_admin_jd]
                        materials_df_new = materials_df[materials_df['JobUniqueID'] != job_uid_edit_admin_jd]
                        receipts_df_new = receipts_df[receipts_df['JobUniqueID'] != job_uid_edit_admin_jd]
                        down_payments_df_new = down_payments_df[down_payments_df['JobUniqueID'] != job_uid_edit_admin_jd]
                        job_files_df_new = job_files_df[job_files_df['JobUniqueID'] != job_uid_edit_admin_jd]
                        jobs_df_new = jobs_df[jobs_df['UniqueID'] != job_uid_edit_admin_jd]
                        
                        save_data(job_time_df_new, 'job_time'); save_data(materials_df_new, 'materials')
                        save_data(receipts_df_new, 'receipts'); save_data(down_payments_df_new, 'down_payments')
                        save_data(job_files_df_new, 'job_files'); save_data(jobs_df_new, 'jobs')
                        
                        del st.session_state[f"confirm_del_job_f_{job_uid_edit_admin_jd}"]
                        st.success(f"Job '{job_to_edit_select_admin_jd}' and related data deleted.")
                        st.cache_data.clear()
                        st.rerun()
                    if cd2.button("CANCEL JOB DELETION", key=f"del_job_no_btn_{job_uid_edit_admin_jd}"):
                        del st.session_state[f"confirm_del_job_f_{job_uid_edit_admin_jd}"]
                        st.rerun()
elif section == 'Job Time Tracking':
    st.header("Job Time Tracking")

    # Filters for displaying time entries
    time_df_display_jtt = job_time_df.copy()
    job_choices_jtt_filter = ["All Jobs"] + sorted(jobs_df['Job Name'].astype(str).str.strip().unique())
    contractor_names_for_filter_jtt = users_df[users_df['Role'].isin(['Contractor', 'Admin', 'Manager'])]['FirstName'].astype(str).str.strip().unique()
    contractor_choices_jtt_filter = ["All Contractors"] + sorted(list(contractor_names_for_filter_jtt))

    selected_contractor_jtt_disp_filter = "All Contractors"
    if current_user_role_val == 'Contractor':
        time_df_display_jtt = time_df_display_jtt[time_df_display_jtt['Contractor'] == current_user_fullname_val]
        selected_contractor_jtt_disp_filter = st.selectbox("Contractor:", options=[current_user_fullname_val],
                                                           key="jtt_contractor_filter_contractor_view", disabled=True)
    elif current_user_role_val == 'Client Viewer' and associated_client_name_val:
        time_df_display_jtt = time_df_display_jtt[time_df_display_jtt['Client'].astype(str).str.strip() == associated_client_name_val.strip()]
        selected_contractor_jtt_disp_filter = st.selectbox("Filter by Contractor:", options=contractor_choices_jtt_filter,
                                                           key="jtt_contractor_filter_client_viewer")
        if selected_contractor_jtt_disp_filter != "All Contractors":
            time_df_display_jtt = time_df_display_jtt[time_df_display_jtt['Contractor'] == selected_contractor_jtt_disp_filter]
    else: # Admin, Manager
        selected_contractor_jtt_disp_filter = st.selectbox("Filter by Contractor:", options=contractor_choices_jtt_filter,
                                                           key="jtt_contractor_filter_admin_manager")
        if selected_contractor_jtt_disp_filter != "All Contractors":
            time_df_display_jtt = time_df_display_jtt[time_df_display_jtt['Contractor'] == selected_contractor_jtt_disp_filter]

    selected_job_jtt_disp_filter = st.selectbox("Filter by Job:", options=job_choices_jtt_filter, key="jtt_job_filter_display")
    if selected_job_jtt_disp_filter != "All Jobs":
        time_df_display_jtt = time_df_display_jtt[time_df_display_jtt['Job'] == selected_job_jtt_disp_filter]

    # Form for adding new time entry
    if current_user_role_val in ['Contractor', 'Admin', 'Manager']:
        st.subheader("Add New Time Entry")
        with st.form("new_time_entry_form_jtt", clear_on_submit=True):
            contractor_for_new_entry_jtt = ""
            if current_user_role_val == 'Contractor':
                contractor_for_new_entry_jtt = current_user_fullname_val
                st.text_input("Contractor (Auto-filled)", value=contractor_for_new_entry_jtt, disabled=True, key="jtt_new_time_contractor_auto")
            else: # Admin or Manager can select
                assignable_contractors_jtt = [c for c in contractor_choices_jtt_filter if c != "All Contractors"]
                if not assignable_contractors_jtt: st.warning("No contractors available to assign time entry.")
                else: contractor_for_new_entry_jtt = st.selectbox("Contractor*", options=assignable_contractors_jtt, key="jtt_new_time_contractor_select")

            form_jobs_available_jtt = jobs_df.copy()
            job_options_new_entry_jtt = ["Select Job"] + sorted(form_jobs_available_jtt['Job Name'].astype(str).str.strip().unique().tolist())
            selected_job_new_entry_jtt = st.selectbox("Job*", options=job_options_new_entry_jtt, key="jtt_new_time_job_select_specific")

            client_for_new_entry_jtt = ""
            job_uid_for_new_entry_jtt = "ERROR_NO_UID"
            if selected_job_new_entry_jtt and selected_job_new_entry_jtt != "Select Job":
                job_data_series_jtt = jobs_df[jobs_df['Job Name'] == selected_job_new_entry_jtt]
                if not job_data_series_jtt.empty:
                    job_data_jtt = job_data_series_jtt.iloc[0]
                    client_for_new_entry_jtt = job_data_jtt['Client']
                    job_uid_for_new_entry_jtt = job_data_jtt['UniqueID']
                st.text_input("Client (Auto-filled)", value=client_for_new_entry_jtt, disabled=True, key="jtt_new_time_client_auto")
            else:
                st.text_input("Client (Will auto-fill after job selection)", value="", disabled=True, key="jtt_new_time_client_placeholder")

            date_new_entry_jtt = st.date_input("Date*", value=datetime.date.today(), key="jtt_new_time_date")
            start_time_new_entry_jtt = st.time_input("Start Time*", value=datetime.time(9, 0), key="jtt_new_time_start")
            end_time_new_entry_jtt = st.time_input("End Time*", value=datetime.time(17, 0), key="jtt_new_time_end")

            if st.form_submit_button("Add Time Entry"):
                if not (contractor_for_new_entry_jtt and selected_job_new_entry_jtt != "Select Job" and \
                        date_new_entry_jtt and start_time_new_entry_jtt and end_time_new_entry_jtt and \
                        client_for_new_entry_jtt and job_uid_for_new_entry_jtt != "ERROR_NO_UID"):
                    st.error("All fields (*) are required. Ensure Job is selected and Client auto-fills.")
                elif end_time_new_entry_jtt <= start_time_new_entry_jtt:
                    st.error("End Time must be after Start Time.")
                else:
                    duration_jtt = (datetime.datetime.combine(date_new_entry_jtt, end_time_new_entry_jtt) -
                                    datetime.datetime.combine(date_new_entry_jtt, start_time_new_entry_jtt)).total_seconds() / 3600

                    new_entry_record_jtt = {'Contractor': contractor_for_new_entry_jtt,
                                            'Client': client_for_new_entry_jtt,
                                            'Job': selected_job_new_entry_jtt,
                                            'Date': date_new_entry_jtt,
                                            'Start Time': start_time_new_entry_jtt.strftime('%H:%M'),
                                            'End Time': end_time_new_entry_jtt.strftime('%H:%M'),
                                            'Time Duration (Hours)': duration_jtt,
                                            'UniqueID': uuid.uuid4().hex,
                                            'JobUniqueID': job_uid_for_new_entry_jtt}

                    updated_job_time_df = pd.concat([job_time_df, pd.DataFrame([new_entry_record_jtt])], ignore_index=True)
                    save_data(updated_job_time_df, 'job_time')
                    job_time_df = load_data('job_time')
                    st.success("Time entry added successfully!"); st.rerun()

    st.markdown("---"); st.subheader("Time Entries Log")
    display_paginated_dataframe(time_df_display_jtt.sort_values(by="Date", ascending=False),
                                "jtt_time_entries_paginated", 10,
                                col_config={"UniqueID": None, "JobUniqueID": None,
                                            "Date": st.column_config.DateColumn(format="YYYY-MM-DD")})
    # ... (Your logic for editing and deleting time entries) ...

elif section == 'Material Usage':
    #st.write(materials_df.columns)
    st.header("Material Usage")

    materials_df_display_mu = materials_df.copy()

    job_choices_mu_filter = ["All Jobs"] + sorted(list(jobs_df['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
    if not users_df.empty:
        contractor_names_for_filter_mu = users_df[users_df['Role'].isin(['Contractor', 'Admin', 'Manager'])]['FirstName'].astype(str).str.strip().unique()
        contractor_choices_mu_filter = ["All Contractors"] + sorted(list(contractor_names_for_filter_mu))
    else:
        contractor_choices_mu_filter = ["All Contractors"]
        st.warning("User data for contractor filtering is unavailable.")

    selected_contractor_mu_disp_filter = "All Contractors"
    if current_user_role_val == 'Contractor':
        materials_df_display_mu = materials_df_display_mu[materials_df_display_mu['Contractor'] == current_user_fullname_val]
        selected_contractor_mu_disp_filter = st.selectbox("Contractor:", options=[current_user_fullname_val],
                                                          key="mu_contractor_filter_user_view", disabled=True)
    elif current_user_role_val == 'Client Viewer' and associated_client_name_val:
        materials_df_display_mu = materials_df_display_mu[materials_df_display_mu['Client'].astype(str).str.strip() == associated_client_name_val.strip()]
        selected_contractor_mu_disp_filter = st.selectbox("Filter by Contractor:", options=contractor_choices_mu_filter,
                                                          key="mu_contractor_filter_client_view")
        if selected_contractor_mu_disp_filter != "All Contractors":
            materials_df_display_mu = materials_df_display_mu[materials_df_display_mu['Contractor'] == selected_contractor_mu_disp_filter]
    else: # Admin, Manager
        selected_contractor_mu_disp_filter = st.selectbox("Filter by Contractor:", options=contractor_choices_mu_filter,
                                                          key="mu_contractor_filter_admin_manager")
        if selected_contractor_mu_disp_filter != "All Contractors":
            materials_df_display_mu = materials_df_display_mu[materials_df_display_mu['Contractor'] == selected_contractor_mu_disp_filter]

    selected_job_mu_disp_filter = st.selectbox("Filter by Job:", options=job_choices_mu_filter, key="mu_job_filter_display")
    if selected_job_mu_disp_filter != "All Jobs":
        materials_df_display_mu = materials_df_display_mu[materials_df_display_mu['Job'] == selected_job_mu_disp_filter]

    if current_user_role_val in ['Contractor', 'Admin', 'Manager']:
        st.subheader("Add New Material Entry")
        with st.form("new_material_entry_form_mu_section", clear_on_submit=True):
            material_name_mu_form = st.text_input("Material Name*", key="mu_form_material_name")
            amount_mu_form = st.number_input("Amount ($)*", min_value=0.00, step=0.01, format="%.2f", key="mu_form_amount")
            contractor_input_mu_form_val = ""
            if current_user_role_val == 'Contractor':
                contractor_input_mu_form_val = current_user_fullname_val
                st.text_input("Contractor (Auto-filled)", value=contractor_input_mu_form_val, disabled=True, key="mu_form_contractor_auto")
            else:
                assignable_contractors_mu_list = [c for c in contractor_choices_mu_filter if c != "All Contractors"]
                if not assignable_contractors_mu_list:
                    st.warning("No contractors available to assign material entry.")
                else:
                    contractor_input_mu_form_val = st.selectbox("Contractor*", options=assignable_contractors_mu_list, key="mu_form_contractor_select")

            job_options_mu_form = ["Select Job"] + sorted(list(jobs_df['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
            selected_job_name_mu_form_val = st.selectbox("Job*", options=job_options_mu_form, key="mu_form_job_select")

            client_name_mu_form_val = ""
            job_uid_mu_form_val = "ERROR_UID_NOT_FOUND"
            if selected_job_name_mu_form_val and selected_job_name_mu_form_val != "Select Job":
                job_data_mu_series = jobs_df[jobs_df['Job Name'] == selected_job_name_mu_form_val]
                if not job_data_mu_series.empty:
                    job_data_mu_row = job_data_mu_series.iloc[0]
                    client_name_mu_form_val = job_data_mu_row['Client']
                    job_uid_mu_form_val = job_data_mu_row['UniqueID']
                st.text_input("Client (Auto-filled)", value=client_name_mu_form_val, disabled=True, key="mu_form_client_auto")
            else:
                st.text_input("Client (Will auto-fill after job selection)", value="", disabled=True, key="mu_form_client_placeholder")

            date_used_mu_form_val = st.date_input("Date Used*", value=datetime.date.today(), key="mu_form_date_used")
            payor_query_mu_form_text = st.text_input("Payor (start typing for suggestions or enter new)*", key="mu_form_payor_text_input")

            if st.form_submit_button("Add Material Entry"):
                if not (material_name_mu_form and amount_mu_form is not None and contractor_input_mu_form_val and \
                        selected_job_name_mu_form_val != "Select Job" and date_used_mu_form_val and \
                        payor_query_mu_form_text.strip() and client_name_mu_form_val and job_uid_mu_form_val != "ERROR_UID_NOT_FOUND"):
                    st.error("All fields (*) are required. Ensure Job is selected, Client auto-fills, and Payor is entered.")
                else:
                    new_material_record = {
                        'Material': material_name_mu_form.strip(),
                        'Contractor': contractor_input_mu_form_val,
                        'Client': client_name_mu_form_val,
                        'Job': selected_job_name_mu_form_val,
                        'Date Used': date_used_mu_form_val,
                        'Amount': amount_mu_form,
                        'Payor': payor_query_mu_form_text.strip(),
                        'UniqueID': uuid.uuid4().hex,
                        'JobUniqueID': job_uid_mu_form_val
                    }
                    updated_materials_df = pd.concat([materials_df, pd.DataFrame([new_material_record])], ignore_index=True)
                    save_data(updated_materials_df, 'materials')
                    materials_df = load_data('materials')
                    st.success("Material entry added successfully!"); st.rerun()

    st.markdown("---"); st.subheader("Material Entries Log")
    display_paginated_dataframe(materials_df_display_mu.sort_values(by="Date Used", ascending=False),
                                "mu_entries_paginated_display", 10,
                                col_config={"UniqueID": None, "JobUniqueID": None,
                                            "Date Used": st.column_config.DateColumn(format="YYYY-MM-DD"),
                                            "Amount": st.column_config.NumberColumn(format="$%.2f")})
    # ... (Your logic for editing and deleting material entries) ...

elif section == 'Upload Receipt':
    st.header("Upload Receipt")

    if current_user_role_val in ['Contractor', 'Manager', 'Admin']:
        job_options_ur_form = ["Select Job"] + sorted(list(jobs_df['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
        selected_job_ur_form = st.selectbox("Select Job for Receipt*", options=job_options_ur_form, key="ur_form_job_select")

        client_name_ur_form = ""
        job_uid_ur_form = "ERROR_UID_UR"
        if selected_job_ur_form != "Select Job":
            job_data_ur_series = jobs_df[jobs_df['Job Name'] == selected_job_ur_form]
            if not job_data_ur_series.empty:
                job_data_ur_row = job_data_ur_series.iloc[0]
                client_name_ur_form = job_data_ur_row['Client']
                job_uid_ur_form = job_data_ur_row['UniqueID']
            st.text_input("Client Name (Auto-filled)", value=client_name_ur_form, disabled=True, key="ur_form_client_auto")
        else:
            st.text_input("Client Name (Will auto-fill after job selection)", value="", disabled=True, key="ur_form_client_placeholder")

        st.subheader("Upload New Receipt")
        with st.form("new_receipt_form_ur_section", clear_on_submit=True):
            contractor_name_ur_input = ""
            if not users_df.empty:
                contractor_choices_ur_list = sorted(list(users_df[users_df['Role'].isin(['Contractor', 'Admin', 'Manager'])]['FirstName'].astype(str).str.strip().unique()))
            else:
                contractor_choices_ur_list = []

            if current_user_role_val == 'Contractor':
                contractor_name_ur_input = current_user_fullname_val
                st.text_input("Contractor (Auto-filled)", value=contractor_name_ur_input, disabled=True, key="ur_form_contractor_auto")
            else:
                if not contractor_choices_ur_list:
                    st.warning("No contractors available in user list.")
                else:
                    contractor_name_ur_input = st.selectbox("Contractor Name (who incurred cost)*", options=[""] + contractor_choices_ur_list, key="ur_form_contractor_select")

            amount_ur_input_val = st.number_input("Receipt Amount ($)*", min_value=0.01, step=0.01, format="%.2f", key="ur_form_amount")
            payor_query_ur_text = st.text_input("Payor (start typing or enter new)*", key="ur_form_payor_text")
            uploaded_file_data_ur = st.file_uploader("Upload Receipt File (PDF, PNG, JPG)*", type=['pdf', 'png', 'jpg', 'jpeg'], key="ur_form_file_uploader")

            if st.form_submit_button("Save Receipt Information"):
                if not (contractor_name_ur_input and selected_job_ur_form != "Select Job" and payor_query_ur_text.strip() and \
                        amount_ur_input_val and uploaded_file_data_ur and client_name_ur_form and job_uid_ur_form != "ERROR_UID_UR"):
                    st.error("Please fill all required fields (*), select a job, and upload a receipt file.")
                else:
                    with st.spinner("Uploading file and saving info..."):
                        unique_filename = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{uploaded_file_data_ur.name}"
                        receipt_link = upload_file_to_drive(uploaded_file_data_ur, unique_filename, DRIVE_FOLDER_ID_RECEIPTS)

                        if receipt_link:
                            new_receipt_record = {
                                'Contractor Name': contractor_name_ur_input.strip().title(),
                                'Client Name': client_name_ur_form.strip(),
                                'Job Name': selected_job_ur_form.strip(),
                                'Payor': payor_query_ur_text.strip(),
                                'Amount': amount_ur_input_val,
                                'File Name': uploaded_file_data_ur.name,
                                'File Path': receipt_link,
                                'Upload Date': datetime.datetime.now().isoformat(),
                                'UniqueID': uuid.uuid4().hex,
                                'JobUniqueID': job_uid_ur_form
                            }
                            updated_receipts_df = pd.concat([receipts_df, pd.DataFrame([new_receipt_record])], ignore_index=True)
                            save_data(updated_receipts_df, 'receipts')
                            receipts_df = load_data('receipts')
                            st.success(f"Receipt '{uploaded_file_data_ur.name}' uploaded and info saved for job '{selected_job_ur_form}'!")
                            st.rerun()
                        else:
                            st.error("File upload to Google Drive failed. Receipt info not saved.")
        st.markdown("---")

    st.subheader("Uploaded Receipts Log")
    receipts_df_display_main_ur = receipts_df.copy()
    if current_user_role_val == 'Client Viewer' and associated_client_name_val:
        receipts_df_display_main_ur = receipts_df_display_main_ur[receipts_df_display_main_ur['Client Name'].astype(str).str.strip() == associated_client_name_val.strip()]

    client_list_ur_view_filter = ["All Clients"] + (sorted(list(receipts_df_display_main_ur['Client Name'].astype(str).str.strip().replace('',np.nan).dropna().unique())))
    sel_client_ur_view = st.selectbox("Filter by Client (View):", client_list_ur_view_filter,
                                      key="ur_view_client_filter",
                                      index=client_list_ur_view_filter.index(st.session_state.get("selected_client_receipt_view", client_list_ur_view_filter[0])))
    st.session_state.selected_client_receipt_view = sel_client_ur_view
    if sel_client_ur_view != "All Clients":
        receipts_df_display_main_ur = receipts_df_display_main_ur[receipts_df_display_main_ur['Client Name'].astype(str).str.strip() == sel_client_ur_view.strip()]

    job_list_ur_view_filter = ["All Jobs"] + (sorted(list(receipts_df_display_main_ur['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique())))
    sel_job_ur_view = st.selectbox("Filter by Job (View):", job_list_ur_view_filter, key="ur_view_job_filter")
    if sel_job_ur_view != "All Jobs":
        receipts_df_display_main_ur = receipts_df_display_main_ur[receipts_df_display_main_ur['Job Name'].astype(str).str.strip() == sel_job_ur_view.strip()]

    display_paginated_dataframe(receipts_df_display_main_ur.sort_values(by="Upload Date", ascending=False),
                                "ur_receipts_log_paginated", page_size=5,
                                col_config={
                                    "File Path": st.column_config.LinkColumn("View Receipt", display_text="Open File ↗️"),
                                    "Upload Date": st.column_config.DatetimeColumn("Upload Date", format="YYYY-MM-DD HH:mm"),
                                    "Amount": st.column_config.NumberColumn("Amount",format="$%.2f"),
                                    "UniqueID": None, "JobUniqueID": None
                                })
    # ... (Your logic for editing and deleting receipts) ...

elif section == 'Down Payments Log':
    st.header("Down Payments Log")

    dp_df_display_dpl = down_payments_df.copy()
    if current_user_role_val == 'Client Viewer' and associated_client_name_val:
        relevant_job_uids_dpl_cv = jobs_df[jobs_df['Client'].astype(str).str.strip() == associated_client_name_val.strip()]['UniqueID'].tolist()
        dp_df_display_dpl = dp_df_display_dpl[dp_df_display_dpl['JobUniqueID'].isin(relevant_job_uids_dpl_cv)]

    if not dp_df_display_dpl.empty and not jobs_df.empty:
        job_info_map_dpl = jobs_df.set_index('UniqueID')[['Job Name', 'Client']].copy()
        missing_job_uids = set(dp_df_display_dpl['JobUniqueID']) - set(job_info_map_dpl.index)
        if missing_job_uids:
            missing_data = pd.DataFrame({'Job Name': ['Unknown Job'] * len(missing_job_uids),
                                         'Client': ['Unknown Client'] * len(missing_job_uids)},
                                        index=list(missing_job_uids))
            job_info_map_dpl = pd.concat([job_info_map_dpl, missing_data])

        dp_df_display_dpl = dp_df_display_dpl.join(job_info_map_dpl, on='JobUniqueID', how='left')
        dp_df_display_dpl.fillna({'Job Name': 'Unknown Job', 'Client': 'Unknown Client'}, inplace=True)
        job_options_dpl_filter = ["All Jobs"] + sorted(list(dp_df_display_dpl['Job Name'].astype(str).str.strip().replace('Unknown Job','').replace('',np.nan).dropna().unique()))
    else:
        job_options_dpl_filter = ["All Jobs"]
        if 'Job Name' not in dp_df_display_dpl.columns: dp_df_display_dpl['Job Name'] = pd.NA
        if 'Client' not in dp_df_display_dpl.columns: dp_df_display_dpl['Client'] = pd.NA

    selected_job_dpl_filter = st.selectbox("Filter by Job:", job_options_dpl_filter, key="dpl_job_filter_display")
    if selected_job_dpl_filter != "All Jobs":
        dp_df_display_dpl = dp_df_display_dpl[dp_df_display_dpl['Job Name'] == selected_job_dpl_filter]

    st.subheader("Down Payments Record")
    display_cols_dpl = ['Job Name', 'Client', 'DateReceived', 'Amount', 'PaymentMethod', 'Notes', 'DownPaymentID', 'JobUniqueID']
    for col_dpl_disp in display_cols_dpl:
        if col_dpl_disp not in dp_df_display_dpl.columns:
            dp_df_display_dpl[col_dpl_disp] = pd.NA

    display_paginated_dataframe(dp_df_display_dpl[display_cols_dpl].sort_values(by="DateReceived", ascending=False),
                                "dpl_paginated_log", 10,
                                col_config={"DownPaymentID": None, "JobUniqueID": None,
                                            "DateReceived": st.column_config.DateColumn(format="YYYY-MM-DD"),
                                            "Amount": st.column_config.NumberColumn(format="$%.2f")})

    if current_user_role_val in ['Admin', 'Manager']:
        st.markdown("---"); st.subheader("Add New Down Payment")
        with st.form("new_down_payment_form_dpl", clear_on_submit=True):
            job_options_dpl_form = ["Select Job"] + sorted(list(jobs_df['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
            selected_job_dpl_form = st.selectbox("Select Job for Down Payment*", options=job_options_dpl_form, key="dpl_form_job_select")
            dp_date_dpl_form = st.date_input("Date Received*", value=datetime.date.today(), key="dpl_form_date")
            dp_amount_dpl_form = st.number_input("Amount ($)*", min_value=0.01, step=0.01, format="%.2f", key="dpl_form_amount")
            dp_method_dpl_form = st.selectbox("Payment Method*", ["Cash", "Check", "Bank Transfer", "Credit Card", "Other"], key="dpl_form_method")
            dp_notes_dpl_form = st.text_area("Notes", key="dpl_form_notes")

            if st.form_submit_button("Add Down Payment"):
                if selected_job_dpl_form == "Select Job" or not dp_date_dpl_form or not dp_amount_dpl_form or not dp_method_dpl_form:
                    st.error("Please fill all required fields (*).")
                else:
                    job_data_dpl_series = jobs_df[jobs_df['Job Name'] == selected_job_dpl_form]
                    if not job_data_dpl_series.empty:
                        job_uid_dpl_form = job_data_dpl_series.iloc[0]['UniqueID']
                        new_dp_record = {
                            'DownPaymentID': uuid.uuid4().hex,
                            'JobUniqueID': job_uid_dpl_form,
                            'DateReceived': dp_date_dpl_form,
                            'Amount': dp_amount_dpl_form,
                            'PaymentMethod': dp_method_dpl_form,
                            'Notes': dp_notes_dpl_form.strip()
                        }
                        updated_dp_df = pd.concat([down_payments_df, pd.DataFrame([new_dp_record])], ignore_index=True)
                        save_data(updated_dp_df, 'down_payments')
                        down_payments_df = load_data('down_payments')
                        st.success(f"Down payment recorded for '{selected_job_dpl_form}'."); st.rerun()
                    else:
                        st.error(f"Job '{selected_job_dpl_form}' not found. Cannot record down payment.")
        # ... (Your logic for editing and deleting down payments) ...

elif section == 'Job File Uploads':
    st.header("Job File Uploads")

    # --- Job and Client Selection for Context ---
    job_file_jobs_filter_df_jfu = jobs_df.copy()
    if current_user_role_val == 'Client Viewer' and associated_client_name_val:
        job_file_jobs_filter_df_jfu = job_file_jobs_filter_df_jfu[job_file_jobs_filter_df_jfu['Client'].astype(str).strip() == associated_client_name_val.strip()]

    job_options_jfu_select = ["Select Job to View/Upload Files"] + sorted(list(job_file_jobs_filter_df_jfu['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
    selected_job_jfu_context = st.selectbox("Select Job:", options=job_options_jfu_select, key="jfu_job_context_select")

    client_name_jfu_context = ""
    job_uid_jfu_context = None
    if selected_job_jfu_context != "Select Job to View/Upload Files":
        job_data_jfu_series = jobs_df[jobs_df['Job Name'] == selected_job_jfu_context]
        if not job_data_jfu_series.empty:
            job_data_jfu_row = job_data_jfu_series.iloc[0]
            client_name_jfu_context = job_data_jfu_row['Client']
            job_uid_jfu_context = job_data_jfu_row['UniqueID']
        st.text_input("Client Name (for selected job):", value=client_name_jfu_context, disabled=True, key="jfu_client_context_auto")
    else:
        st.text_input("Client Name (Will auto-fill after job selection)", value="", disabled=True, key="jfu_client_context_placeholder")

    # --- File Upload Form ---
    if current_user_role_val in ['Admin', 'Manager', 'Contractor']:
        if selected_job_jfu_context != "Select Job to View/Upload Files" and job_uid_jfu_context:
            st.subheader(f"Upload New File for: {selected_job_jfu_context}")
            with st.form("new_job_file_upload_form_jfu", clear_on_submit=True):
                file_category_options_jfu = ["General", "Plans", "Photos", "Reports", "Estimate Pictures", "Change Order Pictures", "Work In Progress Pictures", "Final Pictures", "Other"]
                file_category_jfu_form = st.selectbox("File Category*", options=file_category_options_jfu, key="jfu_form_category_select")
                uploaded_job_file_data = st.file_uploader("Upload File*", type=None, key="jfu_form_file_uploader")

                if st.form_submit_button("Upload File"):
                    if not (file_category_jfu_form and uploaded_job_file_data):
                        st.error("Please select a category and upload a file.")
                    else:
                        with st.spinner("Uploading file to Google Drive..."):
                            unique_filename = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{uploaded_job_file_data.name}"
                            upload_link = upload_file_to_drive(uploaded_job_file_data, unique_filename, DRIVE_FOLDER_ID_JOB_FILES)

                            if upload_link:
                                new_job_file_record = {
                                    'FileID': uuid.uuid4().hex,
                                    'JobUniqueID': job_uid_jfu_context,
                                    'FileName': uploaded_job_file_data.name,
                                    'RelativePath': upload_link, # Store the Drive link
                                    'Category': file_category_jfu_form,
                                    'UploadDate': datetime.datetime.now().isoformat(),
                                    'UploadedByUsername': current_username_val
                                }
                                updated_job_files_df = pd.concat([job_files_df, pd.DataFrame([new_job_file_record])], ignore_index=True)
                                save_data(updated_job_files_df, 'job_files')
                                job_files_df = load_data('job_files')
                                st.success(f"File '{uploaded_job_file_data.name}' uploaded successfully!")
                                st.rerun()
                            else:
                                st.error("Failed to upload file to Google Drive.")
        elif selected_job_jfu_context == "Select Job to View/Upload Files":
            st.info("Select a job above to enable file uploading for that job.")

    # --- Display Existing Job Files ---
    st.markdown("---"); st.subheader("Existing Job Files")
    if selected_job_jfu_context != "Select Job to View/Upload Files" and job_uid_jfu_context:
        job_files_to_display_jfu = job_files_df[job_files_df['JobUniqueID'] == job_uid_jfu_context].copy()

        if not job_files_to_display_jfu.empty:
            job_files_to_display_jfu['Job Name'] = selected_job_jfu_context
            job_files_to_display_jfu['Client'] = client_name_jfu_context
            display_cols_jfu_list = ['FileName', 'Category', 'UploadDate', 'UploadedByUsername']

            st.write(f"Files for job: **{selected_job_jfu_context}** (Client: {client_name_jfu_context})")
            display_paginated_dataframe(job_files_to_display_jfu.sort_values(by="UploadDate", ascending=False),
                                        f"jfu_files_for_job_{job_uid_jfu_context}", 5,
                                        col_config={
                                            "RelativePath": st.column_config.LinkColumn("View File", display_text="Open File ↗️"),
                                            "UploadDate": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
                                            "FileID": None, "JobUniqueID": None,
                                        })
        else:
            st.info(f"No files uploaded yet for job '{selected_job_jfu_context}'.")
    elif selected_job_jfu_context == "Select Job to View/Upload Files":
        st.info("Select a job to view or upload associated files.")

# --- Other sections (Down Payments, Invoice, Reports) ---
# No major changes are needed in these sections unless they involve file I/O.
# Your save_data calls for down_payments.csv will just become save_data(..., 'down_payments')
# The PDF generation for invoices will try to save to a Google Drive folder.

elif section == 'Invoice Generation':
    st.header("Invoice Generation")
    if current_user_role_val in ['Admin', 'Manager']:
        st.subheader("Document Setup")

        # Company Details for PDF in sidebar
        st.sidebar.subheader("Company Details for PDF Document")
        st.sidebar.text_input("Company Name (PDF)", value=st.session_state.company_name_pdf, key="company_name_pdf")
        st.sidebar.text_input("Company Address (PDF)", value=st.session_state.company_address_pdf, key="company_address_pdf")
        st.sidebar.text_input("Company Phone (PDF)", value=st.session_state.company_phone_pdf, key="company_phone_pdf")
        st.sidebar.text_input("Company Email (PDF)", value=st.session_state.company_email_pdf, key="company_email_pdf")

        company_details_for_pdf_doc_ig = {
            "name": st.session_state.company_name_pdf,
            "address": st.session_state.company_address_pdf,
            "phone": st.session_state.company_phone_pdf,
            "email": st.session_state.company_email_pdf
        }

        doc_type_selected_ig = st.radio("Select Document Type:", ("Estimate", "Invoice"), key="ig_doc_type_radio", horizontal=True)

        # Client and Job Selection
        clients_available_ig = ["Select Client"] + (sorted(list(jobs_df['Client'].astype(str).str.strip().replace('',np.nan).dropna().unique())) if not jobs_df.empty else [])
        selected_client_for_doc_ig = st.selectbox("Client:", clients_available_ig, key="ig_client_doc_select")

        jobs_available_for_doc_ig = ["Select Job"]
        selected_job_data_for_doc_ig = None

        if selected_client_for_doc_ig != "Select Client" and not jobs_df.empty:
            jobs_of_selected_client_ig = jobs_df[jobs_df['Client'].astype(str).str.strip() == selected_client_for_doc_ig.strip()]
            if not jobs_of_selected_client_ig.empty:
                jobs_available_for_doc_ig.extend(sorted(list(jobs_of_selected_client_ig['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique())))

        selected_job_name_for_doc_ig = st.selectbox("Job:", jobs_available_for_doc_ig, key="ig_job_doc_select")
        
        # Auto-populate address and description when a job is selected
        job_description_for_doc_pdf_ig = "N/A"
        st.write("Client Address (auto-populated from Job Details)")
        if selected_job_name_for_doc_ig != "Select Job":
            job_data_query_for_doc_ig = jobs_df[jobs_df['Job Name'] == selected_job_name_for_doc_ig]
            if not job_data_query_for_doc_ig.empty:
                selected_job_data_for_doc_ig = job_data_query_for_doc_ig.iloc[0]
                job_description_for_doc_pdf_ig = selected_job_data_for_doc_ig['Description']
                full_address = (
                    f"{selected_job_data_for_doc_ig.get('ClientAddress', '')}, "
                    f"{selected_job_data_for_doc_ig.get('ClientCity', '')}, "
                    f"{selected_job_data_for_doc_ig.get('ClientState', '')} "
                    f"{selected_job_data_for_doc_ig.get('ClientZip', '')}"
                 )
                st.text_input("Full Address", value=full_address.strip(', '), disabled=True, key="ig_client_address_auto")
        else:
            st.text_input("Full Address", value="Select a job to see client address", disabled=True)

        # Document details with Sequential Numbering
        doc_prefix_for_num_ig = "EST" if doc_type_selected_ig == "Estimate" else "INV"
        last_num = 499
        if doc_type_selected_ig == "Estimate" and not estimates_df.empty and 'DocNumber' in estimates_df.columns:
            numeric_est_nums = pd.to_numeric(estimates_df['DocNumber'].str.replace('EST-', '', regex=False), errors='coerce').dropna()
            if not numeric_est_nums.empty: last_num = numeric_est_nums.max()
        elif doc_type_selected_ig == "Invoice" and not invoices_df.empty and 'DocNumber' in invoices_df.columns:
            numeric_inv_nums = pd.to_numeric(invoices_df['DocNumber'].str.replace('INV-', '', regex=False), errors='coerce').dropna()
            if not numeric_inv_nums.empty: last_num = numeric_inv_nums.max()
        
        next_doc_num = int(last_num) + 1
        doc_number_input_ig = st.text_input(f"{doc_type_selected_ig} Number*", value=f"{doc_prefix_for_num_ig}-{next_doc_num}", key="ig_doc_number_input")
        doc_date_input_ig = st.date_input(f"{doc_type_selected_ig} Date*", value=datetime.date.today(), key="ig_doc_date_input")
        tax_rate_input_ig = st.number_input("Tax Rate (%)", min_value=0.0, value=st.session_state.get("ig_tax_rate_val", 2.041), step=0.001, format="%.3f", key="ig_tax_rate_input")
        st.session_state.ig_tax_rate_val = tax_rate_input_ig
        default_notes_text_ig = f"This {doc_type_selected_ig.lower()} outlines the scope and costs." if doc_type_selected_ig == "Estimate" else "Thank you for your business! Payment is due upon receipt."
        doc_notes_input_ig = st.text_area(f"{doc_type_selected_ig} Notes", value=default_notes_text_ig, key="ig_doc_notes_input", height=100)
        st.session_state.invoice_terms = st.text_area("Terms & Conditions (shared for all documents)", value=st.session_state.get("invoice_terms", "Payment due upon receipt."), key="ig_shared_terms_input", height=100)
        
        # --- Checkbox logic ---
        # --- Checkbox logic ---
        st.markdown("---"); st.subheader("Line Items Configuration")
        auto_items_for_current_invoice_ig = []
        if selected_job_data_for_doc_ig is not None:
            job_details_for_items_ig = selected_job_data_for_doc_ig
            job_uid_for_items_ig = job_details_for_items_ig['UniqueID']

            st.markdown("##### Automatic Line Item Options")
            col_auto_opt1_ig, col_auto_opt2_ig = st.columns(2)
            with col_auto_opt1_ig:
                cfg_inc_job_est_mat_cost = st.checkbox("Job Estimated Material Cost", key="ig_cfg_job_est_mat")
                cfg_inc_job_est_time = st.checkbox("Job Estimated Time (Hours)", key="ig_cfg_job_est_time")
                if cfg_inc_job_est_time:
                    st.session_state.inv_est_time_job_rate = st.number_input("Rate for Est. Time ($/hr)", value=st.session_state.get("inv_est_time_job_rate", 50.0), min_value=0.0, key="ig_rate_input_est_time")
            with col_auto_opt2_ig:
                cfg_inc_job_actual_time_total = st.checkbox("Job Total Actual Time (Hours)", key="ig_cfg_job_actual_time")
                if cfg_inc_job_actual_time_total:
                    st.session_state.inv_actual_time_total_job_rate = st.number_input("Rate for Total Actual Time ($/hr)", value=st.session_state.get("inv_actual_time_total_job_rate", 50.0), min_value=0.0, key="ig_rate_input_actual_time")
                cfg_inc_job_actual_mat_cost_total = st.checkbox("Job Total Actual Material Cost", key="ig_cfg_job_actual_mat_total")

            st.markdown("---"); st.markdown("##### Detailed Record Inclusion")
            cfg_inc_records_detailed_time = st.checkbox("Include Detailed Time Entries (per contractor)", key="ig_cfg_records_detailed_time")
            cfg_inc_records_detailed_materials = st.checkbox("Include Detailed Material Usage", key="ig_cfg_records_detailed_mats")
            cfg_inc_records_down_payments = st.checkbox("Include Down Payments for this Job", key="ig_cfg_records_down_payments")

            # --- THIS IS THE LOGIC THAT WAS MISSING ---
            if cfg_inc_job_est_mat_cost:
                val = float(job_details_for_items_ig['Estimated Materials Cost'])
                auto_items_for_current_invoice_ig.append({'description': f"Job Estimated Material Cost: {job_details_for_items_ig['Job Name']}", 'quantity': 1.0, 'unit_price': val, 'total': val, 'source': 'auto'})
            if cfg_inc_job_est_time:
                hrs = float(job_details_for_items_ig['Estimated Hours']); rate = float(st.session_state.inv_est_time_job_rate)
                auto_items_for_current_invoice_ig.append({'description': f"Job Estimated Time: {format_hours(hrs,1)} hrs @ {format_currency(rate)}/hr", 'quantity': hrs, 'unit_price': rate, 'total': hrs * rate, 'source': 'auto'})
            if cfg_inc_job_actual_time_total:
                actual_hrs = float(job_time_df[job_time_df['JobUniqueID'] == job_uid_for_items_ig]['Time Duration (Hours)'].sum()); rate = float(st.session_state.inv_actual_time_total_job_rate)
                auto_items_for_current_invoice_ig.append({'description': f"Job Total Actual Time: {format_hours(actual_hrs,1)} hrs @ {format_currency(rate)}/hr", 'quantity': actual_hrs, 'unit_price': rate, 'total': actual_hrs * rate, 'source': 'auto'})
            if cfg_inc_job_actual_mat_cost_total:
                m_cost = float(materials_df[materials_df['JobUniqueID'] == job_uid_for_items_ig]['Amount'].sum()); r_cost = float(receipts_df[receipts_df['JobUniqueID'] == job_uid_for_items_ig]['Amount'].sum())
                total_m_r_cost = m_cost + r_cost
                auto_items_for_current_invoice_ig.append({'description': "Job Total Actual Material Cost (Usage & Receipts)", 'quantity': 1.0, 'unit_price': total_m_r_cost, 'total': total_m_r_cost, 'source': 'auto'})
            if cfg_inc_records_detailed_time:
                time_summary_ig = job_time_df[job_time_df['JobUniqueID'] == job_uid_for_items_ig].groupby('Contractor')['Time Duration (Hours)'].sum().reset_index()
                for _, row_t_ig in time_summary_ig.iterrows():
                    auto_items_for_current_invoice_ig.append({'description': f"Labor: {row_t_ig['Contractor']}", 'quantity': float(row_t_ig['Time Duration (Hours)']), 'unit_price': 50.0, 'total': float(row_t_ig['Time Duration (Hours)']) * 50.0, 'source': 'auto'})
            if cfg_inc_records_detailed_materials:
                mat_summary_ig = materials_df[materials_df['JobUniqueID'] == job_uid_for_items_ig].groupby('Material')['Amount'].sum().reset_index()
                for _, row_m_ig in mat_summary_ig.iterrows():
                    auto_items_for_current_invoice_ig.append({'description': f"Material: {row_m_ig['Material']}", 'quantity': 1.0, 'unit_price': float(row_m_ig['Amount']), 'total': float(row_m_ig['Amount']), 'source': 'auto'})
            if cfg_inc_records_down_payments:
                for _, row_dp_ig in down_payments_df[down_payments_df['JobUniqueID'] == job_uid_for_items_ig].iterrows():
                    desc = f"Down Payment ({pd.to_datetime(row_dp_ig['DateReceived']).strftime('%Y-%m-%d')}, Ref: {str(row_dp_ig['DownPaymentID'])[:8]})"
                    amt = -float(row_dp_ig['Amount'])
                    auto_items_for_current_invoice_ig.append({'description': desc, 'quantity': 1.0, 'unit_price': amt, 'total': amt, 'source': 'auto'})
        else:
            st.info("Select a job to see automatic line item options.")

        # Combine auto-generated items with existing manual items
        manual_items_from_session_ig = [item for item in st.session_state.invoice_line_items if item.get('source') == 'manual']
        st.session_state.invoice_line_items = auto_items_for_current_invoice_ig + manual_items_from_session_ig
        if not st.session_state.invoice_line_items:
            st.session_state.invoice_line_items = [{'description': '', 'quantity': 1.0, 'unit_price': 0.0, 'total': 0.0, 'source': 'manual'}]

        # --- Manual Line Item Management ---
        # --- Manual Line Item Management ---
        st.markdown("---"); st.subheader("Document Line Items")
        li_h_cols_ig_disp = st.columns([4, 2, 2, 2, 1])
        with li_h_cols_ig_disp[0]: st.markdown("**Description**")
        with li_h_cols_ig_disp[1]: st.markdown("**Quantity**")
        with li_h_cols_ig_disp[2]: st.markdown("**Unit Price ($)**")
        with li_h_cols_ig_disp[3]: st.markdown("**Total ($)**")
        with li_h_cols_ig_disp[4]: st.markdown("**Action**")

        indices_to_delete_from_list_ig = []
        # Loop through and display each line item
        for idx_li, item_li_ig in enumerate(st.session_state.invoice_line_items):
            row_cols_display_ig = st.columns([4, 2, 2, 2, 1])

            # If the item was added manually, make it editable
            if item_li_ig.get('source') == 'manual':
                row_item_key_ig = f"li_manual_{idx_li}"
                desc_input_val = row_cols_display_ig[0].text_input("desc", value=item_li_ig['description'], key=f"desc_{row_item_key_ig}", label_visibility="collapsed")
                qty_input_val = row_cols_display_ig[1].number_input("qty", value=item_li_ig['quantity'], key=f"qty_{row_item_key_ig}", label_visibility="collapsed")
                price_input_val = row_cols_display_ig[2].number_input("price", value=item_li_ig['unit_price'], format="%.2f", key=f"price_{row_item_key_ig}", label_visibility="collapsed")
                current_total_val_ig = qty_input_val * price_input_val
                row_cols_display_ig[3].text_input("total", value=f"{current_total_val_ig:.2f}", disabled=True, key=f"total_{row_item_key_ig}", label_visibility="collapsed")
                if row_cols_display_ig[4].button("🗑️", key=f"del_{row_item_key_ig}"):
                    indices_to_delete_from_list_ig.append(idx_li)
                # Update the item in session state
                st.session_state.invoice_line_items[idx_li] = {'description': desc_input_val, 'quantity': qty_input_val, 'unit_price': price_input_val, 'total': current_total_val_ig, 'source': 'manual'}
            
            # If the item was generated from a checkbox, display it as read-only text
            else:
                with row_cols_display_ig[0]: st.markdown(f"<div style='height: 38px; padding-top: 8px;'>{item_li_ig.get('description', '')}</div>", unsafe_allow_html=True)
                with row_cols_display_ig[1]: st.markdown(f"<div style='text-align: right; height: 38px; padding-top: 8px;'>{format_hours(item_li_ig.get('quantity', 0), 2)}</div>", unsafe_allow_html=True)
                with row_cols_display_ig[2]: st.markdown(f"<div style='text-align: right; height: 38px; padding-top: 8px;'>{format_currency(item_li_ig.get('unit_price', 0))}</div>", unsafe_allow_html=True)
                with row_cols_display_ig[3]: st.markdown(f"<div style='text-align: right; height: 38px; padding-top: 8px;'>{format_currency(item_li_ig.get('total', 0))}</div>", unsafe_allow_html=True)
                with row_cols_display_ig[4]: st.markdown("<div style='text-align: center; height: 38px; padding-top: 8px;'>Auto</div>", unsafe_allow_html=True)

        # Logic to delete manual items
        if indices_to_delete_from_list_ig:
            for i_to_del in sorted(indices_to_delete_from_list_ig, reverse=True):
                st.session_state.invoice_line_items.pop(i_to_del)
            st.rerun()

        # Button to add new manual items
        if st.button("Add New Custom Line Item", key="ig_add_new_custom_item_btn"):
            st.session_state.invoice_line_items.append({'description': '', 'quantity': 1.0, 'unit_price': 0.0, 'total': 0.0, 'source': 'manual'})
            st.rerun()
        # --- Calculate and Display Totals ---
        final_subtotal_ig = sum(float(item.get('total',0.0)) for item in st.session_state.invoice_line_items)
        final_tax_amount_ig = final_subtotal_ig * (tax_rate_input_ig / 100)
        final_grand_total_ig = final_subtotal_ig + final_tax_amount_ig
        st.markdown("---")
        st.markdown(f"#### Subtotal: {format_currency(final_subtotal_ig)}")
        st.markdown(f"#### Tax ({tax_rate_input_ig}%): {format_currency(final_tax_amount_ig)}")
        st.markdown(f"### GRAND TOTAL: {format_currency(final_grand_total_ig)}")

        # --- PDF Generation Button ---
        if st.button(f"Generate {doc_type_selected_ig} PDF", key="ig_final_generate_pdf_btn", type="primary"):
            if selected_job_data_for_doc_ig is None:
                st.error("Please select a valid Client and Job before generating the PDF.")
            else:
                with st.spinner("Generating and uploading PDF..."):
                    pdf_gen_doc = PDF(company_details_for_pdf_doc_ig, logo_path=LOGO_PATH)
                    pdf_gen_doc.add_page()
                    pdf_gen_doc.document_title_section(doc_type_selected_ig, doc_number_input_ig, doc_date_input_ig)
                    pdf_gen_doc.bill_to_job_info(client_data=selected_job_data_for_doc_ig, job_data=selected_job_data_for_doc_ig)
                    pdf_line_headers = ["Description", "Quantity", "Unit Price", "Total"]
                    pdf_line_col_widths = [95, 25, 35, 35]
                    pdf_line_data = [[item['description'], format_hours(item['quantity'], 2), format_currency(item['unit_price']), format_currency(item['total'])] for item in st.session_state.invoice_line_items if item.get('description','').strip()]
                    #pdf_gen_doc.line_items_table(pdf_line_headers, pdf_line_data, pdf_line_col_widths)
                    pdf_gen_doc.totals_section(final_subtotal_ig, f"Tax ({tax_rate_input_ig}%)", final_tax_amount_ig, final_grand_total_ig)
                    pdf_gen_doc.notes_terms_signatures(doc_notes_input_ig, st.session_state.invoice_terms)
                    
                    pdf_output_bytes = pdf_gen_doc.output()

                    if pdf_output_bytes and isinstance(pdf_output_bytes, bytes):
                        pdf_final_filename = f"{doc_number_input_ig}.pdf"
                        class DummyFile:
                            def __init__(self, content, name): self._content = content; self.name = name; self.type = "application/pdf"
                            def getvalue(self): return self._content
                        dummy_pdf_file = DummyFile(pdf_output_bytes, pdf_final_filename)
                        upload_link = upload_file_to_drive(dummy_pdf_file, pdf_final_filename, DRIVE_FOLDER_ID_ESTIMATES_INVOICES)

                        if upload_link:
                            new_doc_record = {'DocNumber': doc_number_input_ig, 'JobUniqueID': selected_job_data_for_doc_ig['UniqueID'], 'DateGenerated': datetime.date.today()}
                            if doc_type_selected_ig == "Estimate":
                                updated_df = pd.concat([estimates_df, pd.DataFrame([new_doc_record])], ignore_index=True)
                                save_data(updated_df, 'estimates')
                            else:
                                updated_df = pd.concat([invoices_df, pd.DataFrame([new_doc_record])], ignore_index=True)
                                save_data(updated_df, 'invoices')
                            st.success("Generated PDF saved to Google Drive.")
                            st.markdown(f"**[View Document in Drive]({upload_link})**")
                            st.cache_data.clear()
                        else:
                            st.error("Failed to save PDF to Google Drive.")
                        st.download_button("Download PDF", pdf_output_bytes, pdf_final_filename, "application/pdf")
                    else:
                        st.error("Failed to generate valid PDF content. The resulting file is empty.")
    else:
        st.error("Access restricted to Admin or Manager for Invoice Generation.")

        
elif section == 'Reports & Analytics':
    st.header("Reports & Analytics")

    if current_user_role_val in ['Admin', 'Manager', 'Client Viewer']:
        # --- Filters for Reports ---
        report_jobs_df_ra_filter = jobs_df.copy()
        if current_user_role_val == 'Client Viewer' and associated_client_name_val:
            report_jobs_df_ra_filter = report_jobs_df_ra_filter[report_jobs_df_ra_filter['Client'].astype(str).strip() == associated_client_name_val.strip()]

        # Client Filter
        report_client_options_ra = ["All Clients"]
        if current_user_role_val != 'Client Viewer':
            if not jobs_df.empty: report_client_options_ra.extend(sorted(list(jobs_df['Client'].astype(str).str.strip().replace('',np.nan).dropna().unique())))
        else:
            report_client_options_ra = [associated_client_name_val] if associated_client_name_val else ["No Associated Client"]

        selected_report_client_ra = st.selectbox("Filter Reports by Client:", report_client_options_ra,
                                                 key="ra_client_filter",
                                                 index=0 if current_user_role_val == 'Client Viewer' else (report_client_options_ra.index(st.session_state.get("ra_client_filter_val", report_client_options_ra[0])) if st.session_state.get("ra_client_filter_val") in report_client_options_ra else 0) )
        if current_user_role_val != 'Client Viewer': st.session_state.ra_client_filter_val = selected_report_client_ra

        # Job Filter
        jobs_for_report_job_filter_ra = report_jobs_df_ra_filter.copy()
        if selected_report_client_ra != "All Clients":
            jobs_for_report_job_filter_ra = jobs_for_report_job_filter_ra[jobs_for_report_job_filter_ra['Client'].astype(str).strip() == selected_report_client_ra.strip()]

        report_job_options_ra = ["All Jobs"] + sorted(list(jobs_for_report_job_filter_ra['Job Name'].astype(str).str.strip().replace('',np.nan).dropna().unique()))
        selected_report_job_ra = st.selectbox("Filter Reports by Job:", report_job_options_ra,
                                              key="ra_job_filter",
                                              index=report_job_options_ra.index(st.session_state.get("ra_job_filter_val", report_job_options_ra[0])) if st.session_state.get("ra_job_filter_val") in report_job_options_ra else 0)
        st.session_state.ra_job_filter_val = selected_report_job_ra

        # --- Apply filters to data for reports ---
        filtered_jobs_ra = jobs_df.copy()
        filtered_time_ra = job_time_df.copy()
        filtered_materials_ra = materials_df.copy()
        filtered_receipts_ra = receipts_df.copy()

        if selected_report_client_ra != "All Clients":
            filtered_jobs_ra = filtered_jobs_ra[filtered_jobs_ra['Client'].astype(str).strip() == selected_report_client_ra.strip()]
            filtered_time_ra = filtered_time_ra[filtered_time_ra['Client'].astype(str).strip() == selected_report_client_ra.strip()]
            filtered_materials_ra = filtered_materials_ra[filtered_materials_ra['Client'].astype(str).strip() == selected_report_client_ra.strip()]
            filtered_receipts_ra = filtered_receipts_ra[filtered_receipts_ra['Client Name'].astype(str).strip() == selected_report_client_ra.strip()]

        if selected_report_job_ra != "All Jobs":
            job_uid_for_report_filter = None
            if not filtered_jobs_ra[filtered_jobs_ra['Job Name'] == selected_report_job_ra].empty:
                job_uid_for_report_filter = filtered_jobs_ra[filtered_jobs_ra['Job Name'] == selected_report_job_ra]['UniqueID'].iloc[0]

            filtered_jobs_ra = filtered_jobs_ra[filtered_jobs_ra['Job Name'].astype(str).strip() == selected_report_job_ra.strip()]
            if job_uid_for_report_filter:
                filtered_time_ra = filtered_time_ra[filtered_time_ra['JobUniqueID'] == job_uid_for_report_filter]
                filtered_materials_ra = filtered_materials_ra[filtered_materials_ra['JobUniqueID'] == job_uid_for_report_filter]
                filtered_receipts_ra = filtered_receipts_ra[filtered_receipts_ra['JobUniqueID'] == job_uid_for_report_filter]
            else:
                if selected_report_job_ra != "All Jobs":
                    filtered_time_ra = pd.DataFrame(columns=job_time_df.columns)
                    filtered_materials_ra = pd.DataFrame(columns=materials_df.columns)
                    filtered_receipts_ra = pd.DataFrame(columns=receipts_df.columns)

        # --- Tabs for Different Reports ---
        report_tab1, report_tab2 = st.tabs(["Contractor & Material Analytics", "Job Performance Analysis"])

        with report_tab1:
            st.markdown("#### Average Daily Duration per Contractor")
            if not filtered_time_ra.empty:
                daily_hours_ra = filtered_time_ra.copy()
                daily_hours_ra['Date'] = pd.to_datetime(daily_hours_ra['Date'], errors='coerce').dt.date
                daily_hours_ra.dropna(subset=['Date', 'Contractor', 'Time Duration (Hours)'], inplace=True)

                if not daily_hours_ra.empty:
                    daily_sum_ra = daily_hours_ra.groupby(['Contractor', 'Date'])['Time Duration (Hours)'].sum().reset_index()
                    if not daily_sum_ra.empty:
                        avg_daily_duration_ra = daily_sum_ra.groupby('Contractor')['Time Duration (Hours)'].mean().reset_index()
                        avg_daily_duration_ra = avg_daily_duration_ra.sort_values(by='Time Duration (Hours)', ascending=False)

                        fig_avg_duration_ra = px.bar(avg_daily_duration_ra, x='Contractor', y='Time Duration (Hours)',
                                                     text='Time Duration (Hours)', template="plotly_dark", color='Contractor',
                                                     labels={'Time Duration (Hours)': 'Avg. Daily Duration (Hours)'})
                        fig_avg_duration_ra.update_traces(texttemplate='%{text:.1f} hrs', textposition='outside')
                        fig_avg_duration_ra.update_layout(showlegend=False)
                        st.plotly_chart(fig_avg_duration_ra, use_container_width=True)
                    else: st.info("No aggregated daily work hour data for contractors based on filters.")
                else: st.info("No valid time entries with dates to calculate average daily duration.")
            else: st.info("Time tracking data insufficient for 'Average Daily Duration' chart.")

            st.markdown("#### Top Materials by Cost (from Material Usage Logs)")
            if not filtered_materials_ra.empty:
                material_costs_ra = filtered_materials_ra.groupby('Material')['Amount'].sum().reset_index()
                material_costs_ra = material_costs_ra[material_costs_ra['Amount'] > 0]
                top_materials_ra = material_costs_ra.sort_values(by='Amount', ascending=False).head(10)

                if not top_materials_ra.empty:
                    fig_top_materials_ra = px.bar(top_materials_ra, x='Material', y='Amount', text='Amount',
                                                  template="plotly_dark", color='Material', labels={'Amount': 'Total Cost ($)'})
                    fig_top_materials_ra.update_traces(texttemplate='$%{text:,.2f}', textposition='outside')
                    fig_top_materials_ra.update_layout(showlegend=False)
                    st.plotly_chart(fig_top_materials_ra, use_container_width=True)
                else: st.info("No material cost data available based on selected filters.")
            else: st.info("Material usage data insufficient for 'Top Materials' chart.")

        with report_tab2:
            st.markdown("#### Job Performance: Estimated vs. Actuals")
            if not filtered_jobs_ra.empty:
                job_perf_df_ra = filtered_jobs_ra[['Job Name', 'Estimated Hours', 'Estimated Materials Cost', 'UniqueID']].copy()

                if not filtered_time_ra.empty:
                    actual_hours_agg_ra = filtered_time_ra.groupby('JobUniqueID')['Time Duration (Hours)'].sum().reset_index()
                    actual_hours_agg_ra.rename(columns={'JobUniqueID': 'UniqueID', 'Time Duration (Hours)': 'Actual Hours'}, inplace=True)
                    job_perf_df_ra = job_perf_df_ra.merge(actual_hours_agg_ra, on='UniqueID', how='left')
                job_perf_df_ra['Actual Hours'] = job_perf_df_ra.get('Actual Hours', pd.Series(dtype='float')).fillna(0.0)

                actual_mat_usage_cost_ra = pd.Series(dtype='float')
                if not filtered_materials_ra.empty:
                    actual_mat_usage_cost_ra = filtered_materials_ra.groupby('JobUniqueID')['Amount'].sum()
                actual_receipts_cost_ra = pd.Series(dtype='float')
                if not filtered_receipts_ra.empty:
                    actual_receipts_cost_ra = filtered_receipts_ra.groupby('JobUniqueID')['Amount'].sum()

                job_perf_df_ra['ActualMatUsageCost'] = job_perf_df_ra['UniqueID'].map(actual_mat_usage_cost_ra).fillna(0.0)
                job_perf_df_ra['ActualReceiptsCost'] = job_perf_df_ra['UniqueID'].map(actual_receipts_cost_ra).fillna(0.0)
                job_perf_df_ra['Total Actual Material Cost'] = job_perf_df_ra['ActualMatUsageCost'] + job_perf_df_ra['ActualReceiptsCost']

                display_cols_perf_ra = ['Job Name', 'Estimated Hours', 'Actual Hours', 'Estimated Materials Cost', 'Total Actual Material Cost']
                st.dataframe(job_perf_df_ra[display_cols_perf_ra].style.format({
                    'Estimated Hours': "{:.1f}", 'Actual Hours': "{:.1f}",
                    'Estimated Materials Cost': format_currency, 'Total Actual Material Cost': format_currency
                }), use_container_width=True, hide_index=True)

                if not job_perf_df_ra.empty:
                    melted_hours_perf_ra = job_perf_df_ra.melt(id_vars='Job Name', value_vars=['Estimated Hours', 'Actual Hours'], var_name='Metric', value_name='Hours')
                    fig_hours_perf_ra = px.bar(melted_hours_perf_ra, x='Job Name', y='Hours', color='Metric', barmode='group',
                                               title='Estimated vs. Actual Hours per Job', template="plotly_dark", text='Hours',
                                               color_discrete_map={'Estimated Hours': '#FF6347', 'Actual Hours': '#1E90FF'})
                    fig_hours_perf_ra.update_traces(texttemplate='%{text:.1f} hrs', textposition='outside')
                    st.plotly_chart(fig_hours_perf_ra, use_container_width=True)

                    melted_mats_perf_ra = job_perf_df_ra.melt(id_vars='Job Name', value_vars=['Estimated Materials Cost', 'Total Actual Material Cost'], var_name='Metric', value_name='Cost')
                    fig_mats_perf_ra = px.bar(melted_mats_perf_ra, x='Job Name', y='Cost', color='Metric', barmode='group',
                                              title='Estimated vs. Actual Material Costs per Job', template="plotly_dark", text='Cost',
                                              color_discrete_map={'Estimated Materials Cost': '#FF6347', 'Total Actual Material Cost': '#1E90FF'})
                    fig_mats_perf_ra.update_traces(texttemplate='$%{text:,.2f}', textposition='outside')
                    st.plotly_chart(fig_mats_perf_ra, use_container_width=True)
                else: st.info("No job performance data to display charts based on current filters.")
            else: st.info("No job data available for performance analysis based on selected filters.")
    else:
        st.error("Access restricted for Reports & Analytics.")


# --- Footer ---
st.sidebar.markdown("---")
st.sidebar.write("Powered by JC")
