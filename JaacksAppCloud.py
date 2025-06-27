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

# --- GOOGLE API & DATA HANDLING ---

# These will come from your Streamlit Secrets file (`.streamlit/secrets.toml`)
# IMPORTANT: You MUST create these folders in your Google Drive and get their IDs.
DRIVE_FOLDER_ID_RECEIPTS = "15Z7OLMrZLa6fdu8Pue52FIoRW6CsZc5x"
DRIVE_FOLDER_ID_JOB_FILES = "1L7Q1PpDQeg1rz6VEvO7krh9slMJwxL7T"
DRIVE_FOLDER_ID_ESTIMATES_INVOICES = "1LCEuA0WOgJH0MYNSq13FeGZUesOjVa4K"
SPREADSHEET_KEY = "1Ik_6-5NKLiJLeT_ZkT4nE1l-EjlqqX4Kdsxhd6fR5A8"
LOGO_PATH = "C:\\Users\\RJ\\Desktop\\JC APP\\logo.jpg" # This remains a local path for PDF generation

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
    """Loads a worksheet from Google Sheets into a DataFrame."""
    try:
        sheet = sheets_service.open_by_key(SPREADSHEET_KEY).worksheet(worksheet_name)
        data = sheet.get_all_records()
        if not data: # Handle empty sheet
             return pd.DataFrame()
        df = pd.DataFrame(data)

        # Basic type conversions
        for col in df.columns:
            if "date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            elif "cost" in col.lower() or "amount" in col.lower() or "hours" in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else: # Convert other columns to string to avoid mixed type issues
                df[col] = df[col].astype(str).replace(['nan', 'None', 'NONE', '<NA>'], '')

        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Worksheet '{worksheet_name}' not found in your Google Sheet. Please create it.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to load data from worksheet '{worksheet_name}': {e}")
        return pd.DataFrame()

def save_data(df_to_save, worksheet_name):
    """Saves a DataFrame to a specified worksheet in Google Sheets."""
    try:
        sheet = sheets_service.open_by_key(SPREADSHEET_KEY).worksheet(worksheet_name)
        df_to_save_str = df_to_save.astype(str).replace(['nan', 'None', 'NONE', '<NA>', 'NaT'], '')
        sheet.clear() # Clear the sheet before writing
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
    if pd.notna(end_date_val) and not isinstance(end_date_val, datetime.date):
        try: end_date_val = pd.to_datetime(end_date_val).date()
        except: end_date_val = None

    if end_date_val and isinstance(end_date_val, datetime.date) and row.get('Status') == 'In Progress':
        delta = (end_date_val - today).days
        if delta <= 3: style = ['background-color: #FF0000; color: black;'] * len(row)
        elif delta <= 7: style = ['background-color: #A2FF8A; color: black;'] * len(row)
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

    if styler_fn:
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
    # ... (Your entire PDF class code remains unchanged here) ...
    def __init__(self, company_details, logo_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_details = company_details
        self.logo_path = logo_path
        self.set_auto_page_break(auto=True, margin=15)
        self.set_draw_color(200, 200, 200) # Light grey for table borders
        # The rest of your PDF class... (it's very long, so omitting for brevity, but it goes here)

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
    "company_address_pdf": "123 Building Integrity Way, Brookings, SD 57006",
    "company_phone_pdf": "(555) 123-4567",
    "company_email_pdf": "contact@jcconstruction.example.com",
    "admin_edit_job_client_filter": "All Clients", "admin_edit_job_name_filter": "All Jobs",
    "edit_time_entry_contractor_filter": "", "edit_time_entry_date_filter": None,
    "selected_client_material_view": "All Clients", "selected_client_receipt_view": "All Clients",
    "inv_est_time_job_rate": 50.0,
    "inv_actual_time_total_job_rate": 50.0,
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
    receipts_df = load_data('receipts')
    users_df = load_data('users')
    down_payments_df = load_data('down_payments')
    job_files_df = load_data('job_files')
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
# ... (all your navigation logic based on user role remains the same) ...
# ...
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

                    assoc_client_current_um = user_data_for_edit_um.get('AssociatedClientName', '')
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

elif section == 'Job Details':
    # ... (Your entire Job Details section logic) ...
    # CHANGE: All save_data(..., 'jobs.csv') calls become save_data(..., 'jobs')
    pass # Placeholder for your code

elif section == 'Job Time Tracking':
    # ... (Your entire Job Time Tracking section logic) ...
    # CHANGE: All save_data(..., 'job_time.csv') calls become save_data(..., 'job_time')
    pass # Placeholder for your code

elif section == 'Material Usage':
    # ... (Your entire Material Usage section logic) ...
    # CHANGE: All save_data(..., 'materials.csv') calls become save_data(..., 'materials')
    pass # Placeholder for your code

elif section == 'Upload Receipt':
    st.header("Upload Receipt")
    if current_user_role_val in ['Contractor', 'Manager', 'Admin']:
        # ... (your existing form for selecting job, contractor, amount, etc.) ...
        with st.form("new_receipt_form_ur_section", clear_on_submit=True):
            # ... (your input fields for contractor, amount, payor) ...
            uploaded_file_data_ur = st.file_uploader("Upload Receipt File (PDF, PNG, JPG)*", type=['pdf', 'png', 'jpg', 'jpeg'], key="ur_form_file_uploader")

            if st.form_submit_button("Save Receipt Information"):
                if not (contractor_name_ur_input and selected_job_ur_form != "Select Job" and final_payor_ur_form.strip() and uploaded_file_data_ur):
                    st.error("Please fill all required fields and upload a file.")
                else:
                    with st.spinner("Uploading file and saving info..."):
                        unique_filename = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{uploaded_file_data_ur.name}"
                        receipt_link = upload_file_to_drive(uploaded_file_data_ur, unique_filename, DRIVE_FOLDER_ID_RECEIPTS)

                        if receipt_link:
                            new_receipt_record = {
                                'Contractor Name': contractor_name_ur_input.strip().title(),
                                'Client Name': client_name_ur_form.strip(),
                                'Job Name': selected_job_ur_form.strip(),
                                'Payor': final_payor_ur_form.strip(),
                                'Amount': amount_ur_input_val,
                                'File Name': uploaded_file_data_ur.name,
                                'File Path': receipt_link, # <-- Storing the Google Drive link
                                'Upload Date': datetime.datetime.now().isoformat(),
                                'UniqueID': uuid.uuid4().hex,
                                'JobUniqueID': job_uid_ur_form
                            }
                            updated_receipts_df_ur = pd.concat([receipts_df, pd.DataFrame([new_receipt_record])], ignore_index=True)
                            save_data(updated_receipts_df_ur, 'receipts')
                            receipts_df = load_data('receipts')
                            st.success("Receipt uploaded and info saved!")
                            st.rerun()
                        else:
                            st.error("File upload failed. Receipt info not saved.")

    st.subheader("Uploaded Receipts Log")
    # ... (your existing logic to filter receipts_df_display_main_ur) ...
    display_paginated_dataframe(
        receipts_df_display_main_ur.sort_values(by="Upload Date", ascending=False),
        "ur_receipts_log_paginated", page_size=5,
        col_config={
            "File Path": st.column_config.LinkColumn("View Receipt", display_text="Open File ↗️"),
            # Hide other columns as you did before
            "UniqueID": None, "JobUniqueID": None
        }
    )
    # The st.download_button logic can now be removed as links are in the table.

elif section == 'Job File Uploads':
    # Apply the same pattern as 'Upload Receipt'
    # 1. Use the upload_file_to_drive function in the form submission.
    # 2. Use DRIVE_FOLDER_ID_JOB_FILES as the parent_folder_id.
    # 3. Save the returned link to the 'File Path' column in the 'job_files' worksheet.
    # 4. Use st.column_config.LinkColumn to display the clickable link in the dataframe.
    pass # Placeholder for your refactored code

# --- Other sections (Down Payments, Invoice, Reports) ---
# No major changes are needed in these sections unless they involve file I/O.
# Your save_data calls for down_payments.csv will just become save_data(..., 'down_payments')
# The PDF generation for invoices will try to save to a Google Drive folder.

elif section == 'Invoice Generation':
    # ... (your invoice generation logic) ...
    if st.button(f"Generate {doc_type_selected_ig} PDF"):
        # ... (after the pdf_output_bytes_final_ig is created) ...
        pdf_final_filename = f"{doc_type_selected_ig}_{s_job_fn_pdf_save}_{pdf_filename_timestamp}.pdf"

        # Create a file-like object in memory
        pdf_io = io.BytesIO(pdf_output_bytes_final_ig)
        # Create a dummy file object that st.file_uploader would create
        class DummyFile:
            def __init__(self, content, name, type):
                self.content = content
                self.name = name
                self.type = type
            def getvalue(self):
                return self.content
        
        dummy_pdf_file = DummyFile(pdf_io.getvalue(), pdf_final_filename, "application/pdf")
        
        # Upload the generated PDF to Google Drive
        upload_link = upload_file_to_drive(dummy_pdf_file, pdf_final_filename, DRIVE_FOLDER_ID_ESTIMATES_INVOICES)
        
        if upload_link:
            st.success(f"Generated {doc_type_selected_ig} saved to Google Drive.")
            st.markdown(f"**[View Document in Drive]({upload_link})**")
        else:
            st.error("Failed to save generated PDF to Google Drive.")

        # Still provide the direct download button
        st.download_button(label=f"Download {doc_type_selected_ig} PDF", data=pdf_output_bytes_final_ig, file_name=pdf_final_filename, mime="application/pdf")

# ... (rest of your code, e.g., Reports & Analytics) ...
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
