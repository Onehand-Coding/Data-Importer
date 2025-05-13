# Content for: web/app.py
import sys
from pathlib import Path

# Add project root to Python path to allow for absolute imports
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import io
import re
import csv
import time
import shutil
import logging
import datetime
from typing import List, Dict, Type, Optional, Mapping, Any

import pandas as pd
import streamlit as st

from core.database import DatabaseManager
from core.importers.base_importer import BaseImporter, ImportResult
from core.importers import AVAILABLE_IMPORTERS
from core.importers.database_source_importer import DatabaseSourceImporter


# --- Configure logging---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TEMP_DIR = ROOT_DIR / "data" / "temp_uploads"
try:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    now = time.time()
    for f in TEMP_DIR.glob("*"):
        if now - f.stat().st_mtime > 3600:
            try:
                if f.is_file():
                    f.unlink()
                elif f.is_dir():
                    shutil.rmtree(f)
            except Exception as e:
                logger.warning(f"Could not delete {f}: {e}")
except Exception as e:
    logger.error(f"Temp dir setup failed: {e}")

# --- Page Configuration ---
st.set_page_config(
    page_title="Data Importer Pro",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="auto"
)

# --- Initialize Session State ---
# For target DB (path is now defined in main UI if SQLite output is chosen)
if 'target_db_path_for_import' not in st.session_state:
    st.session_state.target_db_path_for_import = "data/db/importer_pro.db"
if 'target_db_manager_instance' not in st.session_state:
    st.session_state.target_db_manager_instance = None
if 'db_manager_current_path' not in st.session_state:
    st.session_state.db_manager_current_path = None


# For file uploads
if 'uploaded_file_info' not in st.session_state: st.session_state.uploaded_file_info = None
if 'temp_file_path' not in st.session_state: st.session_state.temp_file_path = None
if 'current_processed_filename' not in st.session_state: st.session_state.current_processed_filename = None

# For general import/export flow
if 'active_source_headers' not in st.session_state: st.session_state.active_source_headers = []
if 'last_headers' not in st.session_state: st.session_state.last_headers = []
if 'column_mapping_state' not in st.session_state: st.session_state.column_mapping_state = {}
if 'target_table_name_input' not in st.session_state: st.session_state.target_table_name_input = ""
if 'last_operation_results' not in st.session_state: st.session_state.last_operation_results = None

# For DB source import
if 'current_source_type' not in st.session_state: st.session_state.current_source_type = "File Upload"
if 'source_db_type' not in st.session_state: st.session_state.source_db_type = "SQLite"
if 'source_db_conn_string' not in st.session_state: st.session_state.source_db_conn_string = ""
if 'source_db_connected' not in st.session_state: st.session_state.source_db_connected = False
if 'source_db_importer_instance' not in st.session_state: st.session_state.source_db_importer_instance = None
if 'source_db_tables' not in st.session_state: st.session_state.source_db_tables = []
if 'source_db_selected_table' not in st.session_state: st.session_state.source_db_selected_table = None
if 'source_db_custom_query' not in st.session_state: st.session_state.source_db_custom_query = "SELECT * FROM your_table_name LIMIT 100;"
if 'source_db_specify_method' not in st.session_state: st.session_state.source_db_specify_method = "Select Table"
if 'source_db_preview_data' not in st.session_state: st.session_state.source_db_preview_data = None
if 'downloadable_file_prepared' not in st.session_state: st.session_state.downloadable_file_prepared = None


# --- Importer Factory ---
def get_importer_for_file(file_path: Path, db_manager_for_target_if_needed: Optional[DatabaseManager]) -> Optional[BaseImporter]:
    extension = file_path.suffix.lower()
    importer_class = AVAILABLE_IMPORTERS.get(extension)
    if importer_class:
        logger.info(f"Found file importer {importer_class.__name__} for extension '{extension}'")
        try:
            return importer_class(db_manager_for_target_if_needed)
        except Exception as e:
            logger.exception(f"Failed to instantiate file importer {importer_class.__name__}")
            st.error(f"Error initializing file importer for {extension} files: {e}")
            return None
    else:
        st.error(f"Unsupported file type: '{extension}'. No file importer found.")
        logger.warning(f"No file importer registered for file extension: {extension}")
        return None

# --- Helper Functions ---
def sanitize_name(name):
    if not isinstance(name, str): name = str(name)
    name = re.sub(r'[^\w_]', '_', name)
    if name and name[0].isdigit(): name = "_" + name
    if not name: return None
    return name.lower()

@st.cache_resource
def get_target_db_manager_cached(db_path_input_str) -> Optional[DatabaseManager]:
    logger.info(f"Requesting TARGET DB Manager for path: {db_path_input_str}")
    if not db_path_input_str or not isinstance(db_path_input_str, str):
         logger.error(f"Invalid target db_path_input: {db_path_input_str}")
         st.error("Invalid target SQLite database path provided."); return None

    db_manager = DatabaseManager(db_path_input_str)
    if not db_manager.connect():
        # Error logged by db_manager.connect()
        # st.error(f"Failed to connect to target SQLite database: {Path(db_path_input_str).name}")
        return None
    logger.info(f"TARGET DB Manager connection successful for: {db_manager.db_path_for_connection}")
    return db_manager

def generate_timestamped_filename(original_name):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    base, ext = Path(original_name).stem, Path(original_name).suffix
    safe_base = re.sub(r'[^\w\.-]', '_', base)
    return f"{safe_base}_{now}{ext}"

# --- UI Components ---
# This function is now primarily for the path input in the sidebar
def show_target_db_path_input_ui():
    """UI for Target SQLite Database Path Input in Sidebar. Returns the current path string."""
    with st.sidebar.expander("🎯 Target SQLite Database (Optional)", expanded=False):
        st.caption("Configure the target SQLite database if you plan to use the 'Import to SQLite Database' output destination. This path will be used when that option is selected below.")
        if "target_db_path_widget" not in st.session_state:
             st.session_state.target_db_path_widget = "data/db/importer_pro.db"

        db_path_val = st.text_input(
            "Target SQLite DB File Path:",
            key="target_db_path_widget",
            help="Path for the target SQLite database file (e.g., data/target.db)."
        )
        # Logic to update main target path state and clear cache if user changes this sidebar input
        if db_path_val != st.session_state.get('target_db_path_for_import'):
            st.session_state.target_db_path_for_import = db_path_val
            if db_path_val != st.session_state.get('db_manager_current_path'): # If it truly changed from what manager uses
                get_target_db_manager_cached.clear()
                st.session_state.target_db_manager_instance = None # Force re-fetch if used
                logger.info(f"Sidebar target DB path updated to: {db_path_val}. Cache cleared.")
                # No rerun here, main flow will pick it up
    return st.session_state.target_db_path_widget


def show_upload_section():
    with st.container(border=True):
        st.subheader("1. Select Data Source: File Upload", divider="rainbow")
        supported_types_str = ", ".join(f"`{ext.lstrip('.')}`" for ext in AVAILABLE_IMPORTERS.keys())
        st.markdown(f"Supported file types: {supported_types_str}")
        uploaded_file = st.file_uploader("Choose a file", type=[ext.lstrip('.') for ext in AVAILABLE_IMPORTERS.keys()], label_visibility="collapsed", key="file_uploader_widget")
        if uploaded_file is not None:
            is_new_or_different_file = False
            current_file_info = st.session_state.get('uploaded_file_info')
            if current_file_info is None or not isinstance(current_file_info, dict): is_new_or_different_file = True
            elif current_file_info.get('name') != uploaded_file.name or current_file_info.get('size') != uploaded_file.size: is_new_or_different_file = True
            if is_new_or_different_file:
                st.session_state.uploaded_file_info = {'name': uploaded_file.name, 'size': uploaded_file.size, 'type': uploaded_file.type, 'content': uploaded_file.getvalue()}
                st.session_state.pop('temp_file_path', None); st.session_state.pop('current_processed_filename', None)
                st.session_state.active_source_headers = []; st.session_state.last_headers = []
                st.session_state.column_mapping_state = {}; st.session_state.target_table_name_input = ""
                st.session_state.pop('last_operation_results', None)
                logger.info(f"New file: {uploaded_file.name}. Reset relevant states."); st.success(f"`{uploaded_file.name}` selected."); st.rerun()
        elif uploaded_file is None and st.session_state.get('uploaded_file_info') is not None:
             logger.info("File uploader cleared. Resetting file-specific states.")
             old_info = st.session_state.pop('uploaded_file_info', None)
             if old_info and isinstance(old_info, dict): st.session_state.pop(f'headers_{old_info.get("name", "old_default_key")}', None)
             st.session_state.pop('temp_file_path', None); st.session_state.pop('current_processed_filename', None)
             st.session_state.active_source_headers = []; st.session_state.last_headers = []
             st.session_state.column_mapping_state = {}; st.rerun()
    return st.session_state.get('uploaded_file_info')

def show_database_source_connector_ui():
    with st.container(border=True):
        st.subheader("1. Select Data Source: Database Connection", divider="rainbow")
        st.session_state.source_db_type = st.selectbox("Select Source Database Type:", ["SQLite", "PostgreSQL", "MySQL"], index=["SQLite", "PostgreSQL", "MySQL"].index(st.session_state.get('source_db_type', "SQLite")), key="source_db_type_widget")
        default_conn_str = {"SQLite": "sqlite:///./your_source_database.db", "PostgreSQL": "postgresql://user:password@host:port/dbname", "MySQL": "mysql+mysqlconnector://user:password@host:port/dbname"}.get(st.session_state.source_db_type, "")
        st.session_state.source_db_conn_string = st.text_input("Source Database Connection String:", value=st.session_state.get('source_db_conn_string', default_conn_str), help=f"Example: {default_conn_str}", key="source_db_conn_str_widget", type="password")
        active_source_db_importer = st.session_state.get('source_db_importer_instance')
        col_connect, col_disconnect = st.columns(2)
        with col_connect:
            if st.button("🔗 Connect to Source DB", key="connect_source_db_btn", use_container_width=True):
                if not st.session_state.source_db_conn_string: st.error("Connection string required."); st.stop()
                if active_source_db_importer and active_source_db_importer.source_engine: active_source_db_importer.close_source_connection()
                new_importer_instance = DatabaseSourceImporter()
                if new_importer_instance.connect_to_source(st.session_state.source_db_conn_string):
                    st.session_state.update({'source_db_connected': True, 'source_db_importer_instance': new_importer_instance, 'source_db_tables': new_importer_instance.get_table_names_from_source(), 'active_source_headers': [], 'source_db_preview_data': None, 'column_mapping_state': {}, 'target_table_name_input': "", 'last_operation_results': None, 'last_headers': []})
                    st.success(f"Connected to {st.session_state.source_db_type} source!")
                else:
                    st.error(f"Failed to connect to {st.session_state.source_db_type}. Check string/logs."); st.session_state.update({'source_db_connected': False, 'source_db_importer_instance': None, 'source_db_tables': [], 'active_source_headers': [], 'source_db_preview_data': None})
                st.rerun()
        with col_disconnect:
            current_source_db_importer = st.session_state.get('source_db_importer_instance')
            if st.session_state.get('source_db_connected') and current_source_db_importer:
                if st.button("❌ Disconnect Source DB", key="disconnect_source_db_btn", use_container_width=True):
                    current_source_db_importer.close_source_connection()
                    st.session_state.update({'source_db_connected': False, 'source_db_importer_instance': None, 'source_db_tables': [], 'active_source_headers': [], 'source_db_preview_data': None, 'column_mapping_state': {}, 'target_table_name_input': ""}); logger.info("Disconnected from source DB."); st.rerun()
        current_importer_for_display = st.session_state.get('source_db_importer_instance')
        if st.session_state.get('source_db_connected') and current_importer_for_display:
            engine_name = current_importer_for_display.source_engine.name if current_importer_for_display.source_engine else 'N/A'
            st.info(f"Connected to source: {st.session_state.source_db_type}. Engine: {engine_name}")
            st.session_state.source_db_specify_method = st.radio("Specify data source:", ["Select Table", "Custom SQL Query"], index=["Select Table", "Custom SQL Query"].index(st.session_state.get('source_db_specify_method', "Select Table")), key="source_db_specify_method_widget", horizontal=True)
            source_identifier_val, is_query_val = (st.session_state.source_db_custom_query, True) if st.session_state.source_db_specify_method == "Custom SQL Query" else (st.session_state.get('source_db_selected_table'), False)
            if st.session_state.source_db_specify_method == "Select Table":
                if st.session_state.source_db_tables:
                    sel_table = st.session_state.get('source_db_selected_table'); opts = st.session_state.source_db_tables; idx = opts.index(sel_table) if sel_table in opts and sel_table is not None else 0
                    source_identifier_val = st.selectbox("Select Source Table:", options=opts, index=idx, key="source_db_table_select_widget")
                    st.session_state.source_db_selected_table = source_identifier_val
                else: st.warning("No tables found or could not fetch.")
            else:
                source_identifier_val = st.text_area("Enter SQL SELECT Query:", value=st.session_state.source_db_custom_query, height=150, key="source_db_query_widget")
                st.session_state.source_db_custom_query = source_identifier_val
            if source_identifier_val:
                if st.button("Load Headers & Preview from DB Source", key="load_db_source_data_btn"):
                    with st.spinner("Loading from source database..."):
                        try:
                            headers = current_importer_for_display.get_headers_from_source(source_identifier_val, is_query_val)
                            st.session_state.active_source_headers = headers
                            st.session_state.source_db_preview_data = current_importer_for_display.get_preview_from_source(source_identifier_val, is_query_val, num_rows=5)
                            if not headers: st.warning("Could not extract headers.")
                            else:
                                st.success(f"Schema and preview loaded. Headers: {', '.join(headers)}")
                                st.session_state.column_mapping_state = { (sanitize_name(h) or f"col_{idx}"): h for idx, h in enumerate(headers)}
                                st.session_state.last_headers = headers
                                default_target_tbl = sanitize_name(source_identifier_val if not is_query_val else "db_query_import") or "imported_data"
                                st.session_state.target_table_name_input = default_target_tbl
                        except Exception as e: st.error(f"Error loading data from source DB: {e}"); logger.error(f"Error loading from DB source '{source_identifier_val}': {e}", exc_info=True); st.session_state.active_source_headers = []; st.session_state.source_db_preview_data = None
                    st.rerun()

@st.cache_data(show_spinner=False)
def get_temporary_filepath(_file_content_bytes: bytes, original_filename: str) -> Optional[Path]:
    try:
        temp_file_name = generate_timestamped_filename(original_filename); temp_path = (TEMP_DIR / temp_file_name).resolve()
        with open(temp_path, "wb") as f: f.write(_file_content_bytes)
        logger.info(f"File content cached to temporary path: {temp_path}"); return temp_path
    except Exception as e: st.error(f"Failed to create temporary file: {e}"); logger.exception("Error creating temporary file:"); return None

@st.cache_data(show_spinner=False)
def get_cached_headers(_importer_instance_generic: Any, source_path_or_identifier: Any, is_query_for_db: bool = False) -> List[str]:
    if isinstance(_importer_instance_generic, BaseImporter) and isinstance(source_path_or_identifier, Path):
        logger.info(f"Cache miss/call: Reading file headers for {source_path_or_identifier.name} using {_importer_instance_generic.__class__.__name__}")
        try: headers = _importer_instance_generic.get_headers(source_path_or_identifier); logger.info(f"Successfully read file headers for {source_path_or_identifier.name}: {headers}"); return headers
        except Exception as e: st.error(f"Could not read headers from file: {e}"); logger.exception(f"Error reading file headers for {source_path_or_identifier}:"); return []
    elif isinstance(_importer_instance_generic, DatabaseSourceImporter) and isinstance(source_path_or_identifier, str):
        logger.info(f"Cache miss/call: Reading DB headers for {source_path_or_identifier} using {_importer_instance_generic.__class__.__name__}")
        try: headers = _importer_instance_generic.get_headers_from_source(source_path_or_identifier, is_query_for_db); logger.info(f"Successfully read DB headers for {source_path_or_identifier}: {headers}"); return headers
        except Exception as e: st.error(f"Could not read headers from DB source: {e}"); logger.exception(f"Error reading DB headers for {source_path_or_identifier}:"); return []
    logger.warning(f"get_cached_headers called with unhandled type: Importer={type(_importer_instance_generic)}, Source={type(source_path_or_identifier)}")
    return []

# --- UI Sections ---
def show_target_config_and_mapping_section(
    active_source_headers: List[str],
    source_identifier_for_key: str,
    active_source_identifier: Any,
    active_is_db_query: bool,
    output_destination_type: str
    # target_db_manager is not passed here
):
    # ... (This function from your last successful run, with the 5-param signature) ...
    # ... (The TypeError was because the CALL in main() was passing 6, but this definition expected 5)
    # This definition is now aligned with the 5-param call from main()
    final_target_table_name_str = st.session_state.get('target_table_name_input')
    sql_type_target_schema = None
    detailed_target_schema_for_validation = None

    if output_destination_type == "Import to SQLite Database":
        st.markdown("**Target SQLite Table Name**")
        default_suggestion = "imported_data"
        if isinstance(active_source_identifier, Path): default_suggestion = sanitize_name(active_source_identifier.stem) or "imported_data"
        elif isinstance(active_source_identifier, str):
            if not active_is_db_query : default_suggestion = sanitize_name(active_source_identifier) or "imported_data"
            else: default_suggestion = "db_query_import"

        current_table_name_val = st.session_state.get('target_table_name_input', default_suggestion)
        if not current_table_name_val : current_table_name_val = default_suggestion

        table_name_from_widget = st.text_input(
            "Target SQLite Table Name:", value=current_table_name_val,
            key=f"target_table_name_widget_{sanitize_name(source_identifier_for_key)}"
        )
        if table_name_from_widget != st.session_state.get('target_table_name_input'):
             st.session_state['target_table_name_input'] = table_name_from_widget
             # Consider if a rerun is needed here if the table name is critical for other immediate UI updates
             # For now, let's assume it's picked up on the next action.
             # st.rerun() # Potentially add if needed

        final_target_table_name_str = sanitize_name(st.session_state.target_table_name_input)

        if not final_target_table_name_str:
            st.error("Target table name is required for SQLite import."); return None, None, None, None
        if final_target_table_name_str != st.session_state.target_table_name_input :
            st.info(f"Using sanitized target table name: `{final_target_table_name_str}`")

    st.divider()
    st.markdown("**Configure Output Columns & Naming**")
    if not active_source_headers:
        st.warning("Source headers not loaded."); return final_target_table_name_str, None, None, None

    if st.session_state.get('last_headers') != active_source_headers:
        logger.info(f"Headers changed for {source_identifier_for_key}. Resetting column mapping.")
        st.session_state.last_headers = active_source_headers
        st.session_state.column_mapping_state = { (sanitize_name(h) or f"col_{idx}"): h for idx, h in enumerate(active_source_headers)}

    current_mapping_state = st.session_state.get('column_mapping_state', {})
    user_selected_output_mapping = {}
    if output_destination_type == "Import to SQLite Database":
        sql_type_target_schema = {}; detailed_target_schema_for_validation = {}

    used_output_field_names = set(); validation_issues_in_mapping = False
    cols_per_row_map = 3; grid_cols_map = st.columns(cols_per_row_map)

    for i, source_header in enumerate(active_source_headers):
        col_idx_map = i % cols_per_row_map; container = grid_cols_map[col_idx_map].container(border=True)
        default_out_field = sanitize_name(source_header) or f"col_{i}"
        current_out_field_ui = default_out_field
        for out_f_key, src_h_val in current_mapping_state.items():
            if src_h_val == source_header: current_out_field_ui = out_f_key; break
        include_col = container.checkbox(f"`{source_header}`", value=True, key=f"include_{source_identifier_for_key}_{i}_{output_destination_type}")
        out_field_name_user_input = container.text_input("Output Field/Col Name:", value=current_out_field_ui, key=f"map_outfield_{source_identifier_for_key}_{i}_{output_destination_type}",label_visibility="collapsed", disabled=not include_col)
        if include_col:
            s_out_field_name = sanitize_name(out_field_name_user_input)
            if not s_out_field_name: container.error("Invalid name."); validation_issues_in_mapping=True
            elif s_out_field_name in used_output_field_names: container.error("Duplicate name."); validation_issues_in_mapping=True
            else:
                used_output_field_names.add(s_out_field_name)
                user_selected_output_mapping[s_out_field_name] = source_header
                if output_destination_type == "Import to SQLite Database" and sql_type_target_schema is not None and detailed_target_schema_for_validation is not None:
                    sql_type = "TEXT"; is_email = False; lower_out_name = s_out_field_name.lower(); lower_src_header = source_header.lower()
                    if any(kw in lower_out_name or kw in lower_src_header for kw in ['email','mail']): sql_type="TEXT UNIQUE"; is_email=True
                    elif any(kw in lower_out_name or kw in lower_src_header for kw in ['phone','tel']): sql_type="TEXT"
                    elif any(kw in lower_out_name or kw in lower_src_header for kw in ['amount','price','value','count','id','num','qty','salary']): sql_type="REAL"
                    elif any(kw in lower_out_name or kw in lower_src_header for kw in ['date','time','joined','created_at','updated_at','dob']): sql_type="DATETIME"
                    sql_type_target_schema[s_out_field_name] = sql_type
                    detailed_target_schema_for_validation[s_out_field_name] = {'type':sql_type, 'is_email':is_email, 'required':"NOT NULL" in sql_type.upper()}
                    container.caption(f"Target SQLite Type: `{sql_type}`")
    st.session_state.column_mapping_state = user_selected_output_mapping
    if not user_selected_output_mapping and active_source_headers : st.warning("No columns mapped for output."); validation_issues_in_mapping = True

    output_name_to_return = final_target_table_name_str if output_destination_type == "Import to SQLite Database" else "output_file_placeholder"
    if not validation_issues_in_mapping and user_selected_output_mapping:
        return output_name_to_return, user_selected_output_mapping, sql_type_target_schema, detailed_target_schema_for_validation
    else:
        return output_name_to_return, None, None, None

def show_data_preview_section(preview_source_df: Optional[pd.DataFrame], final_column_mapping: Optional[Mapping[str, str]]):
    with st.expander("📊 Mapped Data Preview (First 5 Rows)", expanded=True):
        if preview_source_df is None or preview_source_df.empty: st.info("No source data for preview."); return
        if not final_column_mapping:
            st.info("Configure column mapping/selection to see preview."); st.caption("Raw source preview:")
            st.dataframe(preview_source_df.astype(str).head(), use_container_width=True); return
        preview_to_display_dict = {}
        for output_header, source_header in final_column_mapping.items():
            if source_header in preview_source_df.columns: preview_to_display_dict[output_header] = preview_source_df[source_header]
            else: preview_to_display_dict[output_header] = pd.Series([None] * len(preview_source_df), dtype="object")
        if not preview_to_display_dict:
            st.warning("Mapped columns not in preview. Showing raw."); st.dataframe(preview_source_df.astype(str).head(), use_container_width=True)
        else:
            df_preview_final = pd.DataFrame(preview_to_display_dict)
            st.caption("Preview of data mapped for output (headers are output headers):")
            st.dataframe(df_preview_final.astype(str).head(), use_container_width=True)

def show_execute_destination_section(
    active_importer_instance: Any, active_source_identifier: Any, active_is_db_query: bool,
    output_destination_type: str,
    target_sqlite_db_manager: Optional[DatabaseManager],
    target_sqlite_table_name: Optional[str],
    final_output_mapping: Optional[Mapping[str, str]],
    sql_type_target_schema: Optional[Dict[str, str]],
    detailed_target_schema_for_validation: Optional[Dict[str, Any]],
    download_file_format: Optional[str], download_filename: Optional[str]
):
    with st.container(border=True):
        if output_destination_type == "Import to SQLite Database":
            st.subheader("3. Execute Import to Target SQLite DB", divider="rainbow")
            can_import = bool(target_sqlite_db_manager and target_sqlite_db_manager.connection and active_importer_instance and active_source_identifier and target_sqlite_table_name and final_output_mapping and sql_type_target_schema and detailed_target_schema_for_validation)
            if not can_import: st.warning("Complete configurations for SQLite import."); return
            source_display_name = Path(active_source_identifier).name if isinstance(active_source_identifier, Path) else str(active_source_identifier)
            st.markdown(f"Ready to import from `{source_display_name}` into SQLite table `{target_sqlite_table_name}`.")
            import_button_key = f"exec_sqlite_import_btn_{sanitize_name(str(active_source_identifier))}_{target_sqlite_table_name}"
            if st.button(f"🚀 Import to SQLite: '{target_sqlite_table_name}'", type="primary", use_container_width=True, key=import_button_key):
                with st.spinner(f"Importing into '{target_sqlite_table_name}'..."):
                    if not target_sqlite_db_manager.create_dynamic_table(target_sqlite_table_name, sql_type_target_schema):
                        st.error(f"Failed to create/verify target SQLite table '{target_sqlite_table_name}'."); st.rerun()

                    # Ensure detailed_target_schema_for_validation is not None before using in list comprehensions
                    val_schema = detailed_target_schema_for_validation or {}
                    schema_info_val_arg = {
                        'required': [k for k,v_dict in val_schema.items() if isinstance(v_dict, dict) and v_dict.get('required')],
                        'unique': [k for k,v_dict in val_schema.items() if isinstance(v_dict, dict) and "UNIQUE" in v_dict.get('type','').upper()]
                    }

                    if isinstance(active_importer_instance, BaseImporter):
                        if target_sqlite_db_manager:
                            active_importer_instance.db_manager = target_sqlite_db_manager
                            logger.info(f"Set db_manager for {active_importer_instance.__class__.__name__} to target: {target_sqlite_db_manager.db_path_for_connection}")
                        else:
                            st.error("Critical error: Target SQLite DB manager not available for file import operation.")
                            logger.error("Critical error: Target SQLite DB manager not available for file import operation in show_execute_destination_section.")
                            st.stop()
                        active_importer_instance.set_table_schema_info(detailed_target_schema_for_validation)
                        results_obj = active_importer_instance.process_import(Path(active_source_identifier), target_sqlite_table_name, final_output_mapping, schema_info_val_arg)
                        st.session_state['last_operation_results'] = results_obj.to_dict()
                    elif isinstance(active_importer_instance, DatabaseSourceImporter):
                        active_importer_instance.set_table_schema_info(detailed_target_schema_for_validation)
                        results_obj = active_importer_instance.process_import_to_target(target_sqlite_db_manager, str(active_source_identifier), active_is_db_query,target_sqlite_table_name, final_output_mapping, detailed_target_schema_for_validation, schema_info_val_arg)
                        st.session_state['last_operation_results'] = results_obj.to_dict()
                    else: st.error("Unknown importer type."); st.stop()
                st.rerun()
        elif output_destination_type == "Download as File":
            st.subheader("3. Prepare & Download File", divider="rainbow")
            can_download = bool(active_importer_instance and active_source_identifier and final_output_mapping and download_file_format and download_filename)
            if not can_download: st.warning("Complete source, column selection, format, and filename."); return
            st.markdown(f"Ready to prepare `{download_filename}` as `{download_file_format}`.")
            if st.button(f"💾 Prepare & Download {download_file_format}", type="primary", use_container_width=True, key=f"prep_download_{download_file_format}"):
                with st.spinner(f"Preparing {download_filename}..."):
                    try:
                        all_data_rows = []
                        if isinstance(active_importer_instance, DatabaseSourceImporter):
                            logger.info(f"Reading data for export from DB source: {active_source_identifier}")
                            all_data_rows = list(active_importer_instance.read_data_from_source(str(active_source_identifier), active_is_db_query))
                        elif isinstance(active_importer_instance, BaseImporter):
                            if not isinstance(active_source_identifier, Path): st.error("Invalid source for file export."); logger.error(f"File export with non-Path source: {active_source_identifier}"); st.stop()
                            logger.info(f"Reading data for export from file source: {active_source_identifier}")
                            all_data_rows = list(active_importer_instance.read_data(active_source_identifier))
                        else: st.error("Unknown importer type for data export."); logger.error(f"Unknown importer type for export: {type(active_importer_instance)}"); st.stop()

                        if not all_data_rows: st.warning("No data from source to export."); st.stop()
                        source_df = pd.DataFrame(all_data_rows)

                        output_df_cols = {}
                        if final_output_mapping:
                            for out_header, src_header in final_output_mapping.items():
                                if src_header in source_df.columns: output_df_cols[out_header] = source_df[src_header]
                                else: logger.warning(f"Source header '{src_header}' from mapping not found in DataFrame columns: {source_df.columns.tolist()}")
                        else: logger.warning("No column mapping for export; exporting all source_df columns."); output_df_cols = {col: source_df[col] for col in source_df.columns}

                        if not output_df_cols: st.error("No columns selected or available for output."); st.stop()
                        output_df = pd.DataFrame(output_df_cols)

                        file_bytes, mime_type = None, "application/octet-stream"
                        if download_file_format == "CSV": file_bytes = output_df.to_csv(index=False).encode('utf-8'); mime_type = "text/csv"
                        elif download_file_format == "JSON": file_bytes = output_df.to_json(orient="records", indent=2, lines=False).encode('utf-8'); mime_type = "application/json"
                        elif download_file_format == "Excel (.xlsx)":
                            excel_buffer = io.BytesIO();
                            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer: output_df.to_excel(writer, index=False, sheet_name='Sheet1')
                            file_bytes = excel_buffer.getvalue(); mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        if file_bytes: st.session_state.downloadable_file_prepared = {"data": file_bytes, "file_name": download_filename, "mime": mime_type}; st.success(f"{download_filename} ready!")
                        else: st.error(f"Could not generate {download_file_format} file.")
                    except AttributeError as ae: st.error(f"Internal error: Method not found - {ae}"); logger.exception(f"AttributeError during file export: {ae}")
                    except Exception as e: st.error(f"Error preparing file: {e}"); logger.exception("Error during file export prep:")
                st.rerun()
            if st.session_state.get('downloadable_file_prepared'):
                df_info = st.session_state.downloadable_file_prepared
                st.download_button(label=f"✅ Download: {df_info['file_name']}", data=df_info['data'], file_name=df_info['file_name'], mime=df_info['mime'], key="final_download_button_key", on_click=lambda: st.session_state.pop('downloadable_file_prepared', None))

def show_results_section():
    results = st.session_state.get('last_operation_results')
    if results:
        with st.container(border=True):
            st.subheader("📊 Last Operation Results", divider="rainbow")
            total = results.get('total', 0); inserted = results.get('inserted', 0); skipped = results.get('skipped', 0)
            col1, col2, col3 = st.columns(3)
            col1.metric("Rows Processed", total); col2.metric("Rows Inserted", inserted)
            col3.metric("Rows Skipped / Errors", skipped, delta=f"-{skipped}" if skipped > 0 else "0", delta_color="inverse" if skipped > 0 else "normal")
            errors = results.get('errors', [])
            if errors:
                with st.expander("⚠️ View Errors / Skipped Row Details", expanded=True):
                    try:
                        err_df = pd.DataFrame(errors);
                        for col_name in ['row', 'error', 'data']:
                            if col_name not in err_df.columns: err_df[col_name] = 'N/A' if col_name != 'data' else '{}'
                        err_df = err_df[['row', 'error', 'data']]
                        st.dataframe(err_df, use_container_width=True, height=200)
                        st.download_button(label="💾 Download Error Report (.csv)", data=err_df.to_csv(index=False).encode('utf-8'), file_name="import_error_report.csv", mime="text/csv", key="download_errors_button")
                    except Exception as e: st.error(f"Could not display error report: {e}"); logger.exception("Error processing errors for display:")
            elif total > 0 and inserted == total and skipped == 0 : st.info("Operation completed successfully with no errors.")
            elif total > 0 and inserted > 0 : st.info("Operation partially completed.")

# --- Main Application Flow ---
def main():
    st.title("🚀 Data Importer Pro")
    st.caption("Import data from files or databases, and export to SQLite or download as a file.")

    # Display Target DB Path input in the sidebar. This function now just returns the path.
    # The actual DatabaseManager instance for the target is fetched conditionally later.
    target_db_path_from_sidebar = show_target_db_path_input_ui()

    st.session_state.current_source_type = st.radio(
        "Select Data Source Type:", ("File Upload", "Database"),
        index=("File Upload", "Database").index(st.session_state.get('current_source_type', "File Upload")),
        horizontal=True, key="main_source_type_selector"
    )
    st.divider()

    active_importer_instance = None
    active_source_headers = st.session_state.get('active_source_headers', [])
    active_preview_data = None
    active_source_identifier = None
    active_is_db_query = False
    source_name_for_ui_keys = "default_source_key"

    # Step 1: Load Source Data
    if st.session_state.current_source_type == "File Upload":
        uploaded_file_info = show_upload_section()
        if uploaded_file_info:
            temp_file_path_str = st.session_state.get('temp_file_path')
            if not temp_file_path_str or st.session_state.get('current_processed_filename') != uploaded_file_info['name']:
                temp_file_path_obj = get_temporary_filepath(uploaded_file_info['content'], uploaded_file_info['name'])
                if temp_file_path_obj:
                    st.session_state.temp_file_path = str(temp_file_path_obj)
                    st.session_state.current_processed_filename = uploaded_file_info['name']
                    st.session_state.active_source_headers = []
                    st.session_state.last_headers = []
                else: st.stop()
            active_source_identifier = Path(st.session_state.temp_file_path) if st.session_state.get('temp_file_path') else None
            if active_source_identifier and active_source_identifier.exists():
                # For file importers, pass a dummy in-memory DB manager.
                # The actual target DB manager will be used if SQLite output is chosen.
                dummy_target_manager = DatabaseManager(":memory:") # Initialize dummy
                if not dummy_target_manager.connect(): # Must connect it
                    st.error("Failed to init dummy DB manager for file source processing."); st.stop()

                file_importer_instance = get_importer_for_file(active_source_identifier, dummy_target_manager)
                if file_importer_instance:
                    active_importer_instance = file_importer_instance
                    if not st.session_state.active_source_headers or st.session_state.last_headers != st.session_state.active_source_headers :
                         current_hdrs = get_cached_headers(active_importer_instance, active_source_identifier)
                         if current_hdrs :
                            if current_hdrs != st.session_state.active_source_headers:
                                st.session_state.active_source_headers = current_hdrs
                                st.session_state.last_headers = current_hdrs
                                st.rerun()
                            else: st.session_state.active_source_headers = current_hdrs # Ensure it's set
            if active_source_identifier : source_name_for_ui_keys = active_source_identifier.name

    elif st.session_state.current_source_type == "Database":
        show_database_source_connector_ui() # No target_db_manager needed here
        if st.session_state.get('source_db_connected') and st.session_state.get('source_db_importer_instance'):
            active_importer_instance = st.session_state.source_db_importer_instance
            active_source_headers = st.session_state.get('source_db_headers', [])
            active_preview_data = st.session_state.get('source_db_preview_data')
            active_source_identifier = st.session_state.get('source_db_selected_table') or st.session_state.get('source_db_custom_query')
            active_is_db_query = (st.session_state.get('source_db_specify_method') == "Custom SQL Query")
            if active_source_identifier: source_name_for_ui_keys = str(active_source_identifier)

    # Step 2 & 3: Configure Output & Transformation, then Execute
    if active_importer_instance and st.session_state.get('active_source_headers'):
        st.divider()
        with st.container(border=True):
            st.subheader("2. Define Output Destination & Column Mapping", divider="rainbow")

            output_destination_type = st.selectbox(
                "Choose Output Destination:", ["Download as File", "Import to SQLite Database"],
                key="output_destination_selector"
            )

            target_config_name = None; final_output_mapping = None
            sql_type_target_schema = None; detailed_target_schema_for_validation = None

            target_config_name, final_output_mapping, sql_type_target_schema, detailed_target_schema_for_validation = \
                show_target_config_and_mapping_section(
                    st.session_state.active_source_headers,
                    source_name_for_ui_keys,
                    active_source_identifier,
                    active_is_db_query,
                    output_destination_type
                )

            if final_output_mapping:
                preview_df_to_use = None
                if st.session_state.current_source_type == "File Upload" and isinstance(active_importer_instance, BaseImporter) and isinstance(active_source_identifier, Path):
                    try: preview_df_to_use = active_importer_instance.get_preview(active_source_identifier, num_rows=5)
                    except Exception as e: logger.warning(f"Preview failed for file {active_source_identifier}: {e}")
                elif st.session_state.current_source_type == "Database":
                    preview_df_to_use = active_preview_data
                if preview_df_to_use is not None: show_data_preview_section(preview_df_to_use, final_output_mapping)
                else: st.caption("Preview data could not be loaded.")
                st.divider()

                target_sqlite_db_manager_for_action = None
                target_sqlite_table_name_final = None
                download_file_format_for_action = None
                download_filename_for_action = None

                if output_destination_type == "Download as File":
                    download_file_format_for_action = st.selectbox("Select Download File Format:", ["CSV", "JSON", "Excel (.xlsx)"], key="download_format_selector")
                    base_fn = Path(source_name_for_ui_keys).stem if isinstance(active_source_identifier, Path) else sanitize_name(source_name_for_ui_keys) or "output"
                    ext_map = {"CSV": ".csv", "JSON": ".json", "Excel (.xlsx)": ".xlsx"}
                    sugg_dl_fn = f"{base_fn}{ext_map.get(download_file_format_for_action, '.dat')}"
                    download_filename_for_action = st.text_input("Download Filename:", value=sugg_dl_fn, key="download_filename_input")

                elif output_destination_type == "Import to SQLite Database":
                    target_sqlite_table_name_final = target_config_name

                    # Get path from sidebar widget
                    current_target_db_path = st.session_state.target_db_path_widget

                    if current_target_db_path != st.session_state.get('db_manager_current_path') or not st.session_state.get('target_db_manager_instance'):
                        if current_target_db_path:
                             get_target_db_manager_cached.clear()
                             target_sqlite_db_manager_for_action = get_target_db_manager_cached(current_target_db_path)
                             st.session_state.target_db_manager_instance = target_sqlite_db_manager_for_action
                             st.session_state.db_manager_current_path = current_target_db_path
                        else: target_sqlite_db_manager_for_action = None
                    else:
                        target_sqlite_db_manager_for_action = st.session_state.get('target_db_manager_instance')

                    if target_sqlite_db_manager_for_action and target_sqlite_db_manager_for_action.connection:
                        st.success(f"Target SQLite DB for import: `{Path(target_sqlite_db_manager_for_action.db_path_for_connection).name}` (from sidebar config)")
                    elif current_target_db_path: st.error("Target SQLite DB (from sidebar) not connected. Check path.")
                    else: st.warning("Specify Target SQLite Database path (in sidebar) to enable import.")

                show_execute_destination_section(
                    active_importer_instance, active_source_identifier, active_is_db_query,
                    output_destination_type, target_sqlite_db_manager_for_action,
                    target_sqlite_table_name_final, final_output_mapping,
                    sql_type_target_schema, detailed_target_schema_for_validation,
                    download_file_format_for_action, download_filename_for_action
                )
            else: st.info("Configure output columns/mapping to proceed.")

    else:
        target_db_path_display = st.session_state.get('target_db_path_widget')
        if not st.session_state.get('target_db_manager_instance') and target_db_path_display:
             st.sidebar.caption(f"Target SQLite DB for '{target_db_path_display}' will be used if 'Import to SQLite' is chosen as destination.")
        if st.session_state.current_source_type == "File Upload" and not st.session_state.get('uploaded_file_info'):
            st.info("⬆️ Upload a data file (CSV, JSON, XLSX) to begin.")
        elif st.session_state.current_source_type == "Database" and not st.session_state.get('source_db_connected'):
            st.info("🔗 Connect to a source database to begin.")

    show_results_section()

if __name__ == "__main__":
    main()
