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
from typing import List, Dict, Type, Optional, Mapping, Any # Ensure Any is imported

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
    initial_sidebar_state="expanded"
)

# --- Initialize Session State ---
# (Keeping your comprehensive session state initialization)
if 'db_path_input' not in st.session_state:
    st.session_state.db_path_input = "data/db/importer_pro.db"
if 'db_manager' not in st.session_state:
    st.session_state.db_manager = None
if 'uploaded_file_info' not in st.session_state: st.session_state.uploaded_file_info = None
if 'temp_file_path' not in st.session_state: st.session_state.temp_file_path = None
if 'current_processed_filename' not in st.session_state: st.session_state.current_processed_filename = None
if 'active_source_headers' not in st.session_state: st.session_state.active_source_headers = []
if 'last_headers' not in st.session_state: st.session_state.last_headers = []
if 'column_mapping_state' not in st.session_state: st.session_state.column_mapping_state = {}
if 'target_table_name_input' not in st.session_state: st.session_state.target_table_name_input = ""
if 'last_import_results' not in st.session_state: st.session_state.last_import_results = None
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


# --- Importer Factory ---
def get_importer_for_file(file_path: Path, db_manager: DatabaseManager) -> Optional[BaseImporter]:
    extension = file_path.suffix.lower()
    importer_class = AVAILABLE_IMPORTERS.get(extension)
    if importer_class:
        logger.info(f"Found file importer {importer_class.__name__} for extension '{extension}'")
        try:
            return importer_class(db_manager)
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
def get_db_manager(db_path_input) -> Optional[DatabaseManager]:
    logger.info(f"Requesting TARGET DB Manager for path: {db_path_input}")
    if not db_path_input or not isinstance(db_path_input, (str, Path)):
         logger.error(f"Invalid target db_path_input type or value: {db_path_input}")
         st.error("Invalid target database path provided.")
         return None
    resolved_path = Path(db_path_input).resolve()
    resolved_path_str = str(resolved_path)
    db_folder = resolved_path.parent
    try:
        db_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured target database directory exists: {db_folder}")
    except Exception as e:
        st.warning(f"Could not create directory {db_folder} for target database: {e}. Check permissions.")
    db_manager = DatabaseManager(resolved_path_str)
    if not db_manager.connect():
        st.error(f"Failed to connect to target database: {db_manager.db_path.name}")
        return None
    logger.info(f"TARGET DB Manager connection successful for: {db_manager.db_path}")
    return db_manager

def generate_timestamped_filename(original_name):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    base, ext = Path(original_name).stem, Path(original_name).suffix
    safe_base = re.sub(r'[^\w\.-]', '_', base)
    return f"{safe_base}_{now}{ext}"

# --- UI Components ---

def show_target_db_config_section():
    with st.sidebar.expander("⚙️ Target SQLite Database Settings", expanded=True):
        default_db_path = st.session_state.get('db_path_input', "data/db/importer_pro.db")
        db_path_input_val = st.text_input(
            "Target SQLite Database File Path:",
            value=default_db_path,
            key="target_db_path_widget",
            help="Path to the SQLite database file where data will be imported (e.g., data/my_imports.db)."
        )
        if db_path_input_val != st.session_state.db_path_input:
             st.session_state.db_path_input = db_path_input_val
             if 'db_manager' in st.session_state:
                 del st.session_state['db_manager']
             get_db_manager.clear()
             logger.info(f"Target DB path changed to {db_path_input_val}, cleared cached DB manager and instance.")
             st.rerun()
        else:
            st.session_state.db_path_input = db_path_input_val

        db_manager_instance = None
        if st.session_state.db_path_input:
            db_manager_instance = get_db_manager(st.session_state.db_path_input)
            st.session_state.db_manager = db_manager_instance
        else:
            st.info("Enter a target database path to connect.")
            st.session_state.db_manager = None

        if db_manager_instance and db_manager_instance.connection:
             st.success(f"Target DB Connected: `{db_manager_instance.db_path.name}`")
             try:
                 cursor = db_manager_instance.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
                 if cursor:
                      tables = cursor.fetchall()
                      if tables:
                           st.markdown("**Existing Tables in Target DB:**")
                           st.markdown('\n'.join([f"- `{t[0]}`" for t in tables]))
                      else:
                           st.caption("_No user tables found in target DB._")
                 else:
                      st.warning("Could not retrieve table list (cursor error).")
             except Exception as e:
                  st.warning(f"Could not list tables from target DB: {e}")
                  logger.exception("Error fetching table list:")

             db_file_to_download = Path(db_manager_instance.db_path).resolve()
             if db_file_to_download.exists() and db_file_to_download.is_file():
                 try:
                     with open(db_file_to_download, "rb") as fp:
                         st.download_button(
                             label="💾 Download Target Database File", data=fp,
                             file_name=db_file_to_download.name, mime="application/vnd.sqlite3"
                         )
                 except Exception as e:
                     st.error(f"Error preparing target DB for download: {e}")
                     logger.error(f"Error reading database file for download {db_file_to_download}: {e}")
             elif db_path_input_val :
                 st.caption("Database file may not exist yet or path is incorrect. Import data to create it.")
        elif db_path_input_val:
            st.error("Target DB Connection Failed. Check path and permissions.")
    return st.session_state.get('db_manager')

def show_upload_section():
    with st.container(border=True):
        st.subheader("1. Upload Data File", divider="rainbow")
        supported_types_str = ", ".join(f"`{ext.lstrip('.')}`" for ext in AVAILABLE_IMPORTERS.keys())
        st.markdown(f"Select the file you want to import. Supported types: {supported_types_str}")
        uploaded_file = st.file_uploader(
            "Choose a file", type=[ext.lstrip('.') for ext in AVAILABLE_IMPORTERS.keys()],
            label_visibility="collapsed", key="file_uploader_widget"
        )
        if uploaded_file is not None:
            is_new_or_different_file = False
            current_file_info = st.session_state.get('uploaded_file_info')
            if current_file_info is None or not isinstance(current_file_info, dict):
                is_new_or_different_file = True
            elif current_file_info.get('name') != uploaded_file.name or \
                 current_file_info.get('size') != uploaded_file.size:
                is_new_or_different_file = True
            if is_new_or_different_file:
                st.session_state.uploaded_file_info = {
                    'name': uploaded_file.name, 'size': uploaded_file.size,
                    'type': uploaded_file.type, 'content': uploaded_file.getvalue()
                }
                st.session_state.pop('temp_file_path', None)
                old_file_name_for_cache_key = st.session_state.get("current_processed_filename", "old_default_key")
                st.session_state.pop(f'headers_{old_file_name_for_cache_key}', None)
                st.session_state.active_source_headers = []
                st.session_state.last_headers = []
                st.session_state.column_mapping_state = {}
                st.session_state.target_table_name_input = ""
                st.session_state.pop('last_import_results', None)
                logger.info(f"New file uploaded: {uploaded_file.name}. Cleared dependent state.")
                st.success(f"File `{uploaded_file.name}` selected ({uploaded_file.size} bytes).")
                st.rerun()
        elif uploaded_file is None and st.session_state.get('uploaded_file_info') is not None:
             logger.info("Uploaded file removed by user (uploader is None). Clearing associated state.")
             old_file_info = st.session_state.pop('uploaded_file_info', None)
             if old_file_info and isinstance(old_file_info, dict):
                 old_file_name_for_cache_key = old_file_info.get('name', 'old_default_key')
                 st.session_state.pop(f'headers_{old_file_name_for_cache_key}', None)
             st.session_state.pop('temp_file_path', None)
             st.session_state.active_source_headers = []
             st.session_state.last_headers = []
             st.session_state.column_mapping_state = {}
             st.session_state.pop('last_import_results', None)
             st.rerun()
    return st.session_state.get('uploaded_file_info')

def show_database_source_connector_ui(target_db_manager: Optional[DatabaseManager]):
    with st.container(border=True):
        st.subheader("1. Configure Source Database Connection", divider="rainbow")
        st.session_state.source_db_type = st.selectbox(
            "Select Source Database Type:", ["SQLite", "PostgreSQL", "MySQL"],
            index=["SQLite", "PostgreSQL", "MySQL"].index(st.session_state.get('source_db_type', "SQLite")),
            key="source_db_type_widget"
        )
        default_conn_str = ""
        if st.session_state.source_db_type == "SQLite": default_conn_str = "sqlite:///./your_source_database.db"
        elif st.session_state.source_db_type == "PostgreSQL": default_conn_str = "postgresql://user:password@host:port/dbname"
        elif st.session_state.source_db_type == "MySQL": default_conn_str = "mysql+mysqlconnector://user:password@host:port/dbname"
        st.session_state.source_db_conn_string = st.text_input(
            "Source Database Connection String:", value=st.session_state.get('source_db_conn_string', default_conn_str),
            help=f"Example: {default_conn_str}", key="source_db_conn_str_widget", type="password"
        )
        active_source_db_importer = st.session_state.get('source_db_importer_instance')
        col_connect, col_disconnect = st.columns(2)
        with col_connect:
            if st.button("🔗 Connect to Source DB", key="connect_source_db_btn", use_container_width=True):
                if not st.session_state.source_db_conn_string: st.error("Connection string is required."); st.stop()
                if not target_db_manager or not target_db_manager.connection: st.error("Target SQLite database is not connected."); st.stop()
                if active_source_db_importer and active_source_db_importer.source_engine: active_source_db_importer.close_source_connection()
                new_importer_instance = DatabaseSourceImporter(target_db_manager)
                if new_importer_instance.connect_to_source(st.session_state.source_db_conn_string):
                    st.session_state.update({
                        'source_db_connected': True, 'source_db_importer_instance': new_importer_instance,
                        'source_db_tables': new_importer_instance.get_table_names_from_source(),
                        'active_source_headers': [], 'source_db_preview_data': None,
                        'column_mapping_state': {}, 'target_table_name_input': "",
                        'last_import_results': None, 'last_headers': []
                    })
                    st.success(f"Connected to {st.session_state.source_db_type} source!")
                else:
                    st.error(f"Failed to connect to {st.session_state.source_db_type}. Check string/logs.")
                    st.session_state.update({
                        'source_db_connected': False, 'source_db_importer_instance': None,
                        'source_db_tables': [], 'active_source_headers': [], 'source_db_preview_data': None
                    })
                st.rerun()
        with col_disconnect:
            current_source_db_importer = st.session_state.get('source_db_importer_instance')
            if st.session_state.get('source_db_connected') and current_source_db_importer:
                if st.button("❌ Disconnect Source DB", key="disconnect_source_db_btn", use_container_width=True):
                    current_source_db_importer.close_source_connection()
                    st.session_state.update({
                        'source_db_connected': False, 'source_db_importer_instance': None,
                        'source_db_tables': [], 'active_source_headers': [],
                        'source_db_preview_data': None, 'column_mapping_state': {},
                        'target_table_name_input': ""
                    })
                    logger.info("Disconnected from source database by user."); st.rerun()
        current_importer_for_display = st.session_state.get('source_db_importer_instance')
        if st.session_state.get('source_db_connected') and current_importer_for_display:
            engine_name = current_importer_for_display.source_engine.name if current_importer_for_display.source_engine else 'N/A'
            st.info(f"Connected to source: {st.session_state.source_db_type}. Engine: {engine_name}")
            st.session_state.source_db_specify_method = st.radio(
                "Specify data source:", ["Select Table", "Custom SQL Query"],
                index=["Select Table", "Custom SQL Query"].index(st.session_state.get('source_db_specify_method', "Select Table")),
                key="source_db_specify_method_widget", horizontal=True
            )
            source_identifier_val, is_query_val = (st.session_state.source_db_custom_query, True) if st.session_state.source_db_specify_method == "Custom SQL Query" else (st.session_state.get('source_db_selected_table'), False)
            if st.session_state.source_db_specify_method == "Select Table":
                if st.session_state.source_db_tables:
                    current_selected_table = st.session_state.get('source_db_selected_table')
                    table_options = st.session_state.source_db_tables
                    table_index = table_options.index(current_selected_table) if current_selected_table in table_options else 0
                    source_identifier_val = st.selectbox("Select Source Table:", options=table_options, index=table_index, key="source_db_table_select_widget")
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
                                st.session_state.column_mapping_state = {sanitize_name(h) or f"col_{idx}" : h for idx, h in enumerate(headers)}
                                st.session_state.last_headers = headers
                                default_target_table = sanitize_name(source_identifier_val if not is_query_val else "db_query_import") or "imported_data"
                                st.session_state.target_table_name_input = default_target_table
                        except Exception as e:
                            st.error(f"Error loading data from source DB: {e}")
                            logger.error(f"Error loading from DB source '{source_identifier_val}': {e}", exc_info=True)
                            st.session_state.active_source_headers = []; st.session_state.source_db_preview_data = None
                    st.rerun()

@st.cache_data(show_spinner=False)
def get_temporary_filepath(_file_content_bytes: bytes, original_filename: str) -> Optional[Path]:
    try:
        temp_file_name = generate_timestamped_filename(original_filename)
        temp_path = (TEMP_DIR / temp_file_name).resolve()
        with open(temp_path, "wb") as f: f.write(_file_content_bytes)
        logger.info(f"File content cached to temporary path: {temp_path}")
        return temp_path
    except Exception as e:
        st.error(f"Failed to create temporary file: {e}"); logger.exception("Error creating temporary file:")
        return None

@st.cache_data(show_spinner=False)
def get_cached_headers(_importer_instance_generic: Any,
                       source_path_or_identifier: Any,
                       is_query_for_db: bool = False) -> List[str]:
    if isinstance(_importer_instance_generic, BaseImporter) and isinstance(source_path_or_identifier, Path):
        logger.info(f"Cache miss/call: Reading file headers for {source_path_or_identifier.name} using {_importer_instance_generic.__class__.__name__}")
        try:
            headers = _importer_instance_generic.get_headers(source_path_or_identifier)
            logger.info(f"Successfully read file headers for {source_path_or_identifier.name}: {headers}")
            return headers
        except Exception as e:
            st.error(f"Could not read headers from file: {e}"); logger.exception(f"Error reading file headers for {source_path_or_identifier}:")
            return []
    elif isinstance(_importer_instance_generic, DatabaseSourceImporter) and isinstance(source_path_or_identifier, str):
        logger.info(f"Cache miss/call: Reading DB headers for {source_path_or_identifier} using {_importer_instance_generic.__class__.__name__}")
        try:
            headers = _importer_instance_generic.get_headers_from_source(source_path_or_identifier, is_query_for_db)
            logger.info(f"Successfully read DB headers for {source_path_or_identifier}: {headers}")
            return headers
        except Exception as e:
            st.error(f"Could not read headers from DB source: {e}"); logger.exception(f"Error reading DB headers for {source_path_or_identifier}:")
            return []
    logger.warning(f"get_cached_headers called with unhandled type: Importer={type(_importer_instance_generic)}, Source={type(source_path_or_identifier)}")
    return []

def show_target_config_and_mapping_section(
    target_db_manager: DatabaseManager,
    active_source_headers: List[str],
    source_identifier_for_key: str,
    active_source_identifier: Any, # Added
    active_is_db_query: bool      # Added
):
    with st.container(border=True):
        st.subheader("2. Configure Import Target (to SQLite)", divider="rainbow")

        default_suggestion = "imported_data"
        if isinstance(active_source_identifier, Path):
            default_suggestion = sanitize_name(active_source_identifier.stem) or "imported_data"
        elif isinstance(active_source_identifier, str):
            if not active_is_db_query :
                default_suggestion = sanitize_name(active_source_identifier) or "imported_data"
            else:
                default_suggestion = "db_query_import"

        current_target_table_name_from_state = st.session_state.get('target_table_name_input', default_suggestion)
        if not current_target_table_name_from_state:
             current_target_table_name_from_state = default_suggestion

        table_name_from_widget = st.text_input(
            "Target SQLite Table Name:",
            value=current_target_table_name_from_state,
            key=f"target_table_name_widget_{sanitize_name(source_identifier_for_key)}"
        )
        st.session_state['target_table_name_input'] = table_name_from_widget

        final_target_table_name = sanitize_name(table_name_from_widget)

        if not final_target_table_name:
            st.error("Target table name is required and cannot be empty after sanitization.")
            return None, None, None, None

        if final_target_table_name != table_name_from_widget:
            st.info(f"Using sanitized target table name: `{final_target_table_name}`")

        st.divider()
        st.markdown("**Map Source Columns to Target SQLite Fields**")
        if not active_source_headers:
            st.warning("Source headers not loaded. Please load data from source first.")
            return final_target_table_name, None, None, None

        if st.session_state.get('last_headers') != active_source_headers:
            logger.info(f"Active headers changed for {source_identifier_for_key}. Resetting column mapping state.")
            st.session_state.last_headers = active_source_headers
            st.session_state.column_mapping_state = { (sanitize_name(h) or f"col_{idx}"): h for idx, h in enumerate(active_source_headers)}

        current_mapping_state = st.session_state.get('column_mapping_state', {})

        user_selected_mapping = {}
        sql_type_target_schema = {}
        detailed_target_schema_for_validation = {}
        used_target_db_fields = set()
        validation_issues = False

        cols_per_row_map = 3
        grid_cols_map = st.columns(cols_per_row_map)

        for i, source_header in enumerate(active_source_headers):
            col_idx_map = i % cols_per_row_map
            container = grid_cols_map[col_idx_map].container(border=True)
            default_target_db_field = sanitize_name(source_header) or f"column_{i}"
            current_target_db_field_for_ui = default_target_db_field
            for db_f_key, src_h_val in current_mapping_state.items():
                if src_h_val == source_header:
                    current_target_db_field_for_ui = db_f_key
                    break
            include_col = container.checkbox(f"`{source_header}`", value=True, key=f"include_{source_identifier_for_key}_{i}")
            db_field_input_from_user = container.text_input(
                "Target DB Field:", value=current_target_db_field_for_ui,
                key=f"map_db_{source_identifier_for_key}_{i}",
                label_visibility="collapsed", disabled=not include_col
            )
            if include_col:
                sanitized_db_field_input = sanitize_name(db_field_input_from_user)
                if not sanitized_db_field_input: container.error("Invalid"); validation_issues=True
                elif sanitized_db_field_input in used_target_db_fields: container.error("Duplicate"); validation_issues=True
                else:
                    used_target_db_fields.add(sanitized_db_field_input)
                    user_selected_mapping[sanitized_db_field_input] = source_header
                    sql_type_str = "TEXT"
                    is_email_flag = False
                    lower_db_name = sanitized_db_field_input.lower()
                    lower_header = source_header.lower()
                    if any(kw in lower_db_name or kw in lower_header for kw in ['email', 'mail']):
                        sql_type_str = "TEXT UNIQUE"; is_email_flag = True
                    elif any(kw in lower_db_name or kw in lower_header for kw in ['phone', 'tel']): sql_type_str = "TEXT"
                    elif any(kw in lower_db_name or kw in lower_header for kw in ['amount', 'price', 'salary', 'value', 'count', 'id', 'num', 'quantity', 'qty']): sql_type_str = "REAL"
                    elif any(kw in lower_db_name or kw in lower_header for kw in ['date', 'time', 'joined', 'created_at', 'updated_at', 'dob']): sql_type_str = "DATETIME"
                    sql_type_target_schema[sanitized_db_field_input] = sql_type_str
                    detailed_target_schema_for_validation[sanitized_db_field_input] = {
                        'type': sql_type_str, 'is_email': is_email_flag,
                        'required': "NOT NULL" in sql_type_str.upper()
                    }
                    container.caption(f"Target Type: `{sql_type_str}`")
        st.session_state.column_mapping_state = user_selected_mapping

        if not user_selected_mapping and active_source_headers : st.warning("No columns mapped."); validation_issues = True

    if not validation_issues and user_selected_mapping:
        return final_target_table_name, user_selected_mapping, sql_type_target_schema, detailed_target_schema_for_validation
    else:
        return final_target_table_name, None, None, None

def show_data_preview_section(preview_source_df: Optional[pd.DataFrame],
                              final_column_mapping: Optional[Mapping[str, str]]):
    with st.expander("📊 Mapped Data Preview (First 5 Rows)", expanded=False):
        if preview_source_df is None or preview_source_df.empty:
            st.info("No source data available for preview.")
            return
        if not final_column_mapping:
            st.info("Configure column mapping to see a relevant preview.")
            st.caption("Raw source preview:")
            st.dataframe(preview_source_df.astype(str).head(), use_container_width=True)
            return
        preview_to_display_dict = {}
        for target_db_field, source_header in final_column_mapping.items():
            if source_header in preview_source_df.columns:
                preview_to_display_dict[target_db_field] = preview_source_df[source_header]
            else:
                preview_to_display_dict[target_db_field] = pd.Series([None] * len(preview_source_df), dtype="object")
        if not preview_to_display_dict:
            st.warning("Could not generate mapped preview. Showing raw preview if available.")
            st.dataframe(preview_source_df.astype(str).head(), use_container_width=True)
        else:
            df_preview_final = pd.DataFrame(preview_to_display_dict)
            st.caption("Preview of data as it will be mapped to target database fields:")
            st.dataframe(df_preview_final.astype(str).head(), use_container_width=True)

def show_execute_import_section(
    target_db_manager: DatabaseManager, active_importer_instance: Any,
    active_source_identifier: Any, active_is_db_query: bool,
    target_table_name_str: str, final_col_mapping: Mapping[str, str],
    sql_type_target_schema: Dict[str, str],
    detailed_target_schema_for_validation: Dict[str, Any]
):
    with st.container(border=True):
        st.subheader("3. Execute Import to Target SQLite DB", divider="rainbow")
        can_import = bool(target_db_manager and active_importer_instance and active_source_identifier and target_table_name_str and final_col_mapping and sql_type_target_schema and detailed_target_schema_for_validation)
        if not can_import: st.warning("Please complete all configuration steps above.")
        else:
            source_display_name = Path(active_source_identifier).name if isinstance(active_source_identifier, Path) else str(active_source_identifier)
            st.markdown(f"Ready to import from `{source_display_name}` into SQLite table `{target_table_name_str}`.")
        import_button_key = f"exec_import_btn_{sanitize_name(str(active_source_identifier))}_{target_table_name_str}"
        if st.button(f"🚀 Import to SQLite: '{target_table_name_str}'", type="primary", disabled=not can_import, use_container_width=True, key=import_button_key):
            with st.spinner(f"Importing into '{target_table_name_str}'..."):
                if not target_db_manager.create_dynamic_table(target_table_name_str, sql_type_target_schema):
                    st.error(f"Failed to create/verify target SQLite table '{target_table_name_str}'. Import cancelled.")
                    st.session_state['last_import_results'] = {'errors': [{'error': f'Target table creation failed for {target_table_name_str}.', 'row':'N/A', 'data':''}]}
                    st.rerun()

                schema_info_for_validation_arg = {
                    'required': [k for k,v_dict in detailed_target_schema_for_validation.items() if isinstance(v_dict, dict) and v_dict.get('required')],
                    'unique': [k for k,v_dict in detailed_target_schema_for_validation.items() if isinstance(v_dict, dict) and "UNIQUE" in v_dict.get('type','').upper()]
                }
                if isinstance(active_importer_instance, BaseImporter):
                    logger.info(f"Using File Importer ({active_importer_instance.__class__.__name__}) for source: {active_source_identifier}")
                    active_importer_instance.set_table_schema_info(detailed_target_schema_for_validation)
                    import_result_obj = active_importer_instance.process_import(
                        Path(active_source_identifier), target_table_name_str,final_col_mapping, schema_info_for_validation_arg
                    )
                    st.session_state['last_import_results'] = import_result_obj.to_dict()
                elif isinstance(active_importer_instance, DatabaseSourceImporter):
                    logger.info(f"Using Database Source Importer for source: {active_source_identifier}")
                    active_importer_instance.set_table_schema_info(detailed_target_schema_for_validation)
                    try:
                        # Call the actual processing method
                        results_obj = active_importer_instance.process_import_to_target(
                            active_source_identifier, active_is_db_query,
                            target_table_name_str, final_col_mapping,
                            detailed_target_schema_for_validation,
                            schema_info_for_validation_arg
                        )
                        st.session_state['last_import_results'] = results_obj.to_dict()
                        if not results_obj.errors: # Check if there were errors during the call
                           st.success(f"Database source import process for table '{target_table_name_str}' completed.")
                        else:
                           st.warning(f"Database source import for table '{target_table_name_str}' completed with errors.")

                    except Exception as e:
                        st.error(f"Database source import failed: {e}")
                        logger.exception(f"Error during DatabaseSourceImporter processing for {target_table_name_str}")
                        st.session_state['last_import_results'] = {'errors': [{'row': 'Critical', 'error': f'DB Source Import Failed: {e}', 'data': 'N/A'}]}
                else: st.error("Unknown importer type. Cannot proceed.")
            st.rerun()

def show_results_section():
    results = st.session_state.get('last_import_results')
    if results:
        with st.container(border=True):
            st.subheader("📊 Last Import Results", divider="rainbow")
            total = results.get('total', 0); inserted = results.get('inserted', 0); skipped = results.get('skipped', 0)
            col1, col2, col3 = st.columns(3)
            col1.metric("Rows Processed", total); col2.metric("Rows Inserted", inserted)
            col3.metric("Rows Skipped / Errors", skipped, delta=f"-{skipped}" if skipped > 0 else "0", delta_color="inverse" if skipped > 0 else "normal")
            errors = results.get('errors', [])
            if errors:
                with st.expander("⚠️ View Errors / Skipped Row Details", expanded=True):
                    try:
                        err_df = pd.DataFrame(errors);
                        for col in ['row', 'error', 'data']:
                            if col not in err_df.columns: err_df[col] = 'N/A' if col != 'data' else '{}'
                        err_df = err_df[['row', 'error', 'data']]
                        st.dataframe(err_df, use_container_width=True, height=200)
                        st.download_button(label="💾 Download Error Report (.csv)", data=err_df.to_csv(index=False).encode('utf-8'), file_name="import_error_report.csv", mime="text/csv", key="download_errors_button")
                    except Exception as e: st.error(f"Could not display error report: {e}"); logger.exception("Error processing errors for display:")
            elif inserted > 0 : st.info("No errors reported during the last import.")

# --- Main Application Flow ---
def main():
    st.title("🚀 Data Importer Pro")
    st.caption("Upload, map, and import data from files (CSV, JSON, XLSX) or databases into your SQLite database.")

    target_db_manager = show_target_db_config_section()

    st.session_state.current_source_type = st.radio(
        "Select Data Source Type:", ("File Upload", "Database"),
        index=("File Upload", "Database").index(st.session_state.get('current_source_type', "File Upload")),
        horizontal=True, key="source_type_selector"
    )
    st.divider()

    active_importer_instance = None
    active_source_headers = st.session_state.get('active_source_headers', [])
    active_preview_data = None
    active_source_identifier = None
    active_is_db_query = False

    if st.session_state.current_source_type == "File Upload":
        uploaded_file_info = show_upload_section()
        if uploaded_file_info and target_db_manager and target_db_manager.connection:
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
                file_importer_instance = get_importer_for_file(active_source_identifier, target_db_manager)
                if file_importer_instance:
                    active_importer_instance = file_importer_instance
                    if not st.session_state.active_source_headers :
                         current_hdrs = get_cached_headers(active_importer_instance, active_source_identifier)
                         if current_hdrs and current_hdrs != st.session_state.active_source_headers:
                            st.session_state.active_source_headers = current_hdrs
                            st.session_state.last_headers = current_hdrs
                            if current_hdrs: st.rerun()

    elif st.session_state.current_source_type == "Database":
        show_database_source_connector_ui(target_db_manager)
        if st.session_state.get('source_db_connected') and st.session_state.get('source_db_importer_instance'):
            active_importer_instance = st.session_state.source_db_importer_instance
            active_source_headers = st.session_state.get('source_db_headers', [])
            active_preview_data = st.session_state.get('source_db_preview_data')
            active_source_identifier = st.session_state.get('source_db_selected_table') or st.session_state.get('source_db_custom_query')
            active_is_db_query = (st.session_state.get('source_db_specify_method') == "Custom SQL Query")

    if target_db_manager and target_db_manager.connection and active_importer_instance and st.session_state.get('active_source_headers'):
        st.divider()
        source_name_for_ui_keys = Path(active_source_identifier).name if isinstance(active_source_identifier, Path) else str(active_source_identifier)

        target_table_name, final_mapping, sql_type_target_schema, detailed_target_schema_for_validation = show_target_config_and_mapping_section(
            target_db_manager,
            st.session_state.active_source_headers,
            source_name_for_ui_keys,
            active_source_identifier,
            active_is_db_query
        )
        if target_table_name and final_mapping and sql_type_target_schema and detailed_target_schema_for_validation:
            preview_df_for_display = None
            if st.session_state.current_source_type == "File Upload" and isinstance(active_importer_instance, BaseImporter) and isinstance(active_source_identifier, Path):
                preview_df_for_display = active_importer_instance.get_preview(active_source_identifier, num_rows=5)
            elif st.session_state.current_source_type == "Database":
                preview_df_for_display = active_preview_data
            show_data_preview_section(preview_df_for_display, final_mapping)
            st.divider()
            show_execute_import_section(
                target_db_manager, active_importer_instance, active_source_identifier,
                active_is_db_query, target_table_name, final_mapping,
                sql_type_target_schema,
                detailed_target_schema_for_validation
            )

    elif target_db_manager and target_db_manager.connection:
        if st.session_state.current_source_type == "File Upload" and not st.session_state.get('uploaded_file_info'):
            st.info("⬆️ Upload a data file (CSV, JSON, XLSX) to begin.")
        elif st.session_state.current_source_type == "Database" and not st.session_state.get('source_db_connected'):
            st.info("🔗 Connect to a source database to begin.")
    elif not target_db_manager and st.session_state.get('db_path_input'):
         st.sidebar.error("Target SQLite Database not connected. Please check the path in the sidebar and ensure it's valid.")

    show_results_section()

if __name__ == "__main__":
    main()
