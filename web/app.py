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
from typing import List, Dict, Type, Optional, Mapping

import pandas as pd
import streamlit as st

from core.database import DatabaseManager
from core.importers.base_importer import BaseImporter, ImportResult
from core.importers import AVAILABLE_IMPORTERS, CSVImporter

# --- Configure logging---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TEMP_DIR = ROOT_DIR / "data" / "temp_uploads"
# Only clean up files older than 1 hour
try:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    now = time.time()
    for f in TEMP_DIR.glob("*"):
        if now - f.stat().st_mtime > 3600:  # Delete if older than 1 hour
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

# --- Importer Factory ---
def get_importer_for_file(file_path: Path, db_manager: DatabaseManager) -> Optional[BaseImporter]:
    """Factory function to get an importer instance based on file extension."""
    extension = file_path.suffix.lower()
    importer_class = AVAILABLE_IMPORTERS.get(extension) # Use registry from __init__
    if importer_class:
        logger.info(f"Found importer {importer_class.__name__} for extension '{extension}'")
        try:
            return importer_class(db_manager)
        except Exception as e:
            logger.exception(f"Failed to instantiate importer {importer_class.__name__}")
            st.error(f"Error initializing importer for {extension} files: {e}")
            return None
    else:
        st.error(f"Unsupported file type: '{extension}'. No importer found.")
        logger.warning(f"No importer registered for file extension: {extension}")
        return None

# --- Helper Functions ---
def sanitize_name(name):
    """Sanitizes a string to be a valid SQL table/column name."""
    if not isinstance(name, str): name = str(name)
    name = re.sub(r'[^\w_]', '_', name) # Allow letters, numbers, underscore
    if name and name[0].isdigit(): name = "_" + name # Prepend underscore if starts with digit
    if not name: return None # Return None if empty after sanitization
    return name.lower() # Convert to lowercase

@st.cache_resource # Cache the DB manager per session based on path
def get_db_manager(db_path_input) -> Optional[DatabaseManager]:
    """Gets or initializes the DatabaseManager, cached."""
    logger.info(f"Requesting DB Manager for path: {db_path_input}")
    # Ensure the input path is resolved to a string for caching consistency
    if not db_path_input or not isinstance(db_path_input, (str, Path)):
         logger.error(f"Invalid db_path_input type or value: {db_path_input}")
         st.error("Invalid database path provided.")
         return None

    resolved_path = Path(db_path_input).resolve()
    resolved_path_str = str(resolved_path)

    db_folder = resolved_path.parent
    try:
        db_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured database directory exists: {db_folder}")
    except Exception as e:
        st.warning(f"Could not create directory {db_folder}: {e}. Check permissions.")
        # Proceed anyway, connection might still work if dir exists

    db_manager = DatabaseManager(resolved_path_str)
    if not db_manager.connect():
        # Error is logged within db_manager.connect()
        st.error(f"Failed to connect to database: {db_manager.db_path.name}")
        # db_manager.close() # Ensure closed on failure
        return None # Return None to signal failure

    logger.info(f"DB Manager connection successful for: {db_manager.db_path}")
    # No need to manually close here if using cache_resource properly,
    # but ensure __exit__ in DatabaseManager handles cleanup if needed.
    return db_manager


def generate_timestamped_filename(original_name):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    base, ext = Path(original_name).stem, Path(original_name).suffix
    safe_base = re.sub(r'[^\w\.-]', '_', base) # Allow alphanumeric, underscore, dot, hyphen
    return f"{safe_base}_{now}{ext}"

# --- UI Components ---

def show_db_config_section():
    """UI for Database Configuration."""
    with st.sidebar.expander("⚙️ Database Settings", expanded=True):
        default_db_path = st.session_state.get('db_path_input', "data/db/importer_pro.db")
        db_path_input = st.text_input(
            "Database File Path:",
            value=default_db_path,
            key="db_path_widget",
            help="Path to the SQLite database file (e.g., data/my_imports.db). Will be created if it doesn't exist."
        )
        st.session_state['db_path_input'] = db_path_input

        db_manager = None
        if db_path_input:
            db_manager = get_db_manager(db_path_input) # This uses the cached resource
        else:
            st.info("Enter a database path to connect.")

        if db_manager and db_manager.connection:
             st.success(f"Connected: `{db_manager.db_path.name}`")
             try:
                 cursor = db_manager.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
                 if cursor:
                      tables = cursor.fetchall()
                      if tables:
                           st.markdown("**Existing Tables:**")
                           st.markdown('\n'.join([f"- `{t[0]}`" for t in tables]))
                      else:
                           st.caption("_No user tables found._")
                 else:
                      st.warning("Could not retrieve table list (cursor error).")
             except Exception as e:
                  st.warning(f"Could not list tables: {e}")
                  logger.exception("Error fetching table list:")

             # --- Add Download Button for the Database File ---
             db_file_to_download = Path(db_manager.db_path).resolve() # Get path from the connected manager
             if db_file_to_download.exists() and db_file_to_download.is_file():
                 try:
                     # Ensure the DatabaseManager's connection is closed before reading,
                     # to flush all data to disk. The cache_resource should handle this on session end,
                     # but for an explicit download, this can be tricky if the file is actively used.
                     # A simple read might be okay if commits are frequent and WAL mode is not an issue.
                     # Forcing a close on the cached resource is not straightforward from here.
                     # Best effort: Read the file as is. Most recent commits should be there.
                     with open(db_file_to_download, "rb") as fp:
                         st.download_button(
                             label="💾 Download Database File",
                             data=fp, # Pass the file object
                             file_name=db_file_to_download.name,
                             mime="application/vnd.sqlite3" # Standard MIME for SQLite
                         )
                 except Exception as e:
                     st.error(f"Error preparing DB for download: {e}")
                     logger.error(f"Error reading database file for download {db_file_to_download}: {e}")
             else:
                 st.caption("Database file does not exist yet. Import data to create it.")

        elif db_path_input:
            st.error("Connection Failed. Check path and permissions.")

    return db_manager

def show_upload_section():
    """UI for File Upload."""
    with st.container(border=True):
        st.subheader("1. Upload Data File", divider="rainbow")
        supported_types_str = ", ".join(f"`{ext}`" for ext in AVAILABLE_IMPORTERS.keys()) # Dynamic based on registry
        st.markdown(f"Select the file you want to import. Supported types: {supported_types_str}")

        uploaded_file = st.file_uploader(
            "Choose a file",
            type=[ext.lstrip('.') for ext in AVAILABLE_IMPORTERS.keys()], # Dynamic list of extensions
            label_visibility="collapsed",
            key="file_uploader_widget" # Add key for stability
        )

        # --- Manage File State ---
        if uploaded_file is not None:
            # If a new file is uploaded OR it's different from the one in state
            if 'uploaded_file_info' not in st.session_state or st.session_state.uploaded_file_info['name'] != uploaded_file.name:
                st.session_state.uploaded_file_info = {
                    'name': uploaded_file.name,
                    'size': uploaded_file.size,
                    'type': uploaded_file.type,
                    # Storing the raw bytes is safer than the uploader object across reruns
                    'content': uploaded_file.getvalue()
                }
                # Clear dependent state if file changes
                st.session_state.pop('temp_file_path', None)
                # Clear cached headers specific to the old file
                st.session_state.pop(f'headers_{st.session_state.get("uploaded_file_info",{}).get("name","old")}', None)
                st.session_state.pop('column_mapping_state', None)
                st.session_state.pop('target_table_name_input', None) # Reset table name suggestion
                st.session_state.pop('last_import_results', None) # Clear previous results
                logger.info(f"New file uploaded: {uploaded_file.name}. Cleared dependent state.")
                st.success(f"File `{uploaded_file.name}` selected ({uploaded_file.size} bytes).")
        # If uploader becomes None (file removed), clear the state
        elif uploaded_file is None and 'uploaded_file_info' in st.session_state:
             logger.info("Uploaded file removed by user. Clearing state.")
             old_file_name = st.session_state.get('uploaded_file_info',{}).get('name')
             st.session_state.pop('uploaded_file_info', None)
             st.session_state.pop('temp_file_path', None)
             st.session_state.pop(f'headers_{old_file_name}', None) # Clear cached headers for the removed file
             st.session_state.pop('column_mapping_state', None)
             st.session_state.pop('last_import_results', None)


    # Return the file info dictionary from state
    return st.session_state.get('uploaded_file_info')


# --- Modified Temp File Handling ---
# Cache the creation of the temp file path based on file content hash
@st.cache_data(show_spinner=False) # Use cache_data for file content
def get_temporary_filepath(_file_content_bytes: bytes, original_filename: str) -> Optional[Path]:
    """Saves file content to a temporary file and returns the path. Cached."""
    try:
        # Use a hash of content for uniqueness? Or timestamp? Timestamp is simpler.
        temp_file_name = generate_timestamped_filename(original_filename)
        temp_path = (TEMP_DIR / temp_file_name).resolve()

        with open(temp_path, "wb") as f:
            f.write(_file_content_bytes)
        logger.info(f"File content cached to temporary path: {temp_path}")
        return temp_path
    except Exception as e:
        st.error(f"Failed to create temporary file: {e}")
        logger.exception("Error creating temporary file:")
        return None

# --- Header Caching ---
# Use st.cache_data to cache headers based on file path
@st.cache_data(show_spinner=False)
def get_cached_headers(_importer: BaseImporter, _file_path: Path) -> List[str]:
    """Gets headers using the importer, cached based on file path."""
    logger.info(f"Cache miss or first call: Reading headers for {_file_path.name} using {_importer.__class__.__name__}")
    try:
        headers = _importer.get_headers(_file_path)
        logger.info(f"Successfully read headers for {_file_path.name}: {headers}")
        return headers
    except Exception as e:
        st.error(f"Could not read headers from file: {e}")
        logger.exception(f"Error reading file headers in get_cached_headers for {_file_path}:")
        # Return empty list on error to prevent breaking downstream UI, error is shown
        return []


def show_config_import_section(db_manager: DatabaseManager, importer: BaseImporter, file_path: Path):
    """UI for Table Name and Column Mapping."""
    with st.container(border=True):
        st.subheader("2. Configure Import Target", divider="rainbow")

        # --- Table Name ---
        st.markdown("**Database Table Name**")
        st.caption("Choose a name for the table where data will be imported. It will be created if it doesn't exist.")
        # Suggest name based on file, fallback if needed
        default_table_name = sanitize_name(file_path.stem) or "imported_data"
        # Use session state to remember table name input across reruns for the current file
        table_name_input = st.text_input(
            "Table Name:",
            value=st.session_state.get('target_table_name_input', default_table_name),
            key="table_name_widget"
        )
        # Update session state ONLY if the input changes
        if table_name_input != st.session_state.get('target_table_name_input', default_table_name):
            st.session_state['target_table_name_input'] = table_name_input
            logger.debug(f"Table name input changed to: {table_name_input}")

        final_table_name = sanitize_name(table_name_input)

        if not final_table_name:
            st.error("Table name is required and cannot be empty after sanitization.")
            return None, None, None

        if final_table_name != table_name_input:
            st.info(f"Using sanitized table name: `{final_table_name}`")

        st.divider()

        # --- Column Mapping ---
        st.markdown("**Map Source Columns to Database Fields**")
        st.caption("Select columns from your file to import. Edit the 'Database Field Name' if needed (use letters, numbers, underscores).")

        # Get headers from importer
        try:
            headers = importer.get_headers(file_path)
            if not headers:
                st.error("No headers found in the file.")
                return None, None, None
        except Exception as e:
            st.error(f"Error reading headers: {e}")
            logger.exception("Error reading headers:")
            return None, None, None

        # Create a grid layout for column mapping
        cols_per_row = 3
        grid_cols = st.columns(cols_per_row)
        col_idx = 0
        validation_issues = False
        final_mapping = {}
        schema_definition = {}
        db_field_names_used = set()

        # Reset mapping state if headers changed
        if headers != st.session_state.get('last_headers', []):
            logger.info(f"Headers changed or first load for {file_path.name}. Resetting column mapping state.")
            st.session_state['last_headers'] = headers
            st.session_state['column_mapping_state'] = {}

        # Get current mapping state
        current_mapping_state = st.session_state.get('column_mapping_state', {})

        for i, header in enumerate(headers):
            # Generate default sanitized name
            default_sanitized_name = sanitize_name(header) or f"col_{i}"

            # Get state for this header
            header_state = current_mapping_state.get(header, {
                'include': True,
                'db_name': default_sanitized_name
            })

            container = grid_cols[col_idx % cols_per_row].container(border=True)
            include_col = container.checkbox(
                f"`{header}`",
                value=header_state['include'],
                key=f"include_{file_path.name}_{header}_{i}"
            )
            db_field_name_input = container.text_input(
                "DB Field:",
                value=header_state['db_name'],
                key=f"dbname_{file_path.name}_{header}_{i}",
                label_visibility="collapsed",
                disabled=not include_col
            )

            # Store current state
            current_mapping_state[header] = {'include': include_col, 'db_name': db_field_name_input}

            # Process mapping and schema if column is included
            if include_col:
                sanitized_db_field_name = sanitize_name(db_field_name_input)
                if not sanitized_db_field_name:
                    container.error("Invalid database field name.")
                    validation_issues = True
                elif sanitized_db_field_name in db_field_names_used:
                    container.error("Duplicate database field name.")
                    validation_issues = True
                else:
                    db_field_names_used.add(sanitized_db_field_name)
                    final_mapping[sanitized_db_field_name] = header

                    # Define schema based on data type inference
                    field_type = "TEXT"  # Default
                    lower_db_name = sanitized_db_field_name.lower()
                    lower_header = header.lower()

                    # Type inference with improved field detection
                    if any(kw in lower_db_name or kw in lower_header for kw in ['email', 'mail']):
                        field_type = "TEXT UNIQUE"
                    elif any(kw in lower_db_name or kw in lower_header for kw in ['phone', 'tel', 'mobile', 'cell']):
                        field_type = "TEXT"  # Phone numbers should be TEXT
                    elif any(kw in lower_db_name or kw in lower_header for kw in ['amount', 'price', 'salary', 'value', 'count', 'quantity', 'number', 'num', 'id']):
                        field_type = "REAL"
                    elif any(kw in lower_db_name or kw in lower_header for kw in ['date', 'time', 'joined', 'created', 'updated', 'dob', 'birth']):
                        field_type = "DATETIME"

                    schema_definition[sanitized_db_field_name] = field_type
                    container.caption(f"Type: `{field_type}`")

            col_idx += 1

        # Update session state
        st.session_state.column_mapping_state = current_mapping_state

        if not final_mapping and headers:
            st.warning("No columns selected for import.")
            validation_issues = True

    # Return valid config if no issues
    if not validation_issues:
        return final_table_name, final_mapping, schema_definition
    else:
        return final_table_name, None, None


def show_preview_section(importer: BaseImporter, file_path: Path, final_mapping: Mapping[str, str]):
    """UI for Data Preview."""
    # Use an expander for the preview
    with st.expander("📊 Data Preview (First 5 Rows)", expanded=False):
        if not final_mapping:
            st.info("Configure column mapping above to see a preview.")
            return

        st.caption("Showing a preview of the first 5 data rows based on your mapping (column names reflect target DB fields).")
        try:
            # Use importer's preview method
            df_preview = importer.get_preview(file_path, num_rows=5)

            if df_preview.empty:
                st.write("Preview unavailable (file might be empty or have only headers).")
                return

            # Filter and rename based on the final_mapping
            preview_data = {}
            mapped_csv_headers_in_preview = [h for h in final_mapping.values() if h in df_preview.columns]

            if not mapped_csv_headers_in_preview:
                st.warning("None of the mapped source columns were found in the preview data.")
                # Display raw preview if mapping fails?
                st.dataframe(df_preview, use_container_width=True, height=210)
                return

            # Create the preview dict with target DB names as keys
            for db_field, source_header in final_mapping.items():
                if source_header in df_preview.columns:
                    preview_data[db_field] = df_preview[source_header]
                # Optionally add placeholder for mapped columns not in preview sample?
                # else: preview_data[db_field] = [""] * len(df_preview) # Or similar

            df_preview_final = pd.DataFrame(preview_data)
            st.dataframe(df_preview_final, use_container_width=True, height=210) # Adjust height as needed

        except Exception as e:
            st.error(f"Could not generate preview: {e}")
            logger.exception("Error during preview generation:")


def show_import_action_section(db_manager: DatabaseManager, importer: BaseImporter, file_path: Path, table_name: str, mapping: Mapping[str, str], schema: Dict[str, str]):
    """UI for triggering the import."""
    with st.container(border=True):
        st.subheader("3. Execute Import", divider="rainbow")
        # Check if all components are ready
        can_import = bool(db_manager and importer and file_path and table_name and mapping and schema)

        if not can_import:
             st.warning("Please complete all configuration steps above (Database, Upload, Mapping) before importing.")
             # Disable button implicitly via check below
        else:
             st.markdown(f"Ready to import data from `{file_path.name}` into table `{table_name}`.")

        # Add a unique key to the button including file and table name to reset on change
        import_button_key = f"import_button_{file_path.name}_{table_name}"
        if st.button(f"🚀 Import to Table '{table_name}'", type="primary", disabled=not can_import, use_container_width=True, key=import_button_key):
            results = None # Initialize results variable
            with st.spinner(f"Importing data into '{table_name}'... Please wait."):
                 # 1. Ensure table exists
                 table_ok = False
                 try:
                     logger.info(f"Ensuring table '{table_name}' exists with schema: {schema}")
                     # Pass the derived schema to the DB manager
                     table_ok = db_manager.create_dynamic_table(table_name, schema)
                     if table_ok:
                          logger.info(f"Table '{table_name}' created or verified successfully.")
                     else:
                          st.error(f"Failed to create or verify table '{table_name}'. Import cancelled. Check logs.")
                          logger.error(f"create_dynamic_table returned False for table {table_name}")
                          st.session_state['last_import_results'] = {'errors': [{'error': f'Table creation/verification failed for {table_name}.', 'row':'N/A', 'data':''}]}

                 except Exception as e:
                      st.error(f"Error preparing table '{table_name}': {e}")
                      logger.exception("Error during create_dynamic_table in import action:")
                      st.session_state['last_import_results'] = {'errors': [{'error': f'Error preparing table {table_name}: {e}', 'row':'N/A', 'data':''}]}

                 # 2. Run import only if table is ready
                 if table_ok:
                      logger.info(f"Table '{table_name}' OK, proceeding with import.")
                      try:
                           # Prepare schema_info for validation (extract required/unique fields)
                           # Example: Mark email as unique if present
                           unique_fields = [k for k, v in schema.items() if 'UNIQUE' in v.upper()]
                           # Example: Could add logic here to determine required fields if needed
                           required_fields = [] # Currently no UI to set required fields
                           schema_info_for_validation = {'required': required_fields, 'unique': unique_fields}
                           logger.debug(f"Schema info for validation: {schema_info_for_validation}")

                           # Call the main processing method of the importer
                           import_result_obj: ImportResult = importer.process_import(
                               file_path, table_name, mapping, schema_info_for_validation
                           )
                           results_dict = import_result_obj.to_dict() # Convert result object to dict for display/storage
                           st.session_state['last_import_results'] = results_dict # Store results dict
                           logger.info(f"Import process finished for table '{table_name}'. Results: {results_dict}")
                           st.success(f"Import process finished for table '{table_name}'.")
                      except Exception as e:
                           st.error(f"Import failed: {e}")
                           logger.exception(f"Error during importer.process_import for table {table_name}")
                           # Store error state clearly
                           st.session_state['last_import_results'] = {
                               'total': 0, 'inserted': 0, 'skipped': 'N/A', # Indicate failure state
                               'errors': [{'row': 'Critical', 'error': f'Import Process Failed: {e}', 'data': 'N/A'}]
                           }

            # Rerun to display results immediately after button press & processing finishes
            # This ensures the results section updates right away
            st.rerun()


def show_results_section():
    """Displays results from the last import operation stored in session state."""
    results = st.session_state.get('last_import_results')
    if results:
         # Use a container for better layout control
         with st.container(border=True):
            st.subheader("📊 Last Import Results", divider="rainbow")
            # Ensure keys exist before accessing
            total = results.get('total', 0)
            inserted = results.get('inserted', 0)
            skipped = results.get('skipped', 0) # Default to 0 if key missing

            col1, col2, col3 = st.columns(3)
            col1.metric("Rows Processed", total) # Changed label slightly for clarity
            col2.metric("Rows Inserted", inserted)
            # Display skipped count - use inverse delta for visual cue of issues
            col3.metric("Rows Skipped / Errors", skipped, delta=f"-{skipped}" if skipped > 0 else "0", delta_color="inverse")

            errors = results.get('errors', [])
            if errors:
                 # Use an expander that's initially expanded if there are errors
                 with st.expander("⚠️ View Errors / Skipped Row Details", expanded=True):
                      # Convert errors list of dicts to DataFrame for better display
                      try:
                           err_df = pd.DataFrame(errors)
                           # Ensure standard columns exist, even if empty
                           if 'row' not in err_df.columns: err_df['row'] = 'N/A'
                           if 'error' not in err_df.columns: err_df['error'] = 'Unknown Error'
                           if 'data' not in err_df.columns: err_df['data'] = '{}'
                           # Reorder columns for clarity
                           err_df = err_df[['row', 'error', 'data']]
                           st.dataframe(err_df, use_container_width=True, height=200) # Limit height initially

                           # --- Download Button for Errors ---
                           csv_error_bytes = err_df.to_csv(index=False).encode('utf-8')
                           st.download_button(
                               label="💾 Download Error Report (.csv)",
                               data=csv_error_bytes,
                               file_name="import_error_report.csv",
                               mime="text/csv",
                               key="download_errors_button" # Add a key
                           )
                      except Exception as e:
                           st.error(f"Could not display or prepare error report: {e}")
                           logger.exception("Error processing errors for display/download:")
            elif inserted > 0: # If no errors but rows were inserted
                st.info("No errors reported during the last import.")

# --- Main Application Flow ---
def main():
    st.title("🚀 Data Importer Pro")
    st.caption("Upload, map, and import data from files (CSV, JSON) into your SQLite database.") # Updated caption

    # --- Sidebar ---
    db_manager = show_db_config_section()

    # --- Main Area ---
    uploaded_file_info = show_upload_section() # Returns dict from state or None

    importer: Optional[BaseImporter] = None
    temp_file_path: Optional[Path] = None

    # Proceed only if DB is connected and file state exists
    if db_manager and db_manager.connection and uploaded_file_info:
        st.divider()
        # Get temporary file path using cached function based on content
        file_content = uploaded_file_info.get('content')
        file_name = uploaded_file_info.get('name')
        if file_content and file_name:
             temp_file_path = get_temporary_filepath(file_content, file_name)
        else:
            # This case might happen on refresh if state handling isn't perfect
            logger.warning("File content or name missing from session state. Requesting re-upload.")
            st.warning("File information missing. Please re-upload the file.")
            # Clear potentially inconsistent state
            st.session_state.pop('uploaded_file_info', None)
            st.stop() # Stop execution if file content isn't available

        if temp_file_path and temp_file_path.exists():
             # Get importer instance AFTER temp file is ready
             importer = get_importer_for_file(temp_file_path, db_manager)

             if importer:
                  # Show config section (mapping, table name)
                  # Returns table_name, mapping, schema - mapping/schema might be None if invalid
                  table_name, mapping, schema = show_config_import_section(db_manager, importer, temp_file_path)

                  # Only proceed if config is valid
                  if table_name and mapping and schema:
                       # Show preview if mapping is valid
                       show_preview_section(importer, temp_file_path, mapping)
                       st.divider()
                       # Show import button/action area
                       show_import_action_section(db_manager, importer, temp_file_path, table_name, mapping, schema)
                  elif table_name: # Config section shown, but mapping/schema invalid
                       # Warning about invalid config is shown within show_config_import_section
                       pass # Don't show preview or import sections
             else:
                  # Error shown by get_importer_for_file
                  pass
        elif temp_file_path is None:
             # Error shown by get_temporary_filepath if it failed
             st.error("Failed to process uploaded file. Please try again.")
        else: # Path returned but doesn't exist (cache issue?)
             st.error("Temporary file path is invalid. Please re-upload the file.")
             logger.error(f"Temporary file path {temp_file_path} does not exist.")


    elif db_manager and not uploaded_file_info:
         st.info("Upload a data file (CSV, JSON) to begin the import process.") # Updated info text
    elif not db_manager:
        # Info/Error about DB connection shown in sidebar
         pass


    # Always show results area if results exist in state, regardless of other states
    show_results_section()


# --- Run ---
if __name__ == "__main__":
    main()
