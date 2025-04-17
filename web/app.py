# web/app.py
import sys
import io
import csv
import re
import logging
from pathlib import Path
import datetime
from typing import Dict, Type, Optional, Mapping

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
import pandas as pd

# Core components
from core.database import DatabaseManager
# Importer components
from core.importers.base_importer import BaseImporter, ImportResult
from core.importers.csv_importer import CSVImporter
# Future importers: from core.importers.json_importer import JSONImporter

# --- Page Configuration ---
st.set_page_config(
    page_title="Data Importer Pro",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Importer Factory ---
# Register available importers here
IMPORTER_REGISTRY: Dict[str, Type[BaseImporter]] = {
    '.csv': CSVImporter,
    # '.json': JSONImporter, # Example for future
    # '.xlsx': ExcelImporter, # Example for future
}

def get_importer_for_file(file_path: Path, db_manager: DatabaseManager) -> Optional[BaseImporter]:
    """Factory function to get an importer instance based on file extension."""
    extension = file_path.suffix.lower()
    importer_class = IMPORTER_REGISTRY.get(extension)
    if importer_class:
        logging.info(f"Found importer {importer_class.__name__} for extension '{extension}'")
        try:
            return importer_class(db_manager)
        except Exception as e:
            logging.exception(f"Failed to instantiate importer {importer_class.__name__}")
            st.error(f"Error initializing importer for {extension} files: {e}")
            return None
    else:
        st.error(f"Unsupported file type: '{extension}'. No importer found.")
        logging.warning(f"No importer registered for file extension: {extension}")
        return None

# --- Helper Functions ---
def sanitize_name(name):
    if not isinstance(name, str): name = str(name)
    name = re.sub(r'[^\w_]', '_', name)
    if name and name[0].isdigit(): name = "_" + name
    if not name: return None
    return name.lower()

@st.cache_resource # Cache the DB manager per session based on path
def get_db_manager(db_path_input) -> Optional[DatabaseManager]:
    """Gets or initializes the DatabaseManager, cached."""
    logging.info(f"Requesting DB Manager for path: {db_path_input}")
    resolved_path_str = str(Path(db_path_input).resolve()) # Use resolved path as cache key part

    db_folder = Path(resolved_path_str).parent
    try:
        db_folder.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        st.warning(f"Could not create directory {db_folder}: {e}. Check permissions.")

    db_manager = DatabaseManager(resolved_path_str)
    if not db_manager.connect():
        st.error(f"Failed to connect to database: {db_manager.db_path}")
        return None
    logging.info(f"DB Manager connection successful for: {db_manager.db_path}")
    return db_manager

def generate_timestamped_filename(original_name):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    base, ext = Path(original_name).stem, Path(original_name).suffix
    safe_base = re.sub(r'[^\w\.-]', '_', base)
    return f"{safe_base}_{now}{ext}"

# --- UI Components ---

def show_db_config_section():
    """UI for Database Configuration."""
    with st.sidebar.expander("⚙️ Database Settings", expanded=True):
        default_db_path = st.session_state.get('db_path_input', "data/importer_data.db")
        db_path_input = st.text_input(
            "Database File:",
            value=default_db_path,
            key="db_path_widget",
            help="Path to the SQLite database file (e.g., data/my_imports.db)"
        )
        st.session_state['db_path_input'] = db_path_input # Store user input for next run

        db_manager = get_db_manager(db_path_input)

        if db_manager and db_manager.connection:
             st.success(f"Connected: `{db_manager.db_path.name}`")
             # Display Tables
             try:
                 cursor = db_manager.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
                 if cursor:
                      tables = cursor.fetchall()
                      if tables:
                           st.markdown("**Existing Tables:**")
                           st.markdown('\n'.join([f"- `{t[0]}`" for t in tables]))
                      else:
                           st.caption("_No user tables found._")
             except Exception as e:
                  st.warning(f"Could not list tables: {e}")
        elif db_path_input: # Only show error if user actually entered something
            st.error("Connection Failed.")

    return db_manager # Return the manager instance (or None)

def show_upload_section():
    """UI for File Upload."""
    with st.container(border=True):
        st.subheader("1. Upload Data File", divider="rainbow")
        st.markdown("Select the file you want to import. Supported types: " + ", ".join(f"`{ext}`" for ext in IMPORTER_REGISTRY.keys()))

        uploaded_file = st.file_uploader(
            "Choose a file",
            type=[ext.lstrip('.') for ext in IMPORTER_REGISTRY.keys()], # Get types from registry
            label_visibility="collapsed"
        )
        # Store uploaded file info in session state immediately
        if uploaded_file:
             st.session_state['uploaded_file_info'] = {
                 'name': uploaded_file.name,
                 'size': uploaded_file.size,
                 'type': uploaded_file.type,
                 'obj': uploaded_file # Store the object itself (careful with size)
             }
             st.success(f"File `{uploaded_file.name}` uploaded ({uploaded_file.size} bytes).")
        elif 'uploaded_file_info' in st.session_state:
             # If file deselected, clear state
             del st.session_state['uploaded_file_info']

    return uploaded_file # Return the object only if newly uploaded this run

def show_config_import_section(db_manager: DatabaseManager, importer: BaseImporter, file_path: Path):
    """UI for Table Name and Column Mapping."""
    with st.container(border=True):
        st.subheader("2. Configure Import Target", divider="rainbow")

        # --- Table Name ---
        st.markdown("**Database Table Name**")
        st.caption("Choose a name for the table where data will be imported. It will be created if it doesn't exist.")
        default_table_name = sanitize_name(file_path.stem) or "imported_data"
        # Use session state to remember table name input
        table_name_input = st.text_input(
            "Table Name:",
            value=st.session_state.get('target_table_name_input', default_table_name),
            key="table_name_widget"
            )
        st.session_state['target_table_name_input'] = table_name_input # Store input
        final_table_name = sanitize_name(table_name_input)

        if not final_table_name:
             st.error("Table name is required and cannot be empty after sanitization.")
             return None, None, None # Indicate failure

        if final_table_name != table_name_input:
            st.info(f"Using sanitized table name: `{final_table_name}`")

        st.divider()

        # --- Column Mapping ---
        st.markdown("**Map Source Columns to Database Fields**")
        st.caption("Select columns from your file to import. Edit the 'Database Field Name' if needed (use letters, numbers, underscores).")

        try:
             headers = st.session_state.get('csv_headers')
             if not headers: # Read headers if not in state
                  headers = importer.get_headers(file_path)
                  st.session_state['csv_headers'] = headers # Cache headers
        except Exception as e:
            st.error(f"Could not read headers from file: {e}")
            logging.exception("Error reading file headers in mapping section:")
            return None, None, None # Indicate failure

        # Use columns for layout
        num_csv_cols = len(headers)
        cols_per_row = min(num_csv_cols, 4) # Max 4 side-by-side
        grid_cols = st.columns(cols_per_row)

        final_mapping = {} # {db_field: csv_header}
        schema_definition = {} # {db_field: type_string}
        db_field_names_used = set() # Track for duplicates
        validation_issues = False

        # Load previous mapping state if available
        previous_mapping_state = st.session_state.get('column_mapping_state', {})

        col_idx = 0
        for i, header in enumerate(headers):
            container = grid_cols[col_idx % cols_per_row].container(border=True)
            # Retrieve previous state for this header
            prev_state = previous_mapping_state.get(header, {'include': True, 'db_name': sanitize_name(header) or f"col_{i}"})

            include_col = container.checkbox(f"`{header}`", value=prev_state['include'], key=f"include_{header}_{i}")
            db_field_name_input = container.text_input(
                "DB Field:",
                value=prev_state['db_name'],
                key=f"dbname_{header}_{i}",
                label_visibility="collapsed"
            )

            # Store current state
            st.session_state.setdefault('column_mapping_state', {})[header] = {'include': include_col, 'db_name': db_field_name_input}

            if include_col:
                 sanitized_db_field_name = sanitize_name(db_field_name_input)

                 if not sanitized_db_field_name:
                      container.error("⚠️ Invalid Name")
                      validation_issues = True
                 elif sanitized_db_field_name in db_field_names_used:
                      container.warning("⚠️ Duplicate Name")
                      validation_issues = True # Treat as error for now
                 else:
                      db_field_names_used.add(sanitized_db_field_name)
                      final_mapping[sanitized_db_field_name] = header
                      # Define schema (simple heuristics, can be improved)
                      field_type = "TEXT"
                      lower_db_name = sanitized_db_field_name.lower()
                      if "email" in lower_db_name: field_type = "TEXT UNIQUE"
                      # Add more type/constraint heuristics here if desired
                      schema_definition[sanitized_db_field_name] = field_type
                      # container.caption(f"Type: `{field_type}`") # Optional: Show inferred type

            col_idx += 1

        if not final_mapping and headers: # Check headers exist to avoid warning on initial load
             st.warning("No columns selected for import.")
             validation_issues = True

    return final_table_name, final_mapping, schema_definition if not validation_issues else None


def show_preview_section(importer: BaseImporter, file_path: Path, final_mapping: Mapping[str, str]):
    """UI for Data Preview."""
    with st.expander("📊 Data Preview (First 5 Rows)", expanded=False):
        if not final_mapping:
            st.info("Configure column mapping above to see a preview.")
            return

        st.caption("Showing a preview of how the first 5 rows will look based on your mapping.")
        try:
            # Use importer's preview method first
            df_preview = importer.get_preview(file_path, num_rows=5)

            # Select only the columns included in the mapping (based on CSV header)
            mapped_csv_headers = list(final_mapping.values())
            preview_cols_to_show = [col for col in df_preview.columns if col in mapped_csv_headers]
            df_preview_mapped = df_preview[preview_cols_to_show]

            # Rename columns to target DB field names for display
            rename_dict = {csv_col: db_field for db_field, csv_col in final_mapping.items() if csv_col in df_preview_mapped.columns}
            df_preview_mapped.rename(columns=rename_dict, inplace=True)

            st.dataframe(df_preview_mapped, use_container_width=True)
        except Exception as e:
            st.error(f"Could not generate preview: {e}")
            logging.exception("Error during preview generation:")

def show_import_action_section(db_manager: DatabaseManager, importer: BaseImporter, file_path: Path, table_name: str, mapping: Mapping[str, str], schema: Dict[str, str]):
    """UI for triggering the import."""
    with st.container(border=True):
        st.subheader("3. Execute Import", divider="rainbow")
        can_import = table_name and mapping and schema and db_manager and importer and file_path

        if not can_import:
             st.warning("Please complete configuration steps above before importing.")
             return None # No results

        st.markdown(f"Ready to import data from `{file_path.name}` into table `{table_name}` in database `{db_manager.db_path.name}`.")

        if st.button(f"🚀 Import to Table '{table_name}'", type="primary", disabled=not can_import, use_container_width=True):
            results = None
            with st.spinner(f"Importing data into '{table_name}'... Please wait."):
                 # 1. Ensure table exists
                 table_ok = False
                 try:
                     logging.info(f"Ensuring table '{table_name}' exists with schema: {schema}")
                     table_ok = db_manager.create_dynamic_table(table_name, schema)
                     if not table_ok:
                          st.error(f"Failed to create or verify table '{table_name}'. Check logs.")
                 except Exception as e:
                      st.error(f"Error preparing table '{table_name}': {e}")
                      logging.exception("Error during create_dynamic_table in import action:")

                 # 2. Run import if table is ready
                 if table_ok:
                      try:
                           # The process_import method now orchestrates read, map, validate, insert
                           schema_info = {'required': [], 'unique': [k for k,v in schema.items() if 'UNIQUE' in v.upper()]} # Pass basic schema info for validation
                           import_result_obj: ImportResult = importer.process_import(file_path, table_name, mapping, schema_info)
                           results = import_result_obj.to_dict() # Convert result object to dict for display
                           st.session_state['last_import_results'] = results # Store results
                           st.success(f"Import process finished for table '{table_name}'.")
                      except Exception as e:
                           st.error(f"Import failed: {e}")
                           logging.exception(f"Error during importer.process_import for table {table_name}")
                           st.session_state['last_import_results'] = {'errors': [{'error': f'Critical Import Failure: {e}', 'row':'N/A', 'data':''}]} # Store error state
                 else:
                    st.session_state['last_import_results'] = {'errors': [{'error': 'Table creation/verification failed.', 'row':'N/A', 'data':''}]} # Store error state

            # Rerun to display results immediately after button press finishes
            st.rerun()

def show_results_section():
    """Displays results from the last import operation stored in session state."""
    results = st.session_state.get('last_import_results')
    if results:
         st.subheader("📊 Last Import Results", divider="rainbow")
         col1, col2, col3 = st.columns(3)
         col1.metric("CSV Rows Processed", results.get('total', 0))
         col2.metric("Rows Inserted", results.get('inserted', 0))
         col3.metric("Rows Skipped / Errors", results.get('skipped', 0), delta_color="inverse")

         errors = results.get('errors', [])
         if errors:
              with st.expander("⚠️ View Errors / Skipped Row Details", expanded=True):
                   st.dataframe(pd.DataFrame(errors), use_container_width=True)
                   # Add download button
                   try:
                        err_df = pd.DataFrame(errors)
                        csv_error = err_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="💾 Download Error Report (.csv)",
                            data=csv_error,
                            file_name="import_error_report.csv",
                            mime="text/csv",
                            key="download_errors_btn"
                        )
                   except Exception as e:
                        st.error(f"Could not generate error report: {e}")

# --- Main Application Flow ---
def main():
    st.title("🚀 Data Importer Pro")
    st.caption("Upload, map, and import data from files into your SQLite database.")

    # --- Sidebar ---
    db_manager = show_db_config_section()

    # --- Main Area ---
    uploaded_file_obj = show_upload_section() # Gets file only on new upload

    # Use file info from session state if available
    uploaded_file_info = st.session_state.get('uploaded_file_info')
    importer: Optional[BaseImporter] = None
    temp_file_path: Optional[Path] = None

    # Proceed only if DB is connected and file is uploaded (check state)
    if db_manager and uploaded_file_info:
        st.divider()
        # Save to temp path ONCE per uploaded file if needed by importer
        if 'temp_file_path' not in st.session_state or st.session_state.get('current_file_name') != uploaded_file_info['name']:
             temp_dir = Path("./temp_uploads")
             temp_dir.mkdir(exist_ok=True)
             temp_file_name = generate_timestamped_filename(uploaded_file_info['name'])
             temp_path = temp_dir / temp_file_name
             try:
                  with open(temp_path, "wb") as f:
                       # Get file content correctly - use stored obj if new, else need to handle differently
                       # This part is tricky with Streamlit's file handling on rerun
                       # Best practice is often to process immediately or store data differently
                       # For now, assuming importer can handle path (re-read needed)
                       # Let's re-save it each time for simplicity here, though inefficient
                       if uploaded_file_obj: # If just uploaded
                           f.write(uploaded_file_obj.getbuffer())
                       else: # Need to access the object stored in state IF IT'S STILL VALID
                           # This is complex - better approach needed for large files / long sessions
                           # Assuming for now the user triggers import shortly after upload.
                           # A robust solution might use st.cache_data on the processing.
                           # Let's display a warning if the object isn't fresh.
                           st.warning("Re-processing file. Consider re-uploading if session was long.", icon="⚠️")
                           file_obj_from_state = uploaded_file_info.get('obj')
                           if file_obj_from_state:
                               file_obj_from_state.seek(0) # Reset pointer
                               f.write(file_obj_from_state.getbuffer())
                           else:
                               st.error("Cannot access file data. Please re-upload.")
                               st.stop()

                  st.session_state['temp_file_path'] = temp_path
                  st.session_state['current_file_name'] = uploaded_file_info['name']
                  logging.info(f"File ready at temp path: {temp_path}")
                  temp_file_path = temp_path
             except Exception as e:
                  st.error(f"Failed to prepare temporary file: {e}")
                  logging.exception("Error saving uploaded file temporarily:")
                  st.stop()
        else:
             temp_file_path = st.session_state.get('temp_file_path') # Use existing temp path


        if temp_file_path and temp_file_path.exists():
             # Get importer instance AFTER temp file is ready
             importer = get_importer_for_file(temp_file_path, db_manager)

             if importer:
                  # Show config section (mapping, table name)
                  table_name, mapping, schema = show_config_import_section(db_manager, importer, temp_file_path)

                  if table_name and mapping and schema:
                       # Show preview if mapping is valid
                       show_preview_section(importer, temp_file_path, mapping)
                       st.divider()
                       # Show import button/action area
                       show_import_action_section(db_manager, importer, temp_file_path, table_name, mapping, schema)

    # Always show results area if results exist in state
    show_results_section()


# --- Run ---
if __name__ == "__main__":
    main()
    # Note: Temp file cleanup might ideally happen differently, e.g., on session end if possible.
    # Currently relies on reruns or app stop. Consider adding explicit cleanup if needed.
