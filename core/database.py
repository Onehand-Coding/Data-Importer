import re
import sqlite3
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# Configure logging for this module
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path_str: str):

        # Check for special in-memory SQLite URIs
        if db_path_str == ":memory:" or db_path_str.startswith("file::memory"): # More robust check
            self.db_path_is_memory = True
            self.db_path_for_connection = db_path_str
            self.db_path = Path(db_path_str) # Keep for display/logging if needed, but don't resolve for connection
            logger.info(f"DatabaseManager initialized for IN-MEMORY database: {self.db_path_for_connection}")
        else:
            self.db_path_is_memory = False
            # For regular files, resolve the path
            try:
                self.db_path = Path(db_path_str).resolve()
                self.db_path_for_connection = str(self.db_path)
                logger.info(f"DatabaseManager initialized for disk-based path: {self.db_path}")
            except Exception as e: # Handle potential errors if db_path_str is not a valid path string
                logger.error(f"Invalid file path string for DatabaseManager: {db_path_str} - {e}")
                raise ValueError(f"Invalid database file path: {db_path_str}") from e

        self.connection: Optional[sqlite3.Connection] = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self) -> bool:
        try:
            if not self.db_path_is_memory:
                # Ensure the parent directory for the database file exists only for disk-based DBs
                db_parent_dir = self.db_path.parent
                db_parent_dir.mkdir(parents=True, exist_ok=True)
                # logger.info(f"Ensured database directory exists for file-based DB: {db_parent_dir}") # Already logged in __init__

            # Use self.db_path_for_connection for the actual connection
            self.connection = sqlite3.connect(self.db_path_for_connection, timeout=10, check_same_thread=False)
            self.connection.execute("PRAGMA foreign_keys = ON;")
            logger.info(f"Successfully connected to database: {self.db_path_for_connection} (SQLite v{sqlite3.sqlite_version})")
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection error to {self.db_path_for_connection}: {e}", exc_info=True)
            self.connection = None
            return False
        except Exception as e: # Catch other potential errors during connect
            logger.error(f"Unexpected error connecting to {self.db_path_for_connection}: {e}", exc_info=True)
            self.connection = None
            return False

    def close(self):
        if self.connection:
            try:
                # Attempt to commit any lingering transaction before closing, then rollback if commit fails
                try:
                    # Check if still connected and in transaction before commit/rollback
                    if self.connection.in_transaction:
                        logger.debug("Attempting final commit before closing.")
                        self.connection.commit()
                except sqlite3.Error as commit_err:
                    logger.warning(f"Final commit failed before close (rolling back): {commit_err}")
                    try:
                        # Ensure rollback only happens if needed and connection is valid
                        if self.connection.in_transaction:
                             self.connection.rollback()
                    except Exception as rb_err:
                        logger.error(f"Rollback during close failed: {rb_err}")

                self.connection.close()
                logger.info(f"Database connection closed: {self.db_path}")
            except sqlite3.Error as e:
                logger.error(f"Error closing database connection {self.db_path}: {e}", exc_info=True)
            finally:
                 self.connection = None
        else:
            logger.debug("Attempted to close an already closed or non-existent database connection.")


    def execute(self, query: str, params: tuple = (), commit: bool = False) -> Optional[sqlite3.Cursor]:
        """
        Execute a SQL query.
        Re-raises IntegrityError for specific handling by caller.
        Handles other sqlite3 errors and performs rollback.
        """
        if not self.connection:
            logger.error("Database execute error: Not connected.")
            return None

        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            if commit:
                self.connection.commit()
            return cursor
        except sqlite3.IntegrityError as e: # <--- MODIFIED: Catch IntegrityError specifically
            # Don't log error here, let caller handle it if needed.
            # Rollback is important if commit=True was intended but failed mid-way
            # However, typical single-statement INSERT IntegrityError doesn't need rollback usually.
            # Let's re-raise it so the importer can format the error correctly.
            # Rollback might still be needed if part of a larger transaction elsewhere.
            logger.warning(f"Integrity constraint violation: {e}. Re-raising for caller.")
            raise e # <--- MODIFIED: Re-raise IntegrityError
        except sqlite3.Error as e: # Catch other SQLite errors
            logger.error(f"Database execution error: {e}\nQuery: {query}\nParams: {params}", exc_info=True)
            # Attempt Rollback on other SQLite errors
            if self.connection:
                try:
                    if self.connection.in_transaction:
                        logger.warning("Attempting transaction rollback due to caught database error.")
                        self.connection.rollback()
                        logger.info("Transaction rollback completed.")
                    else:
                         logger.debug("No active transaction to rollback.")
                except Exception as rb_err:
                    logger.error(f"Rollback attempt failed: {rb_err}", exc_info=True)
            else:
                 logger.error("Cannot rollback, connection is invalid.")
            return None # Indicate failure for general SQLite errors

    def sanitize_name(self, name):
        """Sanitizes a string to be a valid SQL table/column name."""
        if not isinstance(name, str): name = str(name)
        name = re.sub(r'[^\w_]', '_', name) # Allow letters, numbers, underscore
        if name and name[0].isdigit(): name = "_" + name # Prepend underscore if starts with digit
        if not name: return None # Return None if empty after sanitization
        return name.lower() # Convert to lowercase

    def create_dynamic_table(self, table_name: str, schema_definition: Dict[str, str]) -> bool:
        """Creates a table dynamically based on the provided schema."""
        logger.info(f"create_dynamic_table received call with table_name='{table_name}'")
        sanitized_table_name = self.sanitize_name(table_name)
        logger.info(f"Sanitized table name for creation: '{sanitized_table_name}'")

        if not sanitized_table_name:
            logger.error("Table creation failed: Invalid or empty table name provided after sanitization.")
            return False
        if not schema_definition:
            logger.error(f"Table creation failed for '{sanitized_table_name}': No columns defined in schema_definition.")
            return False

        column_defs = []
        defined_sanitized_cols = {self.sanitize_name(k) for k in schema_definition.keys()}
        if 'id' not in defined_sanitized_cols:
             column_defs.append('"id" INTEGER PRIMARY KEY AUTOINCREMENT')
             logger.info(f"Automatically adding 'id INTEGER PRIMARY KEY AUTOINCREMENT' to table '{sanitized_table_name}'.")

        for col_name, col_type in schema_definition.items():
            sanitized_col_name = self.sanitize_name(col_name)
            if not sanitized_col_name:
                logger.warning(f"Skipping invalid column name '{col_name}' during table creation for '{sanitized_table_name}'.")
                continue
            # Basic validation for column type string (prevent injection)
            # Allow spaces for things like PRIMARY KEY, NOT NULL etc. but be cautious
            validated_type = re.sub(r'[^\w\s\(\)_]', '', col_type).strip().upper()
            if not validated_type:
                logger.warning(f"Column type for '{sanitized_col_name}' was empty or invalid ('{col_type}'), defaulting to TEXT.")
                validated_type = "TEXT"
            # Ensure common types are handled reasonably (add more as needed)
            # If user explicitly maps 'id', respect their type unless it's clearly wrong
            if sanitized_col_name == 'id':
                 # Prioritize user definition if mapping to 'id', but standardize common case
                 if "INTEGER PRIMARY KEY" in validated_type:
                      column_defs.append(f'"{sanitized_col_name}" INTEGER PRIMARY KEY AUTOINCREMENT') # Standardize auto-increment
                 else:
                      # Use user's type if they map 'id' differently (e.g., TEXT PRIMARY KEY)
                      column_defs.append(f'"{sanitized_col_name}" {validated_type}')
            else: # For columns other than 'id'
                column_defs.append(f'"{sanitized_col_name}" {validated_type}')


        if not column_defs:
             # This condition might be unreachable if auto-id is always added when 'id' isn't mapped.
             # Check if only the auto-id exists if it was added
             auto_id_added = 'id' not in defined_sanitized_cols
             if auto_id_added and len(column_defs) == 1 and column_defs[0].startswith('"id"'):
                 logger.error(f"Table creation failed for '{sanitized_table_name}': No valid user-defined columns could be defined.")
                 return False
             elif not column_defs: # Should not happen if schema_definition wasn't empty
                  logger.error(f"Table creation failed for '{sanitized_table_name}': No columns could be defined.")
                  return False


        sql = f'CREATE TABLE IF NOT EXISTS "{sanitized_table_name}" ({", ".join(column_defs)});'
        logger.info(f"Attempting to execute schema statement for table '{sanitized_table_name}':\nSQL: {sql}")
        cursor = self.execute(sql, commit=True) # Execute the CREATE TABLE command

        if cursor is not None:
            logger.info(f"Table '{sanitized_table_name}' created or verified successfully.")
            # Additionally, verify schema if table already existed? Maybe too complex for now.
            return True
        else:
            logger.error(f"Failed to execute schema statement for table '{sanitized_table_name}'. Check previous error log for details.")
            return False
