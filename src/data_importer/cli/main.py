import re
import sys
import logging
import argparse
from pathlib import Path
from typing import Optional, Dict, Type

from data_importer.core.database import DatabaseManager
from data_importer.core.importers import (
    BaseImporter,
    ImportResult,
    CSVImporter,
    JSONImporter,
    ExcelImporter,
)

# --- Configure Logging ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Importer Factory (Updated for CLI) ---
IMPORTER_REGISTRY: Dict[str, Type[BaseImporter]] = {
    ".csv": CSVImporter,
    ".json": JSONImporter,
    ".xlsx": ExcelImporter,  # <-- Add ExcelImporter here
}


def get_importer_for_file(
    file_path: Path, db_manager: DatabaseManager
) -> Optional[BaseImporter]:
    """Factory function to get an importer instance based on file extension."""
    extension = file_path.suffix.lower()
    importer_class = IMPORTER_REGISTRY.get(extension)
    if importer_class:
        logger.info(
            f"Using importer {importer_class.__name__} for extension '{extension}'"
        )
        try:
            return importer_class(db_manager)
        except Exception as e:
            logger.exception(
                f"Failed to instantiate importer {importer_class.__name__}"
            )
            print(f"Error initializing importer for {extension} files: {e}")
            return None
    else:
        print(f"Error: Unsupported file type: '{extension}'. No importer found.")
        logger.warning(f"No importer registered for file extension: {extension}")
        return None


def sanitize_name(name):
    """Sanitizes a string to be a valid SQL table/column name."""
    if not isinstance(name, str):
        name = str(name)
    name = re.sub(r"[^\w_]", "_", name)  # Allow letters, numbers, underscore
    if name and name[0].isdigit():
        name = "_" + name  # Prepend underscore if starts with digit
    if not name:
        return None  # Return None if empty after sanitization
    return name.lower()  # Convert to lowercase


def print_results(results: ImportResult):
    """Print import results in a human-readable format from ImportResult object."""
    print("\nImport Results:")
    print(f"  Total rows processed: {results.total_rows_processed}")
    print(f"  Successfully inserted: {results.rows_inserted}")
    print(f"  Skipped rows: {results.rows_skipped}")

    if results.errors:
        print("\nErrors encountered:")
        for error in results.errors:
            row_info = error.get("row", "Unknown")
            err_msg = error.get("error", "No error message")
            data_snip = error.get("data", "{}")
            print(f"  Row {row_info}: {err_msg}")
            print(f"    Data: {data_snip}")


def main():
    parser = argparse.ArgumentParser(
        description="Import data from files to SQLite database"
    )
    parser.add_argument(
        "input_file",
        help="Path to the input file (e.g., data.csv, data.json, data.xlsx)",  # <-- Updated help text
    )
    parser.add_argument(
        "-t",
        "--table",
        default=None,
        help="Target table name (default: derived from filename)",
    )
    parser.add_argument(
        "-d",
        "--database",
        default="data/db/cli_database.db",
        help="Path to SQLite database file (default: data/db/cli_database.db)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Show detailed (DEBUG) logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)  # Set root logger to DEBUG
        logger.setLevel(logging.DEBUG)  # Also ensure our specific logger is DEBUG

    file_path = Path(args.input_file)
    if not file_path.exists():
        print(f"Error: File not found - {file_path}")
        return

    if not file_path.suffix.lower() in IMPORTER_REGISTRY:
        print(f"Error: Unsupported file type: {file_path.suffix}")
        print(f"Supported types are: {', '.join(IMPORTER_REGISTRY.keys())}")
        return

    db_path_str = args.database
    # Ensure the parent directory for the database exists
    db_parent_dir = Path(db_path_str).resolve().parent
    try:
        db_parent_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured database directory exists: {db_parent_dir}")
    except Exception as e:
        # Log warning but proceed, DatabaseManager might handle creation or it might be an in-memory DB.
        logger.warning(
            f"Could not create directory {db_parent_dir} for database. Error: {e}"
        )

    target_table_name = args.table or sanitize_name(file_path.stem)
    if not target_table_name:
        print(
            f"Error: Could not determine a valid table name from filename '{file_path.name}'. Use --table option."
        )
        return

    print(
        f"Starting import from '{file_path.name}' into table '{target_table_name}' in database '{db_path_str}'"
    )

    try:
        with DatabaseManager(db_path_str) as db:  # Use context manager
            if (
                not db.connection
            ):  # Check if connection was successful within DatabaseManager
                # Error would have been printed by DatabaseManager or get_db_manager if it failed
                print(
                    f"Error: Could not establish database connection to {db_path_str}. Exiting."
                )
                return

            importer = get_importer_for_file(file_path, db)
            if not importer:
                return  # Error already printed by factory

            # --- Prepare for import (Get headers, create mapping/schema) ---
            try:
                headers = importer.get_headers(file_path)
                if not headers:
                    print("Error: Could not read headers or file is empty.")
                    return

                # Default mapping: Use sanitized header as DB field name
                column_mapping = {
                    sanitize_name(h): h for h in headers if sanitize_name(h)
                }
                if len(column_mapping) != len(headers):
                    print(
                        "Warning: Some headers resulted in duplicate or invalid DB field names after sanitization."
                    )

                # Default schema: TEXT for all, unique for email
                schema_definition = {}
                schema_info = {"required": [], "unique": []}  # For validation
                for db_field in column_mapping.keys():
                    col_type = "TEXT"
                    # Basic type inference for CLI - can be expanded
                    if "email" in db_field.lower():
                        col_type = "TEXT UNIQUE"
                        schema_info["unique"].append(db_field)
                    elif any(
                        id_kw in db_field.lower()
                        for id_kw in ["id", "identifier", "number", "num", "pk"]
                    ):
                        # Avoid making all numeric fields INTEGER PRIMARY KEY automatically in CLI
                        # Keep it simple unless more specific CLI schema options are added
                        pass  # Keep as TEXT or infer other types if needed.
                    schema_definition[db_field] = col_type

                print(f"\nUsing inferred mapping and schema:")
                for db_col, csv_col in column_mapping.items():
                    print(
                        f"  '{csv_col}' ({file_path.suffix}) -> '{db_col}' (DB - {schema_definition.get(db_col, 'TEXT')})"
                    )
                print("")

            except Exception as e:
                print(f"Error reading headers or preparing mapping: {e}")
                logger.exception("Error in CLI header/mapping preparation:")
                return

            # --- Create table dynamically ---
            try:
                if not db.create_dynamic_table(target_table_name, schema_definition):
                    print(
                        f"Error: Failed to create or verify table '{target_table_name}'. Check logs."
                    )
                    return
                else:
                    print(f"Table '{target_table_name}' ensured in database.")
            except Exception as e:
                print(f"Error during table creation: {e}")
                logger.exception(f"Error creating table {target_table_name} in CLI:")
                return

            # --- Perform import using base importer's process ---
            print("\nStarting data processing...")
            try:
                results: ImportResult = importer.process_import(
                    file_path, target_table_name, column_mapping, schema_info
                )
                print_results(results)
            except Exception as e:
                print(f"\nImport failed critically: {e}")
                logger.exception(
                    "Critical error during importer.process_import in CLI:"
                )

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        logger.exception("Unexpected error in CLI main execution:")


if __name__ == "__main__":
    main()
