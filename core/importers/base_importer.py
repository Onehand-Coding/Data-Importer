# core/importers/base_importer.py

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Any, Mapping, Optional, Generator
from pathlib import Path
import json
import logging
import pandas as pd
import sqlite3 # <--- ADD THIS LINE

class ImportResult:
    """Holds the results of an import operation."""
    def __init__(self):
        self.total_rows_processed: int = 0
        self.rows_inserted: int = 0
        self.rows_skipped: int = 0
        self.errors: List[Dict[str, Any]] = []

    def add_error(self, row_number: Optional[int], error: str, data_snippet: Optional[str] = None):
        """Adds an error entry."""
        error_entry = {
            'row': row_number if row_number is not None and row_number > 0 else ('Header/File' if row_number == 0 else 'Unknown'),
            'error': error,
            'data': data_snippet or "{}"
        }
        self.errors.append(error_entry)
        logging.warning(f"Import Error [Row: {error_entry['row']}]: {error} - Data: {data_snippet}")

    def to_dict(self) -> Dict[str, Any]:
        """Converts results to a dictionary."""
        return {
            'total': self.total_rows_processed,
            'inserted': self.rows_inserted,
            'skipped': self.rows_skipped,
            'errors': self.errors
        }

class BaseImporter(ABC):
    """
    Abstract base class for data importers.
    Defines the interface for reading, validating, and importing data.
    """
    # Define common supported file extensions for importers inheriting from this
    SUPPORTED_EXTENSIONS: List[str] = []

    def __init__(self, db_manager):
        if db_manager is None:
            raise ValueError("DatabaseManager instance is required.")
        self.db_manager = db_manager
        logging.info(f"{self.__class__.__name__} initialized.")

    @abstractmethod
    def get_headers(self, file_path: Path) -> List[str]:
        """
        Reads the source file and returns a list of header strings.
        Raises Exception on failure (e.g., file not found, format error).
        """
        pass

    @abstractmethod
    def get_preview(self, file_path: Path, num_rows: int = 5) -> pd.DataFrame:
        """
        Reads the first few rows of the source file for preview.
        Should return raw data before mapping.
        Raises Exception on failure.
        """
        pass

    @abstractmethod
    def read_data(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """
        Reads the source file row by row and yields a dictionary
        representing the raw data for each row (keys are original headers).
        Raises Exception on failure.
        """
        pass

    # --- Data Processing & Validation (Can have default implementations) ---

    def map_row(self, raw_row: Dict[str, Any], column_mapping: Mapping[str, str]) -> Dict[str, Any]:
        """
        Transforms a raw data row into a target data row based on mapping.
        Keys in the returned dict are the target database field names.
        Args:
            raw_row: Dict with keys as original source headers.
            column_mapping: Dict mapping {target_db_field: source_csv_header}.
        """
        target_data = {}
        for db_field, source_header in column_mapping.items():
            if source_header in raw_row:
                value = raw_row[source_header]
                # Basic cleaning (can be overridden by subclasses)
                target_data[db_field] = value.strip() if isinstance(value, str) else value
            else:
                # Handle case where expected source header isn't in the raw row
                logging.warning(f"Mapped source header '{source_header}' not found in raw row. Setting target field '{db_field}' to None.")
                target_data[db_field] = None
        return target_data

    def validate_mapped_row(self, mapped_row: Dict[str, Any], row_number: Optional[int], schema_info: Optional[Dict] = None) -> Tuple[bool, List[str]]:
        """
        Validates a single row *after* it has been mapped to target DB fields.
        Can be overridden by subclasses for format-specific validation.
        Args:
            mapped_row: Dict with keys as target DB field names.
            row_number: Original row number for context.
            schema_info: Optional dictionary containing schema details (e.g., required fields, types)
        Returns:
            Tuple (is_valid: bool, error_messages: List[str])
        """
        # Basic Example Validation (can be made more sophisticated)
        errors = []
        required = schema_info.get('required', []) if schema_info else [] # Example: Get required fields from schema info
        unique_fields = schema_info.get('unique', []) if schema_info else [] # Example: Get unique fields

        for field in required:
            if field not in mapped_row or not mapped_row.get(field):
                errors.append(f"Required field '{field.capitalize()}' is missing or empty.")

        # Basic email check if 'email' is a target field
        email_field = next((f for f in mapped_row if 'email' in f.lower()), None)
        if email_field and mapped_row.get(email_field):
            email_str = str(mapped_row[email_field])
            if '@' not in email_str or '.' not in email_str.split('@')[-1]:
                 errors.append(f"Invalid format for field '{email_field.capitalize()}': '{email_str}'")

        # Future: Add more validation based on schema_info (types, lengths, etc.)
        return len(errors) == 0, errors


    # --- Database Interaction (Should rely on DatabaseManager) ---

    def insert_data(self, table_name: str, data_to_insert: Dict[str, Any], result: ImportResult) -> bool:
        """
        Inserts a single row of mapped data into the specified database table.
        Args:
            table_name: Target database table name.
            data_to_insert: Dict with keys as target DB field names and values to insert.
            result: ImportResult object to log errors to.
        Returns:
            True if insertion was successful, False otherwise.
        """
        if not data_to_insert:
            result.add_error(None, "Skipping insert: No data provided for row.", "{}")
            return False

        # Sanitize table and column names (assuming db_manager has sanitize_name)
        sanitized_table_name = self.db_manager.sanitize_name(table_name)
        cols_to_insert = list(data_to_insert.keys())
        sanitized_cols = [self.db_manager.sanitize_name(col) for col in cols_to_insert]

        if None in sanitized_cols:
            invalid_original = [orig for orig, san in zip(cols_to_insert, sanitized_cols) if san is None]
            result.add_error(None, f"Skipping insert: Invalid column name(s) after sanitization: {invalid_original}", str(data_to_insert))
            return False
        if not sanitized_cols:
            result.add_error(None, "Skipping insert: No valid columns to insert.", str(data_to_insert))
            return False

        column_names = ', '.join([f'"{col}"' for col in sanitized_cols])
        placeholders = ', '.join(['?'] * len(sanitized_cols))
        sql = f"INSERT INTO \"{sanitized_table_name}\" ({column_names}) VALUES ({placeholders})"
        params = tuple(data_to_insert[original_col] for original_col in cols_to_insert)

        try:
            cursor = self.db_manager.execute(sql, params, commit=True)
            if cursor is not None:
                return True
            else:
                # Error logged by db_manager.execute, log context here
                result.add_error(None, "Database insert execution failed (check logs).", self._format_data_snippet(data_to_insert))
                return False
        # Use the imported sqlite3 module here
        except sqlite3.IntegrityError as e:
            error_msg = self._format_integrity_error(e, cols_to_insert, sanitized_cols)
            result.add_error(None, error_msg, self._format_data_snippet(data_to_insert))
            return False
        except Exception as e:
            logging.exception(f"Unexpected database insert error for table {sanitized_table_name}: {e}")
            result.add_error(None, f"Unexpected DB Insert Error: {str(e)}", self._format_data_snippet(data_to_insert))
            return False

    # --- Helper methods ---
    def _format_data_snippet(self, data: Dict[str, Any], max_len: int = 250) -> str:
        """Safely formats data dict to a truncated string for logging."""
        try:
            data_str = json.dumps(data)
            if len(data_str) > max_len:
                return data_str[:max_len - 3] + '...'
            return data_str
        except Exception:
            return "[Data serialization error]"

    # Use the imported sqlite3 module here for type hint
    def _format_integrity_error(self, error: sqlite3.IntegrityError, original_cols: List[str], sanitized_cols: List[str]) -> str:
        """Formats IntegrityError messages more nicely."""
        error_str = str(error)
        if 'UNIQUE constraint failed' in error_str:
            try:
                # Format: "UNIQUE constraint failed: table_name.column_name"
                failed_sanitized_column = error_str.split('.')[-1]
                # Find original name corresponding to sanitized name
                original_field = next((orig for orig, san in zip(original_cols, sanitized_cols) if san == failed_sanitized_column), failed_sanitized_column)
                return f"Skipped: Duplicate value for '{original_field}'."
            except Exception:
                 return "Skipped: Duplicate value constraint failed." # Fallback
        elif 'NOT NULL constraint failed' in error_str:
             try:
                failed_sanitized_column = error_str.split('.')[-1]
                original_field = next((orig for orig, san in zip(original_cols, sanitized_cols) if san == failed_sanitized_column), failed_sanitized_column)
                return f"Skipped: Missing value for required field '{original_field}'."
             except Exception:
                 return "Skipped: Missing value for required field." # Fallback
        else:
             return f"Skipped: Database Integrity Error - {error_str}"


    # --- Main Processing Logic ---
    def process_import(self, file_path: Path, table_name: str, column_mapping: Mapping[str, str], schema_info: Optional[Dict] = None) -> ImportResult:
        """
        Orchestrates the import process: read, map, validate, insert.
        """
        results = ImportResult()
        row_number = 1 # Start with 1 for header row (data starts row 2)
        try:
            for raw_row in self.read_data(file_path):
                row_number += 1
                results.total_rows_processed += 1
                try:
                    # 1. Map raw data to target structure
                    mapped_data = self.map_row(raw_row, column_mapping)

                    # 2. Validate the mapped data
                    is_valid, errors = self.validate_mapped_row(mapped_data, row_number, schema_info)
                    if not is_valid:
                        for error in errors:
                            results.add_error(row_number, error, self._format_data_snippet(raw_row))
                        results.rows_skipped += 1
                        continue # Skip this row

                    # 3. Insert into database
                    if self.insert_data(table_name, mapped_data, results):
                        results.rows_inserted += 1
                    else:
                        # Error logged within insert_data
                        results.rows_skipped += 1

                except Exception as row_err:
                    logging.exception(f"Unexpected error processing row {row_number}: {row_err}")
                    results.add_error(row_number, f"Unexpected row processing error: {str(row_err)}", self._format_data_snippet(raw_row))
                    results.rows_skipped += 1

        except Exception as file_err:
             logging.exception(f"Failed to read or process file {file_path}: {file_err}")
             results.add_error(0, f"Failed to read file: {str(file_err)}")
             # No rows processed if file read fails early

        return results
