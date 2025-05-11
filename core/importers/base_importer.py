# Content for: core/importers/base_importer.py
import sqlite3
import json
import logging
import re
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Any, Mapping, Optional, Generator

import pandas as pd
from core.database import DatabaseManager # Ensure this path is correct

logger = logging.getLogger(__name__)

class ImportResult:
    """Holds the results of an import operation."""
    def __init__(self):
        self.total_rows_processed: int = 0
        self.rows_inserted: int = 0
        self.rows_skipped: int = 0
        self.errors: List[Dict[str, Any]] = []

    def add_error(self, row_number: Optional[int], error: str, data_snippet: Optional[str] = None):
        """Adds an error to the import results."""
        row_display = str(row_number) if row_number is not None else "Unknown"
        error_entry = {
            'row': row_display,
            'error': error,
            'data': data_snippet or "{}"
        }
        self.errors.append(error_entry)
        logger.warning(f"Import Error [Row: {row_display}]: {error} - Data: {data_snippet}")

    def to_dict(self) -> Dict[str, Any]:
        """Converts results to a dictionary."""
        return {
            "total": self.total_rows_processed,
            "inserted": self.rows_inserted,
            "skipped": self.rows_skipped,
            "errors": self.errors
        }

class BaseImporter(ABC):
    def __init__(self, db_manager: DatabaseManager):
        if db_manager is None:
            raise ValueError("DatabaseManager instance is required.")
        self.db_manager = db_manager
        self.column_mapping: Mapping[str, str] = {}
        self.schema_info: Dict[str, Any] = {}
        logger.info(f"{self.__class__.__name__} initialized.")

    @abstractmethod
    def get_headers(self, file_path: Path) -> List[str]:
        pass

    @abstractmethod
    def get_preview(self, file_path: Path, num_rows: int = 5) -> pd.DataFrame:
        pass

    @abstractmethod
    def read_data(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        pass

    def set_column_mapping(self, column_mapping: Mapping[str, str]):
        self.column_mapping = column_mapping
        logger.debug(f"Column mapping set for importer: {self.column_mapping}")

    def set_table_schema_info(self, schema_info: Dict[str, Any]):
        self.schema_info = schema_info
        logger.debug(f"Table schema info set for importer: {self.schema_info}")

    def _map_row(self, raw_row: Dict[str, Any]) -> Dict[str, Any]:
        target_data = {}
        for db_field, source_header in self.column_mapping.items():
            value = raw_row.get(source_header)

            if value is not None:
                stripped_value = value.strip() if isinstance(value, str) else value
                # Convert empty string after strip to None, otherwise use stripped_value
                target_data[db_field] = None if isinstance(stripped_value, str) and not stripped_value else stripped_value
            else:
                logger.debug(f"Source header '{source_header}' for DB field '{db_field}' not found in raw row or value is None. Setting to None.")
                target_data[db_field] = None
        return target_data

    def _format_data_snippet(self, data_row: Optional[Dict[str, Any]], max_len: int = 100) -> str:
        if data_row is None:
            return "{}"
        try:
            serializable_data = {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v for k, v in data_row.items()}
            snippet = json.dumps(serializable_data)
        except TypeError:
            snippet = json.dumps({k: str(v) for k, v in data_row.items()})

        if len(snippet) > max_len:
            return snippet[:max_len-3] + "..."
        return snippet

    def _format_integrity_error(self, e: sqlite3.IntegrityError, schema_info_for_validation: Dict) -> str:
        msg = str(e)
        unique_match = re.search(r"UNIQUE constraint failed: \w+\.(.*)", msg, re.IGNORECASE)
        if unique_match:
            column_name_from_error = unique_match.group(1)
            sanitized_db_field = self.db_manager.sanitize_name(column_name_from_error)
            original_column_name = next((sh for db_f, sh in self.column_mapping.items() if db_f == sanitized_db_field), sanitized_db_field)
            return f"Skipped: Duplicate value for '{original_column_name}'."

        not_null_match = re.search(r"NOT NULL constraint failed: \w+\.(.*)", msg, re.IGNORECASE)
        if not_null_match:
            column_name_from_error = not_null_match.group(1)
            sanitized_db_field = self.db_manager.sanitize_name(column_name_from_error)
            original_column_name = next((sh for db_f, sh in self.column_mapping.items() if db_f == sanitized_db_field), sanitized_db_field)
            return f"Skipped: Required field '{original_column_name}' is missing."

        return f"Skipped: Data integrity issue - {msg}"

    def validate_mapped_row(self, mapped_row: Dict[str, Any], row_number: Optional[int], schema_info_for_validation: Dict[str, Any]) -> Tuple[bool, List[str]]:
        is_valid = True
        errors: List[str] = []

        required_fields = schema_info_for_validation.get('required', [])

        for db_field_name, value in mapped_row.items():
            original_header_name = next((sh for db_f, sh in self.column_mapping.items() if db_f == db_field_name), db_field_name)

            if db_field_name in required_fields:
                if value is None: # Checks for None, which now includes previously empty strings
                    errors.append(f"Required field '{original_header_name}' is missing or empty.")
                    is_valid = False

            column_specific_schema_details = self.schema_info.get(db_field_name, {})
            is_email_field = column_specific_schema_details.get('is_email', False)
            if not is_email_field and "email" in db_field_name.lower():
                 is_email_field = True

            if is_email_field:
                if value and isinstance(value, str) and not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", value):
                    errors.append(f"Invalid format for field '{original_header_name}': '{value}'.")
                    is_valid = False
        return is_valid, errors

    def _insert_data(self, sanitized_table_name: str, data_to_insert: Dict[str, Any], result: ImportResult, row_number: Optional[int]) -> bool:
        if not data_to_insert:
            result.add_error(row_number, "No data to insert (empty mapped row).", None)
            return False

        columns = ', '.join(f'"{col}"' for col in data_to_insert.keys())
        placeholders = ', '.join(['?'] * len(data_to_insert))
        query = f'INSERT INTO "{sanitized_table_name}" ({columns}) VALUES ({placeholders})'

        try:
            cursor = self.db_manager.execute(query, tuple(data_to_insert.values()), commit=True)
            if cursor:
                result.rows_inserted += 1
                return True
            else:
                result.add_error(row_number, "DB insert failed (no cursor/unknown reason after execute).", self._format_data_snippet(data_to_insert))
                return False
        except sqlite3.IntegrityError as ie:
            logger.warning(f"Integrity error for table {sanitized_table_name} on row {row_number}: {ie} - Data: {data_to_insert}")
            # Use schema_info_for_validation passed to process_import, or self.schema_info if more appropriate context needed by _format_integrity_error
            error_msg = self._format_integrity_error(ie, self.schema_info)
            result.add_error(row_number, error_msg, self._format_data_snippet(data_to_insert))
            return False
        except Exception as e:
            logger.exception(f"Unexpected database insert error for table {sanitized_table_name} on row {row_number}: {e}")
            result.add_error(row_number, f"Unexpected DB Insert Error: {str(e)}", self._format_data_snippet(data_to_insert))
            return False

    def process_import(self, file_path: Path, table_name: str, column_mapping: Mapping[str, str], schema_info_for_validation: Dict[str, Any]) -> ImportResult:
        results = ImportResult()
        self.set_column_mapping(column_mapping)
        # self.schema_info (full schema) should be set by app logic if detailed validation rules are needed beyond schema_info_for_validation
        # For now, validate_mapped_row primarily uses schema_info_for_validation and infers some things like email from field names or explicit self.schema_info flags.

        sanitized_table_name = self.db_manager.sanitize_name(table_name)
        if not sanitized_table_name:
            results.add_error(0, f"Invalid table name provided: '{table_name}'")
            return results

        try:
            for row_number, raw_row in enumerate(self.read_data(file_path), 1):
                results.total_rows_processed += 1
                logger.debug(f"Raw row {row_number}: {raw_row}")

                try:
                    mapped_row = self._map_row(raw_row) # This now converts empty strings to None
                    logger.debug(f"Mapped row {row_number}: {mapped_row}")

                    is_empty_after_map = all(v is None for v in mapped_row.values())
                    if not mapped_row or is_empty_after_map:
                        logger.warning(f"Row {row_number} resulted in empty mapped data. Skipping.")
                        results.add_error(row_number, "Row is empty after mapping or source row was effectively empty.", self._format_data_snippet(raw_row))
                        results.rows_skipped += 1
                        continue

                    # Pass schema_info_for_validation, which contains 'required', 'unique' lists
                    is_valid, validation_errors = self.validate_mapped_row(mapped_row, row_number, schema_info_for_validation)

                    if is_valid:
                        if not self._insert_data(sanitized_table_name, mapped_row, results, row_number):
                            results.rows_skipped += 1
                    else:
                        results.add_error(row_number, "; ".join(validation_errors), self._format_data_snippet(mapped_row))
                        results.rows_skipped += 1

                except Exception as row_err:
                    logger.exception(f"Unexpected error processing data for row {row_number}: {row_err}")
                    results.add_error(row_number, f"Unexpected row processing error: {str(row_err)}", self._format_data_snippet(raw_row))
                    results.rows_skipped += 1

            if self.db_manager.connection and self.db_manager.connection.in_transaction:
                logger.info("Committing final transaction for batch import.")
                try:
                    self.db_manager.connection.commit()
                except sqlite3.Error as e:
                    logger.error(f"Final commit failed: {e}. Attempting rollback.")
                    try:
                        self.db_manager.connection.rollback()
                    except Exception as rb_e: # pragma: no cover
                        logger.error(f"Rollback attempt also failed: {rb_e}")
                    results.add_error(None, f"Database commit error at end of import: {e}")

        except ValueError as ve:
             logger.exception(f"ValueError during file processing {file_path}: {ve}")
             results.add_error(0, f"Error processing file: {str(ve)}")
        except Exception as file_err:
            logger.exception(f"General failure to read or process file {file_path}: {file_err}")
            results.add_error(0, f"Failed to read or process file: {str(file_err)}")

        return results
