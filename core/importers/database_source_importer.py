# Content for: core/importers/database_source_importer.py
import logging
from pathlib import Path
from typing import Dict, List, Any, Generator, Optional, Mapping, Tuple
import re
import json # Added for _map_row_to_snippet_str

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import sqlite3

from core.database import DatabaseManager
from .base_importer import ImportResult

logger = logging.getLogger(__name__)

class DatabaseSourceImporter:
    def __init__(self, target_db_manager: DatabaseManager):
        if target_db_manager is None:
            raise ValueError("Target DatabaseManager instance is required.")
        self.target_db_manager = target_db_manager
        self.source_engine: Optional[Engine] = None
        self.source_inspector = None
        self.column_mapping: Mapping[str, str] = {}
        self.target_schema_info: Dict[str, Any] = {}
        logger.info(f"{self.__class__.__name__} initialized.")

    def connect_to_source(self, connection_string: str) -> bool:
        display_conn_str = connection_string
        if "@" in connection_string and "://" in connection_string:
            protocol_user_pass, host_db = connection_string.split("@", 1)
            protocol_user = protocol_user_pass.split("://", 1)[0] + "://" + protocol_user_pass.split("://",1)[1].split(":")[0]
            display_conn_str = f"{protocol_user}:********@{host_db}"
        logger.info(f"Attempting to connect to source database: {display_conn_str}")
        try:
            if self.source_engine: self.source_engine.dispose()
            self.source_engine = create_engine(connection_string)
            with self.source_engine.connect() as connection:
                self.source_inspector = inspect(self.source_engine)
            logger.info(f"Successfully connected to source database ({self.source_engine.name if self.source_engine else 'N/A'}).")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to source database ({display_conn_str}): {e}", exc_info=True)
            self.source_engine = None; self.source_inspector = None
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred while connecting to source database ({display_conn_str}): {e}", exc_info=True)
            self.source_engine = None; self.source_inspector = None
            return False

    def close_source_connection(self):
        if self.source_engine:
            self.source_engine.dispose()
            logger.info("Source database engine disposed.")
            self.source_engine = None
            self.source_inspector = None

    def get_table_names_from_source(self) -> List[str]:
        if not self.source_engine or not self.source_inspector:
            logger.error("Not connected to source database. Cannot get table names.")
            return []
        try:
            table_names = self.source_inspector.get_table_names()
            logger.info(f"Fetched table names from source: {table_names}")
            return table_names
        except SQLAlchemyError as e:
            logger.error(f"Error fetching table names from source: {e}", exc_info=True)
            return []

    def _get_quoted_identifier(self, identifier: str) -> str:
        if not self.source_engine:
            return f'"{identifier}"'
        dialect_name = self.source_engine.name
        if dialect_name == 'mysql' or dialect_name == 'mariadb':
            return f"`{identifier}`"
        elif dialect_name == 'postgresql' or dialect_name == 'sqlite':
            return f'"{identifier}"'
        else:
            logger.warning(f"Unknown dialect '{dialect_name}' for quoting, defaulting to double quotes.")
            return f'"{identifier}"'

    def get_headers_from_source(self, source_identifier: str, is_query: bool) -> List[str]:
        if not self.source_engine: raise ConnectionError("Not connected to source database for getting headers.")
        logger.info(f"Getting headers from source {'query' if is_query else 'table'}: {source_identifier[:150]}{'...' if len(source_identifier) > 150 else ''}")
        headers = []
        try:
            with self.source_engine.connect() as connection:
                if is_query:
                    limited_query_str = source_identifier.strip().rstrip(';')
                    # A common way to get headers for a query is to execute it with a condition that's always false or limit 0/1
                    # For many DBs, adding "LIMIT 0" works, but for others "WHERE 1=0" is more universal for no rows.
                    # For just headers from an arbitrary query, LIMIT 1 is safer if LIMIT 0 isn't universally supported for metadata.
                    if "limit" not in limited_query_str.lower():
                        # Ensure there's a space before LIMIT if query doesn't end with one.
                        limited_query_str = f"SELECT * FROM ({limited_query_str}) AS __subquery_for_headers LIMIT 1"
                    else: # Query might already have a limit, or be complex. Trust the user for now.
                        # This path might be problematic if user's query is not a SELECT.
                         limited_query_str = text(f"SELECT * FROM ({limited_query_str}) AS __subquery_for_headers LIMIT 1")


                    result_proxy = connection.execute(text(limited_query_str))
                    headers = list(result_proxy.keys())
                    result_proxy.close()
                else:
                    if not self.source_inspector: self.source_inspector = inspect(self.source_engine)
                    columns = self.source_inspector.get_columns(source_identifier)
                    headers = [col['name'] for col in columns]
            logger.info(f"Retrieved headers: {headers}")
            return headers
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemyError getting headers from source '{source_identifier}': {e}", exc_info=True)
            raise ValueError(f"Could not get headers from source '{source_identifier}': {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error getting headers from source '{source_identifier}': {e}", exc_info=True)
            raise ValueError(f"Unexpected error getting headers: {e}") from e

    def get_preview_from_source(self, source_identifier: str, is_query: bool, num_rows: int = 5) -> pd.DataFrame:
        if not self.source_engine: raise ConnectionError("Not connected to source database for preview.")
        logger.info(f"Generating preview ({num_rows} rows) from source {'query' if is_query else 'table'}: {source_identifier[:150]}{'...' if len(source_identifier) > 150 else ''}")
        try:
            query_to_execute_str = ""
            if is_query:
                temp_query = source_identifier.strip().rstrip(';')
                if "limit" not in temp_query.lower():
                     query_to_execute_str = f"SELECT * FROM ({temp_query}) AS __subquery_for_preview LIMIT {num_rows}"
                else:
                    query_to_execute_str = temp_query
            else:
                quoted_table_name = self._get_quoted_identifier(source_identifier)
                query_to_execute_str = f"SELECT * FROM {quoted_table_name} LIMIT {num_rows}"

            df = pd.read_sql_query(sql=text(query_to_execute_str), con=self.source_engine)
            return df.astype(str)
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemyError generating preview from source '{source_identifier}': {e}", exc_info=True)
            raise ValueError(f"Could not generate preview from source '{source_identifier}': {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error generating preview from source '{source_identifier}': {e}", exc_info=True)
            raise ValueError(f"Unexpected error generating preview: {e}") from e

    def read_data_from_source(self, source_identifier: str, is_query: bool, chunk_size: int = 100) -> Generator[Dict[str, Any], None, None]:
        if not self.source_engine: raise ConnectionError("Not connected to source database for reading data.")
        logger.info(f"Streaming data from source {'query' if is_query else 'table'}: {source_identifier[:150]}{'...' if len(source_identifier) > 150 else ''}")
        query_to_execute_str = source_identifier
        if not is_query:
            quoted_table_name = self._get_quoted_identifier(source_identifier)
            query_to_execute_str = f"SELECT * FROM {quoted_table_name}"
        try:
            with self.source_engine.connect() as connection:
                result_proxy = connection.execute(text(query_to_execute_str).execution_options(stream_results=True))
                while True:
                    chunk_of_rows = result_proxy.fetchmany(chunk_size)
                    if not chunk_of_rows: break
                    for row in chunk_of_rows: yield row._asdict()
                result_proxy.close()
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemyError reading data from source '{source_identifier}': {e}", exc_info=True)
            raise ValueError(f"Could not read data from source '{source_identifier}': {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error reading data from source '{source_identifier}': {e}", exc_info=True)
            raise ValueError(f"Unexpected error reading data: {e}") from e

    def set_column_mapping(self, column_mapping: Mapping[str, str]):
        self.column_mapping = column_mapping
        logger.debug(f"DatabaseSourceImporter: Column mapping for target set: {self.column_mapping}")

    def set_table_schema_info(self, schema_info: Dict[str, Any]):
        self.target_schema_info = schema_info
        logger.debug(f"DatabaseSourceImporter: Target table schema info set: {self.target_schema_info}")

    def _map_row_to_snippet_str(self, data_row: Optional[Dict[str, Any]], max_len: int = 100) -> str:
        if data_row is None: return "{}"
        try:
            snippet = json.dumps({k: str(v) for k, v in data_row.items()})
        except TypeError: snippet = str(data_row)
        return snippet[:max_len-3] + "..." if len(snippet) > max_len else snippet

    def _map_source_row_to_target(self, raw_source_row: Dict[str, Any]) -> Dict[str, Any]:
        data_for_target_table = {}
        if not self.column_mapping:
            logger.warning("Column mapping not set in DatabaseSourceImporter. Attempting 1:1 sanitized map.")
            for src_key, value in raw_source_row.items():
                target_key = self.target_db_manager.sanitize_name(src_key)
                if target_key:
                    stripped_value = value.strip() if isinstance(value, str) else value
                    data_for_target_table[target_key] = None if isinstance(stripped_value, str) and not stripped_value else stripped_value
            return data_for_target_table

        for target_field, source_column_header in self.column_mapping.items():
            value = raw_source_row.get(source_column_header)
            if value is not None:
                stripped_value = value.strip() if isinstance(value, str) else value
                data_for_target_table[target_field] = None if isinstance(stripped_value, str) and not stripped_value else stripped_value
            else:
                data_for_target_table[target_field] = None
        return data_for_target_table

    def _validate_target_row(self, mapped_target_row: Dict[str, Any], row_number: Optional[int], schema_info_for_validation: Dict[str, Any]) -> Tuple[bool, List[str]]:
        is_valid = True
        errors: List[str] = []
        required_fields = schema_info_for_validation.get('required', [])

        # Check required fields first
        for req_field in required_fields:
            # original_source_header logic for error message enrichment
            original_source_header = req_field # Default to target field name if not in mapping (should not happen if mapping is complete)
            # Find which source_db_column maps to this target_sqlite_field (req_field)
            for target_key_in_map, source_val_in_map in self.column_mapping.items():
                if target_key_in_map == req_field:
                    original_source_header = source_val_in_map
                    break

            if mapped_target_row.get(req_field) is None: # Value already processed by _map_source_row_to_target
                errors.append(f"Required field '{original_source_header}' (target: '{req_field}') for target table is missing or empty.")
                is_valid = False

        # Then check other validations like email format
        for target_db_field_name, value in mapped_target_row.items():
            original_source_header = self.column_mapping.get(target_db_field_name, target_db_field_name)
            column_specific_target_schema = self.target_schema_info.get(target_db_field_name, {})

            is_email_field_in_target = False
            if isinstance(column_specific_target_schema, dict):
                is_email_field_in_target = column_specific_target_schema.get('is_email', False)

            if not is_email_field_in_target and "email" in target_db_field_name.lower():
                 is_email_field_in_target = True

            if is_email_field_in_target:
                if value and isinstance(value, str) and not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", value):
                    errors.append(f"Invalid email format for target field '{target_db_field_name}' (from '{original_source_header}'): '{value}'.")
                    is_valid = False
        return is_valid, errors

    def _insert_data_to_target(self, sanitized_target_table_name: str, data_to_insert: Dict[str, Any], result: ImportResult, row_number: Optional[int]) -> bool:
        if not data_to_insert:
            result.add_error(row_number, "No data to insert into target (empty mapped row).", None)
            return False
        columns = ', '.join(f'"{col}"' for col in data_to_insert.keys())
        placeholders = ', '.join(['?'] * len(data_to_insert))
        query = f'INSERT INTO "{sanitized_target_table_name}" ({columns}) VALUES ({placeholders})'
        try:
            cursor = self.target_db_manager.execute(query, tuple(data_to_insert.values()), commit=True)
            if cursor:
                result.rows_inserted += 1
                return True
            # This part of the "else" might be unreachable if execute() raises on failure or returns None only for non-row-returning statements
            result.add_error(row_number, "Target DB insert failed (no cursor/unknown reason).", self._map_row_to_snippet_str(data_to_insert))
            return False
        except sqlite3.IntegrityError as ie:
            logger.warning(f"Target DB Integrity error for table {sanitized_target_table_name} on source row {row_number}: {ie} - Data: {data_to_insert}")
            # Attempt to use _format_integrity_error from target_db_manager if it exists (it does in BaseImporter)
            # The schema passed to _format_integrity_error should be the TARGET schema.
            # self.column_mapping here is {target_field: source_field}, _format_integrity_error expects {db_field: csv_header}
            # This might need adjustment or a specific formatter for DB-to-DB
            error_msg = f"Target DB Integrity Error: {ie}" # Simplified message for now
            if hasattr(self.target_db_manager, '_format_integrity_error_from_mapping_and_schema'):
                 # This method would need to be created in DatabaseManager or a shared utility
                 # It would take the integrity error, the current {target_field: source_field} mapping,
                 # and the detailed target schema to provide a user-friendly column name.
                 # For now, using a simpler message.
                 # error_msg = self.target_db_manager._format_integrity_error_from_mapping_and_schema(ie, self.column_mapping, self.target_schema_info)
                 pass

            result.add_error(row_number, error_msg, self._map_row_to_snippet_str(data_to_insert))
            return False
        except Exception as e:
            logger.exception(f"Unexpected target DB insert error for table {sanitized_target_table_name} on source row {row_number}: {e}")
            result.add_error(row_number, f"Unexpected Target DB Insert Error: {str(e)}", self._map_row_to_snippet_str(data_to_insert))
            return False

    def process_import_to_target(
        self,
        source_identifier: str, is_query: bool,
        target_table_name: str,
        column_mapping_for_target: Mapping[str, str],
        detailed_schema_for_target_table: Dict[str, Any],
        schema_info_for_target_validation: Dict[str, Any]
    ) -> ImportResult:
        results = ImportResult()
        self.set_column_mapping(column_mapping_for_target)
        self.set_table_schema_info(detailed_schema_for_target_table)

        sanitized_target_table_name = self.target_db_manager.sanitize_name(target_table_name)
        if not sanitized_target_table_name:
            results.add_error(0, f"Invalid target table name: '{target_table_name}'")
            return results

        logger.info(f"Starting DB-to-DB import from source '{source_identifier}' to target SQLite table '{sanitized_target_table_name}'")
        try:
            for row_idx, raw_source_row in enumerate(self.read_data_from_source(source_identifier, is_query), 1):
                results.total_rows_processed += 1
                try:
                    data_for_target = self._map_source_row_to_target(raw_source_row)
                    if not data_for_target or all(v is None for v in data_for_target.values()):
                        results.add_error(row_idx, "Row became empty after mapping from source.", self._map_row_to_snippet_str(raw_source_row))
                        results.rows_skipped += 1
                        continue
                    is_valid, validation_errors = self._validate_target_row(data_for_target, row_idx, schema_info_for_target_validation)
                    if is_valid:
                        if not self._insert_data_to_target(sanitized_target_table_name, data_for_target, results, row_idx):
                            results.rows_skipped += 1
                    else:
                        results.add_error(row_idx, "; ".join(validation_errors), self._map_row_to_snippet_str(data_for_target))
                        results.rows_skipped += 1
                except Exception as row_err:
                    logger.exception(f"Unexpected error processing source DB row {row_idx}: {row_err}")
                    results.add_error(row_idx, f"Unexpected processing error for source row: {str(row_err)}", self._map_row_to_snippet_str(raw_source_row))
                    results.rows_skipped += 1
            if self.target_db_manager.connection and self.target_db_manager.connection.in_transaction:
                logger.info("Committing final transaction for DB-to-DB import (if any).")
                self.target_db_manager.connection.commit()
        except ValueError as ve:
             logger.exception(f"ValueError during DB source processing {source_identifier}: {ve}")
             results.add_error(0, f"Error processing DB source: {str(ve)}")
        except Exception as general_err:
            logger.exception(f"General failure during DB-to-DB import from {source_identifier}: {general_err}")
            results.add_error(0, f"Failed to import from DB source: {str(general_err)}")
        logger.info(f"DB-to-DB import finished. Processed: {results.total_rows_processed}, Inserted: {results.rows_inserted}, Skipped: {results.rows_skipped}")
        return results
