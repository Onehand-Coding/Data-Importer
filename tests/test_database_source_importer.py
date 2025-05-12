import unittest
from unittest.mock import patch, MagicMock, ANY
import sqlite3
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError, OperationalError as SQLAlchemyOperationalError

from core.database import DatabaseManager
from core.importers.database_source_importer import DatabaseSourceImporter
from core.importers.base_importer import ImportResult

import logging
logging.disable(logging.CRITICAL)

class TestDatabaseSourceImporter(unittest.TestCase):

    def setUp(self):
        self.target_db_path = ":memory:"
        self.target_db_manager = DatabaseManager(self.target_db_path)
        self.assertTrue(self.target_db_manager.connect(), "Failed to connect to in-memory target DB for tests")

        self.importer = DatabaseSourceImporter(self.target_db_manager)

        self.mock_engine = MagicMock(spec=create_engine("sqlite:///:memory:").__class__)
        self.mock_connection = MagicMock()
        self.mock_inspector = MagicMock()

        self.mock_engine.connect.return_value.__enter__.return_value = self.mock_connection
        self.mock_engine.name = "mock_db"

        self.create_engine_patcher = patch('sqlalchemy.create_engine', return_value=self.mock_engine)
        self.mock_create_engine = self.create_engine_patcher.start()

        self.inspect_patcher = patch('sqlalchemy.inspect', return_value=self.mock_inspector)
        self.mock_sqlalchemy_inspect = self.inspect_patcher.start()

    def tearDown(self):
        if self.importer.source_engine:
            self.importer.close_source_connection()
        if self.target_db_manager and self.target_db_manager.connection:
            self.target_db_manager.close()

        self.create_engine_patcher.stop()
        self.inspect_patcher.stop()

    def test_initialization_success(self):
        self.assertIsNotNone(self.importer)
        self.assertEqual(self.importer.target_db_manager, self.target_db_manager)
        self.assertIsNone(self.importer.source_engine)

    def test_initialization_no_target_db_manager(self):
        with self.assertRaisesRegex(ValueError, "Target DatabaseManager instance is required."):
            DatabaseSourceImporter(None)

    def test_connect_to_source_success(self):
        conn_str = "sqlite:///:memory:"

        # Temporarily stop the default create_engine patcher
        self.create_engine_patcher.stop()
        self.inspect_patcher.stop()

        # Allow real create_engine and inspect for this test case
        actual_engine = create_engine(conn_str) # This will create a real in-memory engine

        # Patch inspect to return a real inspector for the actual_engine
        inspect_patch = patch('sqlalchemy.inspect', return_value=inspect(actual_engine))
        mock_inspect_call = inspect_patch.start()

        create_engine_patch_local = patch('sqlalchemy.create_engine', return_value=actual_engine)
        mock_create_engine_local = create_engine_patch_local.start()

        self.assertTrue(self.importer.connect_to_source(conn_str))
        self.assertIsNotNone(self.importer.source_engine)
        self.assertEqual(self.importer.source_engine.name, "sqlite")
        self.assertIsNotNone(self.importer.source_inspector)

        # Stop local patches
        mock_create_engine_local.stop()
        mock_inspect_call.stop()

        # Restart default patchers
        self.create_engine_patcher.start()
        self.inspect_patcher.start()


    def test_connect_to_source_sqlalchemy_error_on_connect(self):
        self.mock_engine.connect.side_effect = SQLAlchemyError("Simulated DB connection error")
        self.assertFalse(self.importer.connect_to_source("postgresql://test"))
        self.assertIsNone(self.importer.source_engine)
        self.assertIsNone(self.importer.source_inspector)

    def test_connect_to_source_malformed_url_error(self):
        self.mock_create_engine.side_effect = Exception("Simulated malformed URL")
        self.assertFalse(self.importer.connect_to_source("badscheme://test"))
        self.assertIsNone(self.importer.source_engine)

    def test_close_source_connection(self):
        self.importer.source_engine = self.mock_engine
        self.importer.source_inspector = self.mock_inspector
        self.importer.close_source_connection()
        self.mock_engine.dispose.assert_called_once()
        self.assertIsNone(self.importer.source_engine)
        self.assertIsNone(self.importer.source_inspector)

    def test_close_source_connection_no_engine(self):
        self.importer.source_engine = None
        self.importer.close_source_connection()

    def test_get_table_names_from_source_success(self):
        self.importer.source_engine = self.mock_engine
        self.importer.source_inspector = self.mock_inspector
        expected_tables = ["table1", "table2"]
        self.mock_inspector.get_table_names.return_value = expected_tables
        tables = self.importer.get_table_names_from_source()
        self.assertEqual(tables, expected_tables)

    def test_get_table_names_no_connection(self):
        self.importer.source_engine = None
        self.importer.source_inspector = None
        tables = self.importer.get_table_names_from_source()
        self.assertEqual(tables, [])

    def test_get_table_names_sqlalchemy_error(self):
        self.importer.source_engine = self.mock_engine
        self.importer.source_inspector = self.mock_inspector
        self.mock_inspector.get_table_names.side_effect = SQLAlchemyError("Simulated error")
        tables = self.importer.get_table_names_from_source()
        self.assertEqual(tables, [])

    def test_get_quoted_identifier(self):
        self.importer.source_engine = self.mock_engine
        self.mock_engine.name = "mysql"
        self.assertEqual(self.importer._get_quoted_identifier("myTable"), "`myTable`")
        self.mock_engine.name = "postgresql"
        self.assertEqual(self.importer._get_quoted_identifier("myTable"), '"myTable"')
        self.mock_engine.name = "sqlite"
        self.assertEqual(self.importer._get_quoted_identifier("myTable"), '"myTable"')
        self.mock_engine.name = "unknown_dialect"
        self.assertEqual(self.importer._get_quoted_identifier("myTable"), '"myTable"')

    def test_get_headers_from_source_table_success(self):
        self.importer.source_engine = self.mock_engine
        self.importer.source_inspector = self.mock_inspector
        self.mock_inspector.get_columns.return_value = [{'name': 'col1'}, {'name': 'col2'}]
        headers = self.importer.get_headers_from_source("test_table", is_query=False)
        self.assertEqual(headers, ['col1', 'col2'])

    def test_get_headers_from_source_query_success(self):
        self.importer.source_engine = self.mock_engine
        mock_result_proxy = MagicMock()
        # SQLAlchemy Row objects (typically what fetchone returns) have a `keys()` method
        # or `_fields` attribute that gives column names.
        # `result_proxy.keys()` is appropriate for a ResultProxy.
        mock_result_proxy.keys.return_value = ['headerA', 'headerB']
        self.mock_connection.execute.return_value = mock_result_proxy

        query = "SELECT a, b FROM test_table"
        headers = self.importer.get_headers_from_source(query, is_query=True)
        self.assertEqual(headers, ['headerA', 'headerB'])
        self.mock_connection.execute.assert_called_once()
        executed_sql = self.mock_connection.execute.call_args[0][0].text
        self.assertIn("LIMIT 1", executed_sql.upper())

    @patch('pandas.read_sql_query')
    def test_get_preview_from_source_table(self, mock_pd_read_sql):
        self.importer.source_engine = self.mock_engine
        self.mock_engine.name = "sqlite"
        mock_df = pd.DataFrame({'col1': ['data1'], 'col2': ['data2']})
        mock_pd_read_sql.return_value = mock_df
        df_preview = self.importer.get_preview_from_source("my_table", is_query=False, num_rows=1)
        mock_pd_read_sql.assert_called_once()
        called_sql = mock_pd_read_sql.call_args[1]['sql'].text
        self.assertEqual(called_sql, 'SELECT * FROM "my_table" LIMIT 1') # SQLite uses "
        pd.testing.assert_frame_equal(df_preview, mock_df.astype(str))

    def test_read_data_from_source_table(self):
        self.importer.source_engine = self.mock_engine
        self.mock_engine.name = "postgresql"
        mock_row1 = MagicMock()
        mock_row1._asdict.return_value = {'id': 1, 'name': 'Alice'}
        mock_row2 = MagicMock()
        mock_row2._asdict.return_value = {'id': 2, 'name': 'Bob'}
        mock_result_proxy = MagicMock()
        mock_result_proxy.fetchmany.side_effect = [[mock_row1, mock_row2], []]
        self.mock_connection.execute.return_value = mock_result_proxy
        data = list(self.importer.read_data_from_source("my_pg_table", is_query=False, chunk_size=50))
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], {'id': 1, 'name': 'Alice'})
        executed_sql = self.mock_connection.execute.call_args[0][0].text
        self.assertEqual(executed_sql, 'SELECT * FROM "my_pg_table"') # PostgreSQL uses "

    def test_map_source_row_to_target(self):
        self.importer.column_mapping = {"target_id": "src_id", "target_name": "src_name"}
        raw_row = {"src_id": 1, "src_name": " Test Name ", "extra_col": "ignore"}
        mapped = self.importer._map_source_row_to_target(raw_row)
        self.assertEqual(mapped, {"target_id": 1, "target_name": "Test Name"})
        raw_row_empty_str = {"src_id": 2, "src_name": "  "}
        mapped_empty = self.importer._map_source_row_to_target(raw_row_empty_str)
        self.assertEqual(mapped_empty, {"target_id": 2, "target_name": None})

    def test_validate_target_row(self):
        # This method uses self.target_schema_info for 'is_email' etc.
        # and schema_info_for_validation for 'required' list.
        self.importer.target_schema_info = {
            "email_col": {"is_email": True, "type": "TEXT UNIQUE"},
            "name_col": {"type": "TEXT", "required": True},
            "optional_col": {"type": "TEXT"}
        }
        # This mapping helps _validate_target_row find original source header for error messages
        self.importer.column_mapping = {"name_col": "Source Name", "email_col": "Source Email"}

        schema_info_val_arg = {"required": ["name_col"], "unique": ["email_col"]}

        valid_row = {"name_col": "Alice", "email_col": "alice@example.com", "optional_col": "data"}
        is_valid, errors = self.importer._validate_target_row(valid_row, 1, schema_info_val_arg)
        self.assertTrue(is_valid, f"Validation failed for valid row: {errors}")
        self.assertEqual(len(errors), 0)

        # Test missing required field
        missing_req_row = {"email_col": "bob@example.com"} # name_col is not in the dict
        is_valid, errors = self.importer._validate_target_row(missing_req_row, 2, schema_info_val_arg)
        self.assertFalse(is_valid, "Validation should have failed for missing required field")
        self.assertEqual(len(errors), 1)
        self.assertIn("Required field 'Source Name' (target: 'name_col') for target table is missing or empty.", errors[0])

        # Test missing required field which is present but None
        missing_req_row_none = {"name_col": None, "email_col": "bob@example.com"}
        is_valid, errors = self.importer._validate_target_row(missing_req_row_none, 2, schema_info_val_arg)
        self.assertFalse(is_valid, "Validation should have failed for None required field")
        self.assertEqual(len(errors), 1)
        self.assertIn("Required field 'Source Name' (target: 'name_col') for target table is missing or empty.", errors[0])


        invalid_email_row = {"name_col": "Charlie", "email_col": "charlie.com"}
        is_valid, errors = self.importer._validate_target_row(invalid_email_row, 3, schema_info_val_arg)
        self.assertFalse(is_valid)
        self.assertIn("Invalid email format for target field 'email_col'", errors[0])


    @patch.object(DatabaseSourceImporter, '_insert_data_to_target')
    @patch.object(DatabaseSourceImporter, '_validate_target_row', return_value=(True, []))
    @patch.object(DatabaseSourceImporter, '_map_source_row_to_target')
    @patch.object(DatabaseSourceImporter, 'read_data_from_source')
    def test_process_import_to_target_success_flow(
        self, mock_read_data, mock_map_row, mock_validate, mock_insert_data_to_target):

        # Define a side effect function for the _insert_data_to_target mock
        def insert_side_effect(sanitized_table_name, data_to_insert, results_obj, row_number):
            results_obj.rows_inserted += 1
            return True
        mock_insert_data_to_target.side_effect = insert_side_effect # Apply the side effect

        mock_read_data.return_value = iter([
            {"src_id": 1, "src_name": "Alice_Source"},
            {"src_id": 2, "src_name": "Bob_Source"}
        ])
        mock_map_row.side_effect = [
            {"id_target": 1, "name_target": "Alice_Target"},
            {"id_target": 2, "name_target": "Bob_Target"}
        ]

        source_id = "my_source_table_or_query"
        is_query = False
        target_table = "target_sqlite_table"
        # Mapping: {target_sqlite_field: source_db_column_name}
        mapping_for_target = {"id_target": "src_id", "name_target": "src_name"}
        # Detailed schema for the target table (used by _validate_target_row via self.target_schema_info)
        detailed_target_schema = {
            "id_target": {"type": "INTEGER"},
            "name_target": {"type": "TEXT"}
        }
        # Simplified validation rules for the target table (used by _validate_target_row)
        schema_info_validation_for_target = {"required": [], "unique": []}

        results = self.importer.process_import_to_target(
            source_id, is_query, target_table, mapping_for_target,
            detailed_target_schema, schema_info_validation_for_target
        )

        self.assertEqual(results.total_rows_processed, 2)
        self.assertEqual(results.rows_inserted, 2) # Check if side_effect worked
        self.assertEqual(results.rows_skipped, 0)
        self.assertEqual(len(results.errors), 0)
        self.assertEqual(mock_read_data.call_count, 1)
        self.assertEqual(mock_map_row.call_count, 2)
        self.assertEqual(mock_validate.call_count, 2)
        self.assertEqual(mock_insert_data_to_target.call_count, 2) # Verify _insert_data_to_target was called
        self.assertEqual(self.importer.column_mapping, mapping_for_target)
        self.assertEqual(self.importer.target_schema_info, detailed_target_schema)

if __name__ == '__main__':
    unittest.main()
