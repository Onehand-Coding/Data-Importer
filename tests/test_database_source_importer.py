# tests/test_database_source_importer.py
import unittest
from unittest.mock import patch, MagicMock, ANY
import sqlite3
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect, text  # Ensure text is imported
from sqlalchemy.exc import (
    SQLAlchemyError,
    OperationalError as SQLAlchemyOperationalError,
    NoSuchTableError,
)
# from sqlalchemy.sql.elements import TextClause # Alternative for isinstance check

from data_importer.core.database import DatabaseManager
from data_importer.core.importers.database_source_importer import DatabaseSourceImporter
from data_importer.core.importers.base_importer import ImportResult

import logging

logging.disable(logging.CRITICAL)  # Disable logging for cleaner test output


class TestDatabaseSourceImporter(unittest.TestCase):
    def setUp(self):
        self.target_db_path = ":memory:"
        self.target_db_manager = DatabaseManager(self.target_db_path)
        self.assertTrue(
            self.target_db_manager.connect(),
            "Failed to connect to in-memory target DB for tests",
        )

        self.importer = DatabaseSourceImporter()

        self.mock_engine = MagicMock(spec=create_engine("sqlite:///:memory:").__class__)
        self.mock_connection = MagicMock()
        self.mock_inspector = MagicMock()

        connect_cm = MagicMock()
        connect_cm.__enter__.return_value = self.mock_connection
        connect_cm.__exit__.return_value = None
        self.mock_engine.connect.return_value = connect_cm
        self.mock_engine.name = "mock_db"

        self.create_engine_patcher = patch(
            "sqlalchemy.create_engine", return_value=self.mock_engine
        )
        self.mock_create_engine = self.create_engine_patcher.start()

        self.inspect_patcher = patch(
            "sqlalchemy.inspect", return_value=self.mock_inspector
        )
        self.mock_sqlalchemy_inspect = self.inspect_patcher.start()

    def tearDown(self):
        if self.importer and self.importer.source_engine:
            self.importer.close_source_connection()
        if self.target_db_manager and self.target_db_manager.connection:
            self.target_db_manager.close()

        self.create_engine_patcher.stop()
        self.inspect_patcher.stop()

    def test_initialization_success(self):
        self.assertIsNotNone(self.importer)
        self.assertIsNone(self.importer.source_engine)
        self.assertIsNone(self.importer.target_db_manager)

    def test_connect_to_source_success(self):
        conn_str = "sqlite:///:memory:"
        self.create_engine_patcher.stop()
        self.inspect_patcher.stop()

        real_engine = create_engine(conn_str)
        with patch(
            "sqlalchemy.create_engine", return_value=real_engine
        ) as mock_create_real, patch(
            "sqlalchemy.inspect", return_value=inspect(real_engine)
        ) as mock_inspect_real:
            self.assertTrue(self.importer.connect_to_source(conn_str))
            self.assertIsNotNone(self.importer.source_engine)
            self.assertEqual(self.importer.source_engine.name, "sqlite")
            self.assertIsNotNone(self.importer.source_inspector)

        self.create_engine_patcher.start()
        self.inspect_patcher.start()

    def test_connect_to_source_sqlalchemy_error_on_connect(self):
        self.mock_engine.connect.side_effect = SQLAlchemyError(
            "Simulated DB connection error"
        )
        self.assertFalse(self.importer.connect_to_source("postgresql://test"))
        self.assertIsNone(self.importer.source_engine)
        self.assertIsNone(self.importer.source_inspector)

    def test_connect_to_source_malformed_url_error(self):
        self.mock_create_engine.side_effect = Exception("Simulated malformed URL")
        self.assertFalse(self.importer.connect_to_source("badscheme://test"))
        self.assertIsNone(self.importer.source_engine)
        self.mock_create_engine.side_effect = None

    def test_close_source_connection(self):
        self.importer.source_engine = self.mock_engine
        self.importer.source_inspector = self.mock_inspector
        self.importer.close_source_connection()
        self.mock_engine.dispose.assert_called_once()
        self.assertIsNone(self.importer.source_engine)
        self.assertIsNone(self.importer.source_inspector)

    def test_close_source_connection_no_engine(self):
        self.importer.source_engine = None
        self.importer.close_source_connection()  # Should not raise error

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
        self.mock_inspector.get_table_names.side_effect = SQLAlchemyError(
            "Simulated error"
        )
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
        self.mock_engine.name = "unknown_dialect"  # Default case
        self.assertEqual(self.importer._get_quoted_identifier("myTable"), '"myTable"')

    def test_get_headers_from_source_table_success(self):
        self.importer.source_engine = self.mock_engine
        self.importer.source_inspector = self.mock_inspector
        self.mock_inspector.get_columns.return_value = [
            {"name": "col1"},
            {"name": "col2"},
        ]
        headers = self.importer.get_headers_from_source("test_table", is_query=False)
        self.assertEqual(headers, ["col1", "col2"])
        self.mock_inspector.get_columns.assert_called_with("test_table")

    def test_get_headers_from_source_query_success(self):
        self.importer.source_engine = self.mock_engine
        mock_result_proxy = MagicMock()
        mock_result_proxy.keys.return_value = ["headerA", "headerB"]
        self.mock_connection.execute.return_value = mock_result_proxy

        query = "SELECT a, b FROM test_table"
        headers = self.importer.get_headers_from_source(query, is_query=True)
        self.assertEqual(headers, ["headerA", "headerB"])
        self.mock_connection.execute.assert_called_once()
        executed_call_arg = self.mock_connection.execute.call_args[0][0]
        self.assertIsInstance(executed_call_arg, type(text("")))
        self.assertIn("LIMIT 1", str(executed_call_arg).upper())
        self.assertIn(
            query.upper(), str(executed_call_arg).upper()
        )  # Check original query is part of it

    @patch("pandas.read_sql_query")
    def test_get_preview_from_source_table(self, mock_pd_read_sql):
        self.importer.source_engine = self.mock_engine
        self.mock_engine.name = "sqlite"
        mock_df = pd.DataFrame({"col1": ["data1"], "col2": ["data2"]})
        mock_pd_read_sql.return_value = mock_df
        df_preview = self.importer.get_preview_from_source(
            "my_table", is_query=False, num_rows=1
        )

        mock_pd_read_sql.assert_called_once()
        called_sql_arg = mock_pd_read_sql.call_args[1][
            "sql"
        ]  # Get the 'sql' keyword argument
        self.assertIsInstance(called_sql_arg, type(text("")))
        self.assertEqual(str(called_sql_arg), 'SELECT * FROM "my_table" LIMIT 1')
        pd.testing.assert_frame_equal(df_preview, mock_df.astype(str))

    def test_read_data_from_source_table(self):
        self.importer.source_engine = self.mock_engine
        self.mock_engine.name = "postgresql"
        mock_row1 = MagicMock()
        mock_row1._asdict.return_value = {"id": 1, "name": "Alice"}
        mock_row2 = MagicMock()
        mock_row2._asdict.return_value = {"id": 2, "name": "Bob"}
        mock_result_proxy = MagicMock()
        mock_result_proxy.fetchmany.side_effect = [[mock_row1, mock_row2], []]
        self.mock_connection.execute.return_value = mock_result_proxy

        data = list(
            self.importer.read_data_from_source(
                "my_pg_table", is_query=False, chunk_size=50
            )
        )

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], {"id": 1, "name": "Alice"})
        self.mock_connection.execute.assert_called_once()
        executed_sql_arg = self.mock_connection.execute.call_args[0][0]
        self.assertIsInstance(executed_sql_arg, type(text("")))
        self.assertEqual(str(executed_sql_arg), 'SELECT * FROM "my_pg_table"')

    def test_map_source_row_to_target(self):
        self.importer.column_mapping = {
            "target_id": "src_id",
            "target_name": "src_name",
        }
        raw_row = {"src_id": 1, "src_name": " Test Name ", "extra_col": "ignore"}
        mapped = self.importer._map_source_row_to_target(raw_row)
        self.assertEqual(
            mapped, {"target_id": 1, "target_name": "Test Name"}
        )  # Trimmed

        raw_row_empty_str = {"src_id": 2, "src_name": "  "}  # Empty string after strip
        mapped_empty = self.importer._map_source_row_to_target(raw_row_empty_str)
        self.assertEqual(
            mapped_empty, {"target_id": 2, "target_name": None}
        )  # Empty becomes None

    def test_validate_target_row(self):
        self.importer.target_schema_info = {
            "email_col": {"is_email": True, "type": "TEXT UNIQUE"},
            "name_col": {"type": "TEXT", "required": True},
            "optional_col": {"type": "TEXT"},
        }
        self.importer.column_mapping = {
            "name_col": "Source Name",
            "email_col": "Source Email",
            "optional_col": "Source Optional",
        }
        schema_info_val_arg = {"required": ["name_col"], "unique": ["email_col"]}

        valid_row = {
            "name_col": "Alice",
            "email_col": "alice@example.com",
            "optional_col": "data",
        }
        is_valid, errors = self.importer._validate_target_row(
            valid_row, 1, schema_info_val_arg
        )
        self.assertTrue(is_valid, f"Validation failed for valid row: {errors}")
        self.assertEqual(len(errors), 0)

        missing_req_row = {"email_col": "bob@example.com"}
        is_valid, errors = self.importer._validate_target_row(
            missing_req_row, 2, schema_info_val_arg
        )
        self.assertFalse(is_valid)
        self.assertEqual(len(errors), 1)
        self.assertIn(
            "Required field 'Source Name' (target: 'name_col') for target table is missing or empty.",
            errors[0],
        )

        missing_req_row_none = {"name_col": None, "email_col": "bob@example.com"}
        is_valid, errors = self.importer._validate_target_row(
            missing_req_row_none, 2, schema_info_val_arg
        )
        self.assertFalse(is_valid)
        self.assertEqual(len(errors), 1)
        self.assertIn(
            "Required field 'Source Name' (target: 'name_col') for target table is missing or empty.",
            errors[0],
        )

        invalid_email_row = {"name_col": "Charlie", "email_col": "charlie.com"}
        is_valid, errors = self.importer._validate_target_row(
            invalid_email_row, 3, schema_info_val_arg
        )
        self.assertFalse(is_valid)
        self.assertEqual(len(errors), 1)
        self.assertIn(
            "Invalid email format for target field 'email_col' (from 'Source Email'): 'charlie.com'.",
            errors[0],
        )

    @patch.object(DatabaseSourceImporter, "_insert_data_to_target")
    @patch.object(
        DatabaseSourceImporter, "_validate_target_row", return_value=(True, [])
    )
    @patch.object(DatabaseSourceImporter, "_map_source_row_to_target")
    @patch.object(DatabaseSourceImporter, "read_data_from_source")
    def test_process_import_to_target_success_flow(
        self, mock_read_data, mock_map_row, mock_validate, mock_insert_data_to_target
    ):
        def insert_side_effect(
            sanitized_table_name, data_to_insert, results_obj, row_number
        ):
            results_obj.rows_inserted += 1
            return True

        mock_insert_data_to_target.side_effect = insert_side_effect

        mock_read_data.return_value = iter(
            [
                {"src_id": 1, "src_name": "Alice_Source"},
                {"src_id": 2, "src_name": "Bob_Source"},
            ]
        )
        mock_map_row.side_effect = [
            {"id_target": 1, "name_target": "Alice_Target"},
            {"id_target": 2, "name_target": "Bob_Target"},
        ]

        source_id = "my_source_table_or_query"
        is_query = False
        target_table = "target_sqlite_table"
        mapping_for_target = {"id_target": "src_id", "name_target": "src_name"}
        detailed_target_schema = {
            "id_target": {"type": "INTEGER"},
            "name_target": {"type": "TEXT"},
        }
        schema_info_validation_for_target = {"required": [], "unique": []}

        results = self.importer.process_import_to_target(
            self.target_db_manager,
            source_id,
            is_query,
            target_table,
            mapping_for_target,
            detailed_target_schema,
            schema_info_validation_for_target,
        )

        self.assertEqual(results.total_rows_processed, 2)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(mock_read_data.call_count, 1)
        self.assertEqual(mock_map_row.call_count, 2)
        self.assertEqual(mock_validate.call_count, 2)
        self.assertEqual(mock_insert_data_to_target.call_count, 2)

    def test_get_headers_from_source_table_not_found(self):
        self.importer.source_engine = self.mock_engine
        self.importer.source_inspector = self.mock_inspector
        self.mock_inspector.get_columns.side_effect = NoSuchTableError(
            "Simulated table not found"
        )
        with self.assertRaisesRegex(
            ValueError, "Could not get headers from source 'non_existent_table'"
        ):
            self.importer.get_headers_from_source("non_existent_table", is_query=False)
        self.mock_inspector.get_columns.assert_called_with("non_existent_table")

    def test_get_headers_from_source_table_inspector_returns_empty(self):
        self.importer.source_engine = self.mock_engine
        self.importer.source_inspector = self.mock_inspector
        self.mock_inspector.get_columns.return_value = []
        headers = self.importer.get_headers_from_source(
            "empty_columns_table", is_query=False
        )
        self.assertEqual(headers, [])

    def test_get_headers_from_source_query_returns_no_rows_but_keys_exist(self):
        self.importer.source_engine = self.mock_engine
        mock_result_proxy = MagicMock()
        mock_result_proxy.keys.return_value = ["colA", "colB"]
        self.mock_connection.execute.return_value = mock_result_proxy

        query = "SELECT colA, colB FROM test_table WHERE 1=0"
        headers = self.importer.get_headers_from_source(query, is_query=True)

        self.assertEqual(headers, ["colA", "colB"])
        self.mock_connection.execute.assert_called_once()
        executed_sql_text = self.mock_connection.execute.call_args[0][0]
        self.assertIsInstance(executed_sql_text, type(text("")))
        self.assertIn("LIMIT 1", str(executed_sql_text).upper())

    def test_read_data_from_source_empty_table(self):
        self.importer.source_engine = self.mock_engine
        self.mock_engine.name = "sqlite"

        mock_result_proxy = MagicMock()
        mock_result_proxy.fetchmany.return_value = []
        self.mock_connection.execute.return_value = mock_result_proxy

        data_iterator = self.importer.read_data_from_source(
            "empty_table", is_query=False, chunk_size=50
        )
        data = list(data_iterator)

        self.assertEqual(data, [])
        # --- Start of change for test_read_data_from_source_empty_table ---
        self.mock_connection.execute.assert_called_once()
        actual_arg = self.mock_connection.execute.call_args[0][0]
        self.assertIsInstance(
            actual_arg, type(text("")), "Argument should be a SQLAlchemy TextClause"
        )
        self.assertEqual(str(actual_arg), 'SELECT * FROM "empty_table"')
        # --- End of change for test_read_data_from_source_empty_table ---

    def test_read_data_from_source_empty_query_result(self):
        self.importer.source_engine = self.mock_engine

        mock_result_proxy = MagicMock()
        mock_result_proxy.fetchmany.return_value = []
        self.mock_connection.execute.return_value = mock_result_proxy

        query = "SELECT * FROM my_table WHERE 1=0"
        data_iterator = self.importer.read_data_from_source(
            query, is_query=True, chunk_size=50
        )
        data = list(data_iterator)

        self.assertEqual(data, [])
        # --- Start of change for test_read_data_from_source_empty_query_result ---
        self.mock_connection.execute.assert_called_once()
        actual_arg = self.mock_connection.execute.call_args[0][0]
        self.assertIsInstance(
            actual_arg, type(text("")), "Argument should be a SQLAlchemy TextClause"
        )
        self.assertEqual(str(actual_arg), query)
        # --- End of change for test_read_data_from_source_empty_query_result ---

    def test_map_source_row_to_target_empty_mapping(self):
        self.importer.column_mapping = {}
        raw_row = {"src_id": 1, "src_name": " Test Name "}
        # With target_db_manager=None on importer, and empty mapping, it returns the raw row
        mapped = self.importer._map_source_row_to_target(raw_row)
        self.assertEqual(mapped, {"src_id": 1, "src_name": " Test Name "})

    def test_map_source_row_to_target_source_key_missing_in_row(self):
        self.importer.column_mapping = {
            "target_id": "src_id",
            "target_name": "src_name_missing",
        }
        raw_row = {"src_id": 1, "another_field": "data"}
        mapped = self.importer._map_source_row_to_target(raw_row)
        self.assertEqual(mapped, {"target_id": 1, "target_name": None})

    def test_validate_target_row_multiple_errors(self):
        self.importer.target_schema_info = {
            "email_col": {"is_email": True, "type": "TEXT UNIQUE"},
            "name_col": {"type": "TEXT", "required": True},
            "age_col": {"type": "INTEGER", "required": True},
        }
        self.importer.column_mapping = {
            "email_col": "SourceEmail",
            "name_col": "SourceName",
            "age_col": "SourceAge",
        }
        schema_info_val_arg = {
            "required": ["name_col", "age_col"],
            "unique": ["email_col"],
        }
        invalid_row = {
            "email_col": "not-an-email"
        }  # Missing name_col and age_col, invalid email
        is_valid, errors = self.importer._validate_target_row(
            invalid_row, 1, schema_info_val_arg
        )

        self.assertFalse(is_valid)
        self.assertEqual(len(errors), 3)  # Expecting 3 errors
        expected_errors = [
            "Invalid email format for target field 'email_col' (from 'SourceEmail'): 'not-an-email'.",
            "Required field 'SourceAge' (target: 'age_col') for target table is missing or empty.",
            "Required field 'SourceName' (target: 'name_col') for target table is missing or empty.",
        ]
        for err_msg in expected_errors:
            self.assertIn(err_msg, errors)

    def test_process_import_to_target_target_db_manager_none(self):
        with self.assertRaisesRegex(
            ValueError,
            "Target DatabaseManager instance cannot be None for import process.",
        ):
            self.importer.process_import_to_target(
                None, "source_table", False, "target_table", {}, {}, {}
            )

    @patch.object(DatabaseSourceImporter, "read_data_from_source")
    def test_process_import_to_target_no_data_from_source(self, mock_read_data):
        mock_read_data.return_value = iter([])  # Simulate no data
        results = self.importer.process_import_to_target(
            self.target_db_manager,
            "empty_source",
            False,
            "target_sqlite_table",
            {"target_col": "src_col"},
            {"target_col": {"type": "TEXT"}},
            {"required": [], "unique": []},
        )
        self.assertEqual(results.total_rows_processed, 0)
        self.assertEqual(results.rows_inserted, 0)
        mock_read_data.assert_called_once()

    @patch.object(DatabaseSourceImporter, "_insert_data_to_target")
    @patch.object(
        DatabaseSourceImporter,
        "_validate_target_row",
        return_value=(False, ["Validation error"]),
    )
    @patch.object(
        DatabaseSourceImporter,
        "_map_source_row_to_target",
        return_value={"mapped_col": "mapped_value"},
    )
    @patch.object(DatabaseSourceImporter, "read_data_from_source")
    def test_process_import_to_target_all_rows_skipped_validation(
        self, mock_read_data, mock_map_row, mock_validate, mock_insert_data
    ):
        mock_read_data.return_value = iter([{"src_id": 1}, {"src_id": 2}])

        target_table_name = "target_table_for_skip_test"
        self.target_db_manager.create_dynamic_table(
            target_table_name, {"mapped_col": "TEXT"}
        )
        dummy_schema_info_for_validation = {"required": ["mapped_col"], "unique": []}

        results = self.importer.process_import_to_target(
            self.target_db_manager,
            "source_data",
            False,
            target_table_name,
            {"mapped_col": "src_id"},
            {"mapped_col": {"type": "TEXT"}},
            dummy_schema_info_for_validation,
        )

        self.assertEqual(results.total_rows_processed, 2)
        self.assertEqual(results.rows_skipped, 2)
        self.assertEqual(len(results.errors), 2)
        self.assertIn(
            {
                "row": "1",
                "error": "Validation error",
                "data": '{"mapped_col": "mapped_value"}',
            },
            results.errors,
        )
        mock_insert_data.assert_not_called()


if __name__ == "__main__":
    unittest.main()
