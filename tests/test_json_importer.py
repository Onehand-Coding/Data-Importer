import json
import logging
import unittest
from pathlib import Path
from typing import Any
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pandas as pd

from data_importer.core.database import DatabaseManager
from data_importer.core.importers.json_importer import JSONImporter
from data_importer.core.importers.base_importer import ImportResult

# Suppress noisy logging during tests unless debugging
logging.basicConfig(level=logging.WARNING)


class TestJSONImporter(unittest.TestCase):
    def setUp(self):
        """Creates a temporary directory and in-memory DB for each test."""
        self.temp_dir = TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_json_db.sqlite"
        self.db_manager = DatabaseManager(str(self.db_path))

    def tearDown(self):
        """Cleans up the temporary directory."""
        self.temp_dir.cleanup()

    # Use 'Any' type hint here now that it's imported
    def _create_temp_json(self, data: Any, suffix=".json") -> Path:
        """Helper to create a temporary JSON file."""
        # Ensure file is written with UTF-8 encoding
        with NamedTemporaryFile(
            mode="w",
            suffix=suffix,
            delete=False,
            dir=self.temp_dir.name,
            encoding="utf-8",
        ) as f:
            json.dump(
                data, f, ensure_ascii=False
            )  # ensure_ascii=False for wider character support
            return Path(f.name)

    def test_get_headers_valid_json_list(self):
        """Test reading headers from a valid JSON list of objects."""
        json_data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "city": "New York"},  # Missing email, extra city
        ]
        json_path = self._create_temp_json(json_data)
        # Use context manager for db connection safety
        with DatabaseManager(str(self.db_path)) as db:
            importer = JSONImporter(db)
            headers = importer.get_headers(json_path)
        # Headers should be sorted union of all keys
        self.assertEqual(headers, ["city", "email", "id", "name"])

    def test_get_headers_empty_list(self):
        """Test reading headers from an empty JSON list."""
        json_data = []
        json_path = self._create_temp_json(json_data)
        with DatabaseManager(str(self.db_path)) as db:
            importer = JSONImporter(db)
            headers = importer.get_headers(json_path)
        self.assertEqual(headers, [])

    def test_get_headers_invalid_json_format(self):
        """Test reading headers from invalid JSON."""
        json_path = Path(self.temp_dir.name) / "invalid.json"
        # Create invalid JSON file content
        with open(json_path, "w", encoding="utf-8") as f:
            f.write("[{'id': 1},")  # Invalid JSON syntax
        # Test within context manager
        with DatabaseManager(str(self.db_path)) as db:
            importer = JSONImporter(db)
            # Use assertRaisesRegex for more specific error checking
            with self.assertRaisesRegex(ValueError, "Invalid JSON format"):
                importer.get_headers(json_path)

    def test_get_headers_not_a_list(self):
        """Test reading headers from JSON that isn't a list of objects."""
        json_data = {"record1": {"id": 1}, "record2": {"id": 2}}  # Dict, not list
        json_path = self._create_temp_json(json_data)
        with DatabaseManager(str(self.db_path)) as db:
            importer = JSONImporter(db)
            # Use assertRaisesRegex for more specific error checking
            with self.assertRaisesRegex(
                ValueError, "JSON file must contain a list of objects"
            ):
                importer.get_headers(json_path)

    def test_get_preview(self):
        """Test generating a preview DataFrame."""
        json_data = [
            {"id": 1, "name": "Alice", "value": 10.5},
            {"id": 2, "name": "Bob", "value": 20.0},
            {"id": 3, "name": "Charlie", "extra": "data"},
        ]
        json_path = self._create_temp_json(json_data)
        with DatabaseManager(str(self.db_path)) as db:
            importer = JSONImporter(db)
            df_preview = importer.get_preview(json_path, num_rows=2)

        self.assertIsInstance(df_preview, pd.DataFrame)
        self.assertEqual(len(df_preview), 2)
        # Check columns are the sorted union of keys based on get_headers logic
        self.assertListEqual(
            df_preview.columns.tolist(), ["extra", "id", "name", "value"]
        )
        # Check first row data (as strings)
        self.assertEqual(df_preview.iloc[0]["id"], "1")
        self.assertEqual(df_preview.iloc[0]["name"], "Alice")
        self.assertEqual(df_preview.iloc[0]["value"], "10.5")
        self.assertEqual(
            df_preview.iloc[0]["extra"], ""
        )  # Check filled value for missing key

    def test_read_data(self):
        """Test the data reading generator."""
        json_data = [
            {"colA": "val1", "colB": 100},
            {"colA": "val2", "colB": 200, "colC": True},
        ]
        json_path = self._create_temp_json(json_data)
        rows = []
        # Use context manager for db
        with DatabaseManager(str(self.db_path)) as db:
            importer = JSONImporter(db)
            # Consume the generator
            rows = list(importer.read_data(json_path))

        self.assertEqual(len(rows), 2)
        self.assertDictEqual(rows[0], {"colA": "val1", "colB": 100})
        self.assertDictEqual(rows[1], {"colA": "val2", "colB": 200, "colC": True})

    def test_full_import_process(self):
        """Test the complete import process using process_import."""
        json_data = [
            {"user_id": 101, "username": "tester1", "status": "active"},
            {"user_id": 102, "username": "tester2", "status": "inactive"},
            {"user_id": 103, "username": "tester3", "status": "active"},
        ]
        json_path = self._create_temp_json(json_data)
        table_name = "users_json"
        # Mapping: target_db_field -> source_json_key
        mapping = {"id_num": "user_id", "name": "username", "state": "status"}
        # Schema: target_db_field -> type
        schema = {"id_num": "INTEGER", "name": "TEXT", "state": "TEXT"}
        schema_info = {}  # No specific validation rules for this test

        # Use context manager for db operations
        with DatabaseManager(str(self.db_path)) as db:
            importer = JSONImporter(db)
            # 1. Create table
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            # 2. Perform import
            results: ImportResult = importer.process_import(
                json_path, table_name, mapping, schema_info
            )

            # Verify results object immediately after import within the same context if possible
            self.assertEqual(results.total_rows_processed, 3)
            self.assertEqual(results.rows_inserted, 3)
            self.assertEqual(results.rows_skipped, 0)
            self.assertEqual(len(results.errors), 0)

            # Verify database content within the same context
            cursor = db.execute(
                f'SELECT name, state FROM "{table_name}" WHERE id_num = 102'
            )
            row = cursor.fetchone() if cursor else None
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "tester2")  # Name column
            self.assertEqual(row[1], "inactive")  # State column


if __name__ == "__main__":
    unittest.main()
