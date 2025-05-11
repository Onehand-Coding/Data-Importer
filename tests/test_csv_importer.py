import sqlite3
import logging
import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from core.database import DatabaseManager
from core.importers.csv_importer import CSVImporter
from core.importers.base_importer import ImportResult

# Suppress noisy logging during tests unless debugging
logging.basicConfig(level=logging.WARNING)

class TestCSVImporter(unittest.TestCase):

    def setUp(self):
        """Creates a temporary directory and in-memory DB for each test."""
        self.temp_dir = TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_db.sqlite"
        # Use file-based DB for context manager testing, though memory works too
        self.db_manager = DatabaseManager(str(self.db_path))
        # No need to connect here, context manager in tests handles it

    def tearDown(self):
        """Cleans up the temporary directory."""
        self.temp_dir.cleanup()

    def _create_temp_csv(self, content: str, suffix='.csv') -> Path:
        """Helper to create a temporary CSV file."""
        # Use NamedTemporaryFile within the setUp temp directory
        with NamedTemporaryFile(mode='w', suffix=suffix, delete=False, dir=self.temp_dir.name) as f:
            f.write(content)
            return Path(f.name)

    def test_valid_csv_import(self):
        """Test importing a standard valid CSV file."""
        csv_content = """name,email,phone,company
John Doe,john@example.com,123456,Acme Inc
Jane Smith,jane@example.com,,Startup Co"""
        csv_path = self._create_temp_csv(csv_content)

        table_name = "contacts_valid"
        # Expected mapping (sanitized db_field: csv_header)
        mapping = {
            "name": "name",
            "email": "email",
            "phone": "phone",
            "company": "company"
        }
        # Expected schema (sanitized db_field: type)
        schema = {
            "name": "TEXT",
            "email": "TEXT UNIQUE", # Assume email is unique
            "phone": "TEXT",
            "company": "TEXT"
        }
        schema_info = {'unique': ['email']} # Info for validation


        with self.db_manager as db: # Use context manager
            importer = CSVImporter(db)
            # 1. Create table first
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            # 2. Perform import
            results: ImportResult = importer.process_import(csv_path, table_name, mapping, schema_info)

        # Verify results object
        self.assertEqual(results.total_rows_processed, 2)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(results.rows_skipped, 0)
        self.assertEqual(len(results.errors), 0)

        # Optionally verify database content
        with self.db_manager as db:
            cursor = db.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0] if cursor else 0
            self.assertEqual(count, 2)
            cursor = db.execute(f"SELECT email FROM {table_name} WHERE name = 'Jane Smith'")
            email = cursor.fetchone()[0] if cursor else None
            self.assertEqual(email, 'jane@example.com')


    def test_csv_missing_required_field_validation(self):
        """Test validation skips row if required field is missing (hypothetical)."""
        # NOTE: BaseImporter validation doesn't enforce required by default yet.
        # This test assumes 'name' becomes required via schema_info.
        csv_content = """name,email,phone
,missing.name@example.com,123456
Valid Name,valid@example.com,789012"""
        csv_path = self._create_temp_csv(csv_content)

        table_name = "contacts_req"
        mapping = {"name": "name", "email": "email", "phone": "phone"}
        schema = {"name": "TEXT", "email": "TEXT UNIQUE", "phone": "TEXT"}
        # Simulate 'name' being required for validation step
        schema_info = {'required': ['name'], 'unique': ['email']}

        with self.db_manager as db:
            importer = CSVImporter(db)
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results = importer.process_import(csv_path, table_name, mapping, schema_info)

        # Expect 1 skipped (missing name), 1 inserted
        self.assertEqual(results.total_rows_processed, 2)
        self.assertEqual(results.rows_inserted, 1)
        self.assertEqual(results.rows_skipped, 1)
        self.assertEqual(len(results.errors), 1)
        self.assertIn("Required field 'Name' is missing", results.errors[0]['error'])

    def test_csv_invalid_email_format_validation(self):
        """Test validation skips row with invalid email format."""
        csv_content = """name,email
Valid Name,valid@example.com
Invalid Email,invalid-email-format
Another Valid,another@sample.net"""
        csv_path = self._create_temp_csv(csv_content)

        table_name = "contacts_email_fmt"
        mapping = {"name": "name", "email": "email"}
        schema = {"name": "TEXT", "email": "TEXT UNIQUE"}
        schema_info = {'unique': ['email']} # Default validation checks email format

        with self.db_manager as db:
            importer = CSVImporter(db)
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results = importer.process_import(csv_path, table_name, mapping, schema_info)

        # Expect 1 skipped (bad email), 2 inserted
        self.assertEqual(results.total_rows_processed, 3)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(results.rows_skipped, 1)
        self.assertEqual(len(results.errors), 1)
        self.assertIn("Invalid format for field 'Email'", results.errors[0]['error'])


    def test_csv_duplicate_entry_unique_constraint(self):
        """Test database UNIQUE constraint skips duplicate email."""
        csv_content = """name,email
Alice,alice@example.com
Bob,bob@example.com
Charlie,alice@example.com""" # Duplicate email
        csv_path = self._create_temp_csv(csv_content)

        table_name = "contacts_dupe"
        mapping = {"name": "name", "email": "email"}
        schema = {"name": "TEXT", "email": "TEXT UNIQUE"} # UNIQUE constraint
        schema_info = {'unique': ['email']}

        with self.db_manager as db:
            importer = CSVImporter(db)
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results = importer.process_import(csv_path, table_name, mapping, schema_info)

        # Expect 1 skipped (duplicate email), 2 inserted
        self.assertEqual(results.total_rows_processed, 3)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(results.rows_skipped, 1)
        self.assertEqual(len(results.errors), 1)
        # Error message comes from BaseImporter._format_integrity_error
        self.assertIn("Skipped: Duplicate value for 'email'", results.errors[0]['error'])

        # Verify DB content
        with self.db_manager as db:
            cursor = db.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0] if cursor else 0
            self.assertEqual(count, 2) # Only Alice and Bob should be there


if __name__ == '__main__':
    unittest.main()
