# Content for: tests/test_csv_importer.py
import sqlite3
import logging
import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from core.database import DatabaseManager
from core.importers.csv_importer import CSVImporter
from core.importers.base_importer import ImportResult

logging.disable(logging.CRITICAL)

class TestCSVImporter(unittest.TestCase):

    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_csv_db.sqlite"
        self.db_manager = DatabaseManager(str(self.db_path))

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_temp_csv(self, content: str, suffix='.csv', encoding='utf-8') -> Path:
        with NamedTemporaryFile(mode='w', suffix=suffix, delete=False, dir=self.temp_dir.name, encoding=encoding) as f:
            f.write(content)
            return Path(f.name)

    def test_valid_csv_import(self):
        csv_content = """Name,Email,Phone,Company
John Doe,john@example.com,123456,Acme Inc
Jane Smith,jane@example.com,,Startup Co"""
        csv_path = self._create_temp_csv(csv_content)
        table_name = "contacts_valid_csv"
        mapping = {"name": "Name", "email": "Email", "phone": "Phone", "company": "Company"}
        schema = {"name": "TEXT", "email": "TEXT UNIQUE", "phone": "TEXT", "company": "TEXT"}
        schema_info_for_validation = {'unique': ['email'], 'required': ['name', 'email']}

        with self.db_manager as db:
            importer = CSVImporter(db)
            importer.set_table_schema_info({
                "name": {"required": True}, # For validate_mapped_row context
                "email": {"is_email": True, "unique": True}
            })
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results: ImportResult = importer.process_import(csv_path, table_name, mapping, schema_info_for_validation)

        self.assertEqual(results.total_rows_processed, 2)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(results.rows_skipped, 0, f"Expected 0 errors, got: {results.errors}")
        self.assertEqual(len(results.errors), 0)

        with self.db_manager as db:
            cursor = db.execute(f"SELECT COUNT(*) FROM {table_name}")
            self.assertIsNotNone(cursor)
            count = cursor.fetchone()[0]
            self.assertEqual(count, 2)
            cursor = db.execute(f"SELECT phone FROM {table_name} WHERE name = 'Jane Smith'")
            self.assertIsNotNone(cursor)
            phone_value = cursor.fetchone()[0]
            self.assertIsNone(phone_value, f"Expected phone for Jane Smith to be NULL, got '{phone_value}'") # Changed to assertIsNone

    def test_csv_missing_required_field_validation(self):
        csv_content = """Name,Email,Phone
,missing.name@example.com,123456
Valid Name,valid@example.com,789012"""
        csv_path = self._create_temp_csv(csv_content)
        table_name = "contacts_req_csv"
        mapping = {"name": "Name", "email": "Email", "phone": "Phone"}
        schema = {"name": "TEXT NOT NULL", "email": "TEXT UNIQUE", "phone": "TEXT"}
        schema_info_for_validation = {'required': ['name'], 'unique': ['email']}

        with self.db_manager as db:
            importer = CSVImporter(db)
            importer.set_table_schema_info({
                "name": {"required": True}, "email": {"is_email": True}
            })
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results = importer.process_import(csv_path, table_name, mapping, schema_info_for_validation)

        self.assertEqual(results.total_rows_processed, 2)
        self.assertEqual(results.rows_inserted, 1)
        self.assertEqual(results.rows_skipped, 1)
        self.assertEqual(len(results.errors), 1)
        # Expecting original cased 'Name' as per BaseImporter's original_header_name logic
        self.assertIn("Required field 'Name' is missing or empty.", results.errors[0]['error'])

    def test_csv_invalid_email_format_validation(self):
        csv_content = """Name,Email
Valid Name,valid@example.com
Invalid Email,invalid-email-format
Another Valid,another@sample.net"""
        csv_path = self._create_temp_csv(csv_content)
        table_name = "contacts_email_fmt_csv"
        mapping = {"name": "Name", "email": "Email"}
        schema = {"name": "TEXT", "email": "TEXT UNIQUE"}
        schema_info_for_validation = {'unique': ['email']}

        with self.db_manager as db:
            importer = CSVImporter(db)
            importer.set_table_schema_info({
                "email": {"is_email": True, "unique" : True} # For validate_mapped_row
            })
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results = importer.process_import(csv_path, table_name, mapping, schema_info_for_validation)

        self.assertEqual(results.total_rows_processed, 3)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(results.rows_skipped, 1)
        self.assertEqual(len(results.errors), 1)
        # Expecting original cased 'Email'
        self.assertIn("Invalid format for field 'Email': 'invalid-email-format'.", results.errors[0]['error'])

    def test_csv_duplicate_entry_unique_constraint(self):
        csv_content = """Name,Email
Alice,alice@example.com
Bob,bob@example.com
Charlie,alice@example.com"""
        csv_path = self._create_temp_csv(csv_content)
        table_name = "contacts_dupe_csv"
        mapping = {"name_db": "Name", "email_db": "Email"}
        schema = {"name_db": "TEXT", "email_db": "TEXT UNIQUE"}
        schema_info_for_validation = {'unique': ['email_db']}

        with self.db_manager as db:
            importer = CSVImporter(db)
            importer.set_table_schema_info({"email_db": {"unique": True}}) # For _format_integrity_error context
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results = importer.process_import(csv_path, table_name, mapping, schema_info_for_validation)

        self.assertEqual(results.total_rows_processed, 3)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(results.rows_skipped, 1)
        self.assertEqual(len(results.errors), 1)
        # _format_integrity_error should use the original mapped CSV header 'Email'
        self.assertIn("Skipped: Duplicate value for 'Email'", results.errors[0]['error'])
        self.assertEqual(results.errors[0]['row'], '3')

if __name__ == '__main__':
    unittest.main()
