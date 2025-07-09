# Content for: tests/test_excel_importer.py
import unittest
import logging
import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
import openpyxl

from data_importer.core.database import DatabaseManager
from data_importer.core.importers.excel_importer import ExcelImporter
from data_importer.core.importers.base_importer import ImportResult

logging.disable(logging.CRITICAL)


class TestExcelImporter(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_excel_db.sqlite"
        self.db_manager = DatabaseManager(str(self.db_path))

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_temp_xlsx(
        self, data_rows_with_headers: list[list[any]], file_name: str = "test.xlsx"
    ) -> Path:
        file_path = Path(self.temp_dir.name) / file_name
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        if not data_rows_with_headers:
            pass
        else:
            for row_data in data_rows_with_headers:
                sheet.append(row_data)
        workbook.save(file_path)
        return file_path

    def test_get_headers_valid_excel(self):
        data = [["Name", "Email", "Age"], ["Alice", "alice@example.com", 30]]
        xlsx_path = self._create_temp_xlsx(data)
        with self.db_manager as db:
            importer = ExcelImporter(db)
            headers = importer.get_headers(xlsx_path)
            self.assertEqual(headers, ["Name", "Email", "Age"])

    def test_get_headers_empty_file(self):
        xlsx_path = self._create_temp_xlsx([])
        with self.db_manager as db:
            importer = ExcelImporter(db)
            with self.assertRaisesRegex(
                ValueError, "appears to have no headers or is empty"
            ):
                importer.get_headers(xlsx_path)

    def test_get_headers_header_only_file(self):
        data = [["Name", "Email", "Age"]]
        xlsx_path = self._create_temp_xlsx(data)
        with self.db_manager as db:
            importer = ExcelImporter(db)
            headers = importer.get_headers(xlsx_path)
            self.assertEqual(headers, ["Name", "Email", "Age"])

    def test_get_preview_valid_excel(self):
        data = [
            ["Name", "Email", "Value"],
            ["Alice", "a@ex.com", 100],
            ["Bob", "b@ex.com", 200.50],
        ]
        xlsx_path = self._create_temp_xlsx(data)
        with self.db_manager as db:
            importer = ExcelImporter(db)
            preview_df = importer.get_preview(
                xlsx_path, num_rows=2
            )  # Corrected num_rows to match data
            self.assertEqual(len(preview_df), 2)
            self.assertListEqual(list(preview_df.columns), ["Name", "Email", "Value"])
            self.assertEqual(preview_df.iloc[1]["Value"], "200.5")
            self.assertEqual(preview_df.iloc[0]["Name"], "Alice")

    def test_read_data_valid_excel(self):
        data = [["Col1", "Col2"], ["Data1A", 123], ["Data2A", 456.789]]
        xlsx_path = self._create_temp_xlsx(data)
        with self.db_manager as db:
            importer = ExcelImporter(db)
            rows = list(importer.read_data(xlsx_path))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0], {"Col1": "Data1A", "Col2": 123})
            self.assertEqual(rows[1], {"Col1": "Data2A", "Col2": 456.789})

    def test_read_data_header_only(self):
        data = [["ID", "Product"]]
        xlsx_path = self._create_temp_xlsx(data)
        with self.db_manager as db:
            importer = ExcelImporter(db)
            rows = list(importer.read_data(xlsx_path))
            self.assertEqual(len(rows), 0)

    def test_process_import_valid_excel(self):
        data = [
            ["full_name", "email", "age"],
            ["Alice", "alice@example.com", 30.0],
            ["Bob", "bob@example.com", 25.5],
        ]
        xlsx_path = self._create_temp_xlsx(data, "import_excel.xlsx")
        table_name = "excel_import_ok"
        mapping = {"name": "full_name", "email_addr": "email", "age_val": "age"}
        schema = {"name": "TEXT", "email_addr": "TEXT UNIQUE", "age_val": "REAL"}
        schema_info_for_validation = {"unique": ["email_addr"], "required": ["name"]}

        with self.db_manager as db:
            importer = ExcelImporter(db)
            importer.set_table_schema_info(
                {
                    "name": {"required": True},
                    "email_addr": {"is_email": True, "unique": True},
                    "age_val": {"type": "REAL"},
                }
            )
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results: ImportResult = importer.process_import(
                xlsx_path, table_name, mapping, schema_info_for_validation
            )

        self.assertEqual(results.total_rows_processed, 2)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(results.rows_skipped, 0, f"Errors: {results.errors}")
        self.assertEqual(len(results.errors), 0)

        with self.db_manager as db:
            cursor = db.execute(
                f"SELECT name, email_addr, age_val FROM {table_name} ORDER BY name"
            )
            self.assertIsNotNone(cursor)
            db_rows = cursor.fetchall()
            self.assertEqual(db_rows[0], ("Alice", "alice@example.com", 30.0))
            self.assertEqual(db_rows[1], ("Bob", "bob@example.com", 25.5))

    def test_process_import_duplicate_email_xlsx(self):
        data = [
            ["Name", "Email"],
            ["Alice", "common@example.com"],
            ["Bob", "bob@example.com"],
            ["Charlie", "common@example.com"],
        ]
        xlsx_path = self._create_temp_xlsx(data, "dupe_email.xlsx")
        table_name = "excel_dupes"
        mapping = {"name_db": "Name", "email_db": "Email"}
        schema = {"name_db": "TEXT", "email_db": "TEXT UNIQUE"}
        schema_info_for_validation = {"unique": ["email_db"]}

        with self.db_manager as db:
            importer = ExcelImporter(db)
            importer.set_table_schema_info(
                {"email_db": {"unique": True}}
            )  # For _format_integrity_error context
            self.assertTrue(db.create_dynamic_table(table_name, schema))
            results = importer.process_import(
                xlsx_path, table_name, mapping, schema_info_for_validation
            )

        self.assertEqual(results.total_rows_processed, 3)
        self.assertEqual(results.rows_inserted, 2)
        self.assertEqual(results.rows_skipped, 1)
        self.assertEqual(len(results.errors), 1)
        # _format_integrity_error uses the original cased header "Email" via mapping.
        self.assertIn(
            "Skipped: Duplicate value for 'Email'", results.errors[0]["error"]
        )
        self.assertEqual(results.errors[0]["row"], "3")


if __name__ == "__main__":
    unittest.main()
