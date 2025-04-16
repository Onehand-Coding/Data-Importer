import csv
from pathlib import Path
from typing import Dict, Any
from io import TextIOWrapper
from .base_importer import BaseImporter

class CSVImporter(BaseImporter):
    """Handles CSV file imports."""

    def import_from_file(self, file_path: Path) -> Dict[str, Any]:
        """Import data from a CSV file."""
        results = {
            'total': 0,
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': self.validation_errors
        }

        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                return self._import_from_file_handle(csvfile, results)
        except Exception as e:
            self.log_error(0, f"File error: {str(e)}", {})
            results['errors'] = self.validation_errors
            return results

    def _import_from_file_handle(self, file_handle: TextIOWrapper, results: Dict[str, Any]) -> Dict[str, Any]:
        reader = csv.DictReader(file_handle)
        if not reader.fieldnames:
            self.log_error(0, "Empty CSV file or missing headers", {})
            return results

        required_fields = {'name', 'email'}
        if not required_fields.issubset(set(reader.fieldnames)):
            self.log_error(0, f"CSV missing required fields: {required_fields}", {})
            return results

        for row_num, row in enumerate(reader, start=1):
            results['total'] += 1
            try:
                # Clean row data
                cleaned_row = {k: v.strip() if isinstance(v, str) else v for k, v in row.items()}

                # Validate
                is_valid, errors = self.validate_row(cleaned_row)
                if not is_valid:
                    for error in errors:
                        self.log_error(row_num, error, cleaned_row)
                    results['skipped'] += 1
                    continue

                # Insert into database
                if self._insert_contact(cleaned_row):
                    results['inserted'] += 1

            except Exception as e:
                self.log_error(row_num, f"Processing error: {str(e)}", row)
                results['skipped'] += 1

        return results

    def _insert_contact(self, contact_data: Dict[str, Any]) -> bool:
        """Insert a single contact into the database."""
        sql = """
        INSERT INTO contacts (name, email, phone, company)
        VALUES (:name, :email, :phone, :company)
        """
        try:
            self.db_manager.execute(sql, contact_data, commit=True)
            return True
        except sqlite3.IntegrityError as e:
            self.log_error(0, f"Duplicate email: {contact_data['email']}", contact_data)
            return False
