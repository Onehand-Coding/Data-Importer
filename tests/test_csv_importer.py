import unittest
import sqlite3
from pathlib import Path
from tempfile import NamedTemporaryFile
from core.database import DatabaseManager
from core.importers.csv_importer import CSVImporter

class TestCSVImporter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a test database in memory
        cls.db_manager = DatabaseManager(":memory:")
        cls.db_manager.connect()
        cls.db_manager.create_tables()
        
    def setUp(self):
        # Clear tables before each test
        self.db_manager.execute("DELETE FROM contacts", commit=True)
        
    def test_valid_csv_import(self):
        # Create a test CSV file
        csv_content = """name,email,phone,company
John Doe,john@example.com,123456,Acme Inc
Jane Smith,jane@example.com,,Startup Co"""
        
        with NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)
        
        # Perform import
        importer = CSVImporter(self.db_manager)
        results = importer.import_from_file(csv_path)
        
        # Verify results
        self.assertEqual(results['total'], 2)
        self.assertEqual(results['inserted'], 2)
        self.assertEqual(results['skipped'], 0)
        
        # Clean up
        csv_path.unlink()
        
    def test_invalid_csv(self):
        # CSV missing required email field
        csv_content = """name,phone,company
John Doe,123456,Acme Inc"""
        
        with NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            csv_path = Path(f.name)
        
        importer = CSVImporter(self.db_manager)
        results = importer.import_from_file(csv_path)
        
        self.assertEqual(results['inserted'], 0)
        self.assertTrue(len(results['errors']) > 0)
        
        csv_path.unlink()

if __name__ == '__main__':
    unittest.main()
