import argparse
from pathlib import Path
from core.database import DatabaseManager
from core.importers.csv_importer import CSVImporter

def print_results(results: dict):
    """Print import results in a human-readable format."""
    print("\nImport Results:")
    print(f"  Total rows processed: {results['total']}")
    print(f"  Successfully inserted: {results['inserted']}")
    print(f"  Skipped rows: {results['skipped']}")

    if results['errors']:
        print("\nErrors encountered:")
        for error in results['errors']:
            print(f"  Row {error['row']}: {error['error']}")
            print(f"     Data: {error['data']}")

def main():
    parser = argparse.ArgumentParser(
        description="Import contact data from CSV to SQLite database"
    )
    parser.add_argument(
        "csv_file",
        help="Path to the CSV file to import"
    )
    parser.add_argument(
        "-d", "--database",
        default="contacts.db",
        help="Path to SQLite database file (default: contacts.db)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show detailed output"
    )

    args = parser.parse_args()

    if not Path(args.csv_file).exists():
        print(f"Error: File not found - {args.csv_file}")
        return

    print(f"Starting import from {args.csv_file} to {args.database}")

    with DatabaseManager(args.database) as db:
        db.create_tables()
        importer = CSVImporter(db)
        results = importer.import_from_file(Path(args.csv_file))
        print_results(results)

if __name__ == "__main__":
    main()
