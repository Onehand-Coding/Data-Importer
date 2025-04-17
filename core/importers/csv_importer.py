# core/importers/csv_importer.py

import csv
import logging
from pathlib import Path
from typing import Dict, List, Any, Generator, Optional
import pandas as pd
from .base_importer import BaseImporter, ImportResult

class CSVImporter(BaseImporter):
    """Importer implementation for CSV files."""
    SUPPORTED_EXTENSIONS = ['.csv']

    def get_headers(self, file_path: Path) -> List[str]:
        """Reads CSV headers."""
        logging.info(f"Reading headers from CSV: {file_path}")
        try:
            with file_path.open('r', encoding='utf-8-sig', newline='') as csvfile:
                # Use Sniffer to handle various dialects, though less reliable
                # dialect = csv.Sniffer().sniff(csvfile.read(1024*10))
                # csvfile.seek(0)
                # reader = csv.reader(csvfile, dialect)
                reader = csv.reader(csvfile) # Assume standard comma-separated for now
                raw_headers = next(reader)
                headers = [h.strip() for h in raw_headers if h and h.strip()]
                if not headers:
                     raise ValueError("CSV file appears to have no headers or is empty.")
                logging.info(f"Found headers: {headers}")
                return headers
        except StopIteration:
             raise ValueError("CSV file appears to be empty.")
        except Exception as e:
            logging.exception(f"Failed to read headers from {file_path}: {e}")
            raise # Re-raise the exception

    def get_preview(self, file_path: Path, num_rows: int = 5) -> pd.DataFrame:
        """Reads first N rows of CSV for preview using pandas."""
        logging.info(f"Generating preview ({num_rows} rows) for CSV: {file_path}")
        try:
            # Use pandas for robust preview generation
            df = pd.read_csv(
                file_path,
                nrows=num_rows,
                encoding='utf-8-sig',
                engine='python' # Often more robust for tricky CSVs
            )
            # Convert to display-friendly types if needed, handle NaNs
            return df.fillna('').astype(str)
        except Exception as e:
            logging.exception(f"Failed to generate preview for {file_path}: {e}")
            raise

    def read_data(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """Reads CSV data row by row using csv.DictReader."""
        logging.info(f"Reading data rows from CSV: {file_path}")
        try:
            with file_path.open('r', encoding='utf-8-sig', newline='') as csvfile:
                # DictReader uses the first row as headers by default
                reader = csv.DictReader(csvfile)
                if not reader.fieldnames:
                     logging.warning(f"CSV file {file_path} has no field names (headers).")
                     return # Yield nothing if no headers

                cleaned_fieldnames = [h.strip() for h in reader.fieldnames]
                reader.fieldnames = cleaned_fieldnames # Use cleaned names

                for row_dict in reader:
                     # Clean keys just in case they have odd spacing from DictReader
                     yield {k.strip(): v for k, v in row_dict.items()}

        except Exception as e:
            logging.exception(f"Failed during data reading from {file_path}: {e}")
            raise # Re-raise

    # --- Override validation if CSV needs specific checks ---
    # def validate_mapped_row(self, mapped_row: Dict[str, Any], row_number: Optional[int], schema_info: Optional[Dict] = None) -> Tuple[bool, List[str]]:
    #     is_valid, errors = super().validate_mapped_row(mapped_row, row_number, schema_info)
    #     # Add CSV-specific validation if needed
    #     # e.g., check for comma counts or specific encoding issues? Usually not needed here.
    #     return is_valid, errors

    # --- We no longer need import_dynamic or _insert_dynamic_data here ---
    # --- The base class process_import orchestrates everything ---
    # --- and calls self.insert_data (defined in base class) ---

    # Keep this only if needed for compatibility, but ideally remove
    def import_from_file(self, file_path: Path, *args, **kwargs) -> Dict[str, Any]:
        """ Deprecated. Use process_import via factory. """
        raise NotImplementedError("Direct use of import_from_file is deprecated. Use the factory/process_import.")
