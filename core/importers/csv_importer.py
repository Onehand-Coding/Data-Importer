import csv
import logging
from pathlib import Path
from typing import Dict, List, Any, Generator, Optional

import pandas as pd

from .base_importer import BaseImporter, ImportResult

# Configure logging for this module
logger = logging.getLogger(__name__)

class CSVImporter(BaseImporter):
    """Importer implementation for CSV files."""
    SUPPORTED_EXTENSIONS = ['.csv']

    def get_headers(self, file_path: Path) -> List[str]:
        """Reads CSV headers."""
        logger.info(f"Reading headers from CSV: {file_path}")
        try:
            # Use pandas for more robust header reading, handling various encodings/delimiters better
            # Read only the first row to get headers quickly
            df_header = pd.read_csv(
                file_path,
                nrows=0, # Read zero rows, only headers
                encoding='utf-8-sig', # Handle BOM
                engine='python' # Often better for tricky CSVs
            )
            headers = [str(h).strip() for h in df_header.columns if str(h).strip()]
            if not headers:
                 # Fallback or try standard csv reader if pandas fails
                 try:
                     with file_path.open('r', encoding='utf-8-sig', newline='') as csvfile:
                         reader = csv.reader(csvfile)
                         raw_headers = next(reader)
                         headers = [h.strip() for h in raw_headers if h and h.strip()]
                 except StopIteration:
                     raise ValueError("CSV file appears to be empty (could not read headers).")
                 except Exception as csv_err:
                      logger.error(f"CSV reader fallback failed for headers: {csv_err}")
                      raise ValueError("Could not determine headers from CSV file.") from csv_err

            if not headers:
                raise ValueError("CSV file appears to have no valid headers or is empty.")

            logger.info(f"Found headers: {headers}")
            return headers
        except pd.errors.EmptyDataError:
             raise ValueError("CSV file appears to be empty.")
        except Exception as e:
            logger.exception(f"Failed to read headers from {file_path}: {e}")
            raise # Re-raise the exception

    def get_preview(self, file_path: Path, num_rows: int = 5) -> pd.DataFrame:
        """Reads first N rows of CSV for preview using pandas."""
        logger.info(f"Generating preview ({num_rows} rows) for CSV: {file_path}")
        try:
            # Use pandas for robust preview generation
            df = pd.read_csv(
                file_path,
                nrows=num_rows,
                encoding='utf-8-sig',
                engine='python', # Often more robust for tricky CSVs
                keep_default_na=False # Treat empty strings as empty strings, not NaN
            )
            # Convert all to string for consistent preview display
            return df.astype(str)
        except pd.errors.EmptyDataError:
             # Return empty DataFrame if the file is empty but has headers
             logger.warning(f"Preview requested for empty file (or only headers): {file_path}")
             try: # Try to get headers to build empty frame
                 headers = self.get_headers(file_path)
                 return pd.DataFrame(columns=headers)
             except: # If headers fail too, return totally empty frame
                 return pd.DataFrame()
        except Exception as e:
            logger.exception(f"Failed to generate preview for {file_path}: {e}")
            raise

    def read_data(self, file_path: Path) -> Generator[Dict[str, Any], None, None]:
        """Reads CSV data row by row using csv.DictReader."""
        logger.info(f"Reading data rows from CSV: {file_path}")
        try:
            with file_path.open('r', encoding='utf-8-sig', newline='') as csvfile:
                # Use Sniffer to detect dialect (delimiter, quote char etc.) for robustness
                try:
                    sample = csvfile.read(2048) # Read a sample for sniffing
                    dialect = csv.Sniffer().sniff(sample)
                    csvfile.seek(0) # Rewind after sniffing
                    reader = csv.DictReader(csvfile, dialect=dialect)
                    logger.info(f"Detected CSV dialect: delimiter='{dialect.delimiter}', quotechar='{dialect.quotechar}'")
                except csv.Error:
                    logger.warning("Could not detect CSV dialect, falling back to standard comma delimiter.")
                    csvfile.seek(0)
                    reader = csv.DictReader(csvfile) # Fallback to default

                if not reader.fieldnames:
                     logger.warning(f"CSV file {file_path} has no field names (headers).")
                     return # Yield nothing if no headers

                # Clean fieldnames obtained from DictReader
                cleaned_fieldnames = [h.strip() for h in reader.fieldnames if h is not None] # Handle potential None headers
                reader.fieldnames = cleaned_fieldnames # Use cleaned names

                row_count = 0
                for row_dict in reader:
                     row_count += 1
                     # Clean keys and values just in case they have odd spacing
                     # Also handle potential None keys read by DictReader in rare cases
                     cleaned_row = {}
                     for k, v in row_dict.items():
                          if k is not None: # Skip None keys
                             cleaned_key = k.strip()
                             cleaned_value = v.strip() if isinstance(v, str) else v
                             cleaned_row[cleaned_key] = cleaned_value
                          else:
                             logger.warning(f"Row {row_count+1}: Found unexpected None key in DictReader output.")

                     yield cleaned_row

        except Exception as e:
            logger.exception(f"Failed during data reading from {file_path}: {e}")
            raise # Re-raise

    # Keep this only if needed for compatibility, but ideally remove
    def import_from_file(self, file_path: Path, *args, **kwargs) -> Dict[str, Any]:
        """ Deprecated. Use process_import via factory. """
        # This method is no longer used by the updated CLI or tests.
        # It could be removed entirely or kept just for legacy reference.
        # For safety, let's keep the NotImplementedError.
        raise NotImplementedError("Direct use of import_from_file is deprecated. Use the factory/process_import.")
