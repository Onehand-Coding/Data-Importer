import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Iterator

import pandas as pd

from data_importer.core.database import DatabaseManager
from .base_importer import BaseImporter, ImportResult

# Configure logging for this module
logger = logging.getLogger(__name__)

# Attempt to import is_valid_email from a common utility module,
# otherwise use a local placeholder.
try:
    from core.utils.common_utils import is_valid_email
except ImportError:
    logger.info(
        "core.utils.common_utils.is_valid_email not found. Using a basic placeholder for email validation."
    )

    def is_valid_email(email: str) -> bool:
        """Basic email validation placeholder."""
        if email and isinstance(email, str):
            parts = email.split("@")
            if len(parts) == 2 and parts[0] and parts[1]:
                domain_parts = parts[1].split(".")
                if len(domain_parts) >= 2 and all(dp for dp in domain_parts):
                    return True
        return False


class JSONImporter(BaseImporter):
    """
    Importer for JSON files.
    The importer expects the JSON file to contain a list of objects (dictionaries).
    Each object in the list represents a row of data.
    """

    def __init__(self, db_manager: DatabaseManager, encoding: str = "utf-8"):
        """
        Initializes the JSONImporter.

        Args:
            db_manager (DatabaseManager): The database manager instance.
            encoding (str): The file encoding to use. Defaults to 'utf-8'.
        """
        super().__init__(db_manager)
        self.data: Optional[List[Dict[str, Any]]] = None
        self.encoding: str = encoding
        self.file_path_loaded: Optional[Path] = (
            None  # Tracks the path of the currently loaded file data
        )
        logger.info(f"JSONImporter initialized with encoding '{self.encoding}'.")

    def _ensure_data_loaded(self, file_path: Path) -> None:
        """
        Ensures that data from the specified file_path is loaded into `self.data`.
        If the requested file_path is different from the already loaded one,
        or if data hasn't been loaded yet, it calls `_load_json_data`.

        Args:
            file_path (Path): The path to the JSON file.

        Raises:
            FileNotFoundError: If the file_path does not exist.
            ValueError: If the JSON is malformed or its root structure is not a list.
        """
        if self.file_path_loaded != file_path or self.data is None:
            self._load_json_data(file_path)
            self.file_path_loaded = file_path
        # If data is already loaded for this path, do nothing.

    def _load_json_data(self, file_path: Path) -> None:
        """
        Loads and performs initial validation on the JSON data from the given file path.
        - Sets `self.data = []` for 0-byte files or files containing an empty JSON list ('[]').

        Args:
            file_path (Path): The path to the JSON file.

        Raises:
            FileNotFoundError: If the file_path does not exist.
            ValueError: If the JSON is malformed, its root structure is not a list,
                        or any other reading/parsing issue occurs.
        """
        logger.debug(f"Attempting to load JSON data from: {file_path}")

        if not file_path.exists():
            logger.error(f"JSON file not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_path.stat().st_size == 0:
            logger.warning(
                f"JSON file is empty (0 bytes): {file_path.name}. Treating as empty dataset."
            )
            self.data = []
            return

        try:
            with file_path.open("r", encoding=self.encoding) as f:
                loaded_json = json.load(f)
        except json.JSONDecodeError as e:
            err_msg = f"Invalid JSON format in '{file_path.name}': {e.msg} (line {e.lineno} col {e.colno})."
            logger.error(err_msg)
            raise ValueError(err_msg) from e
        except Exception as e:
            err_msg = (
                f"Could not read or parse JSON file '{file_path.name}'. Error: {str(e)}"
            )
            logger.error(
                err_msg, exc_info=True
            )  # Log full traceback for unexpected errors
            raise ValueError(err_msg) from e

        if not isinstance(loaded_json, list):
            # MODIFIED SECTION TO MATCH TEST EXPECTATION
            err_msg = "JSON file must contain a list of objects"  # Exact message expected by the test
            # Log more detailed info for debugging, but don't include in the raised exception message
            logger.error(
                f"JSON structure error in '{file_path.name}': {err_msg}. "
                f"Actual root type found: {type(loaded_json).__name__}."
            )
            raise ValueError(err_msg)
            # END OF MODIFIED SECTION

        self.data = loaded_json
        if not self.data:
            logger.info(
                f"JSON file {file_path.name} loaded successfully but contains an empty list (no records)."
            )
        else:
            logger.info(
                f"Successfully loaded {len(self.data)} records from {file_path.name}."
            )

    def get_headers(self, file_path: Path) -> List[str]:
        """
        Extracts headers from the JSON file by taking a union of all keys from all objects
        in the list. If the JSON list is empty or items are not objects,
        it handles this gracefully. Headers are alphabetically sorted.

        Args:
            file_path (Path): The path to the JSON file.

        Returns:
            List[str]: A sorted list of unique header strings.

        Raises:
            FileNotFoundError: If the file_path does not exist.
            ValueError: If the JSON is malformed or its root structure is not a list.
        """
        self._ensure_data_loaded(file_path)

        if (
            self.data is None
        ):  # Should ideally not be reached if _ensure_data_loaded works
            logger.error(
                f"Data is None after attempting to load {file_path.name}. Cannot extract headers."
            )
            return []

        if not self.data:  # Empty list in JSON
            logger.info(
                f"No data items to extract headers from in {file_path.name} (JSON list is empty)."
            )
            return []

        all_keys = set()
        non_object_items_count = 0
        for i, item in enumerate(self.data):
            if not isinstance(item, dict):
                logger.warning(
                    f"Item at index {i} in {file_path.name} is not a dictionary (type: {type(item).__name__}). "
                    f"Value snippet: '{str(item)[:100]}'. Skipping for header extraction."
                )
                non_object_items_count += 1
                continue
            all_keys.update(item.keys())

        if non_object_items_count > 0 and non_object_items_count == len(self.data):
            logger.warning(
                f"All {len(self.data)} items in {file_path.name} were non-dictionary types. "
                "No headers could be extracted."
            )
            return []
        elif non_object_items_count > 0:
            logger.warning(
                f"Skipped {non_object_items_count} non-dictionary items in {file_path.name} while extracting headers."
            )

        sorted_headers = sorted(list(all_keys))
        if not sorted_headers and self.data:
            logger.warning(
                f"No headers found in {file_path.name}, though data was present. "
                "This might mean all JSON objects were empty or non-dictionary items were skipped."
            )
        else:
            logger.info(
                f"Extracted and sorted headers for {file_path.name}: {sorted_headers}"
            )
        return sorted_headers

    def get_preview(self, file_path: Path, num_rows: int = 5) -> pd.DataFrame:
        """
        Provides a preview of the data from the JSON file.
        Columns are derived using `get_headers` for consistency.
        Missing keys in individual JSON objects result in NaN/None in the DataFrame,
        which are then filled with empty strings for the preview.

        Args:
            file_path (Path): The path to the JSON file.
            num_rows (int): The number of rows to preview. Clamped to be non-negative.

        Returns:
            pd.DataFrame: A DataFrame representing the preview rows.

        Raises:
            FileNotFoundError: If the file_path does not exist.
            ValueError: If JSON is malformed, root is not a list, or headers cannot be extracted.
        """
        num_rows = max(0, num_rows)
        self._ensure_data_loaded(file_path)

        if self.data is None:
            logger.error(
                f"Preview failed: Data is None after attempting to load {file_path.name}."
            )
            return pd.DataFrame()

        headers = self.get_headers(file_path)

        if not self.data:
            logger.info(
                f"No data to preview from {file_path.name} (JSON list is empty)."
            )
            return pd.DataFrame(columns=headers)

        valid_data_for_preview = [item for item in self.data if isinstance(item, dict)]

        if not valid_data_for_preview:
            logger.info(
                f"No dictionary items found in {file_path.name} to generate a preview."
            )
            return pd.DataFrame(columns=headers)

        preview_list = valid_data_for_preview[:num_rows]

        if not preview_list and num_rows > 0:
            logger.info(
                f"No dictionary items available in the first {num_rows} records of {file_path.name} for preview."
            )
            return pd.DataFrame(columns=headers)

        df = pd.DataFrame(preview_list, columns=headers)
        df = df.fillna("").astype(str)

        logger.info(
            f"Generated preview with {len(df)} rows and columns {df.columns.tolist()} from {file_path.name}"
        )
        return df

    def read_data(self, file_path: Path) -> Iterator[Dict[str, Any]]:
        """
        Reads JSON data from the specified file path after ensuring it's loaded.
        Yields each dictionary (object) from the JSON list.
        Non-dictionary items in the list are skipped and logged with a warning.

        Args:
            file_path (Path): The path to the JSON file.

        Yields:
            Iterator[Dict[str, Any]]: An iterator over dictionaries from the JSON list.

        Raises:
            FileNotFoundError: If the file_path does not exist (from _ensure_data_loaded).
            ValueError: If JSON is malformed or root structure is not a list (from _ensure_data_loaded).
        """
        self._ensure_data_loaded(file_path)

        if self.data is None:
            logger.error(
                f"Data not loaded for {file_path.name} prior to reading. Cannot yield records."
            )
            return iter([])

        if not self.data:
            logger.info(f"JSON data for {file_path.name} is empty. No records to read.")
            return iter([])

        logger.info(
            f"Starting to read and yield records from JSON file: {file_path.name}. Total items initially: {len(self.data)}"
        )
        num_skipped = 0
        for i, item in enumerate(self.data):
            if not isinstance(item, dict):
                logger.warning(
                    f"Skipping item at index {i} in {file_path.name} during data read: "
                    f"Expected a dictionary, but got type {type(item).__name__}. Value snippet: '{str(item)[:100]}'."
                )
                num_skipped += 1
                continue
            yield item

        if num_skipped > 0:
            logger.warning(
                f"Skipped a total of {num_skipped} non-dictionary items while reading data from {file_path.name}."
            )
        logger.info(
            f"Finished yielding records from {file_path.name}. Yielded {len(self.data) - num_skipped} records."
        )
