from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
from pathlib import Path

class BaseImporter(ABC):
    """Abstract base class for all data importers."""

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.validation_errors = []

    @abstractmethod
    def import_from_file(self, file_path: Path) -> Dict[str, any]:
        """Import data from a file."""
        pass

    def validate_row(self, row: Dict[str, any]) -> Tuple[bool, List[str]]:
        """Validate a single row of data."""
        errors = []

        # Basic validation
        if not row.get('name'):
            errors.append("Name is required")
        if not row.get('email'):
            errors.append("Email is required")
        elif '@' not in row['email']:
            errors.append("Invalid email format")

        return len(errors) == 0, errors

    def log_error(self, row_number: int, error: str, row_data: Dict[str, any]):
        """Log an import error."""
        self.validation_errors.append({
            'row': row_number,
            'error': error,
            'data': row_data
        })
