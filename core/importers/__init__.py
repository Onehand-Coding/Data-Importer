from .base_importer import BaseImporter, ImportResult
from .csv_importer import CSVImporter
from .json_importer import JSONImporter
from .excel_importer import ExcelImporter

# Define which importers are available via the factory
AVAILABLE_IMPORTERS = {
    '.csv': CSVImporter,
    '.json': JSONImporter,
    '.xlsx': ExcelImporter,
}

__all__ = [
    'BaseImporter',
    'ImportResult',
    'CSVImporter',
    'JSONImporter',
    'ExcelImporter',
    'AVAILABLE_IMPORTERS'
    ]
