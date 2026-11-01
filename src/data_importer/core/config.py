import os
import tempfile
from pathlib import Path


def get_data_dir() -> Path:
    """Return a writable data directory outside the repository."""
    override = os.getenv("DATA_IMPORTER_DATA_DIR")
    if override:
        return Path(override).expanduser()
    return Path(tempfile.gettempdir()) / "data-importer"


DATA_DIR = get_data_dir()
