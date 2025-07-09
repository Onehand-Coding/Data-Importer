import os
from pathlib import Path


def get_project_root() -> Path:
    """Find the project root directory by locating pyproject.toml.

    Returns:
        Path: Absolute path to project root directory.

    Raises:
        FileNotFoundError: If pyproject.toml cannot be found in parent directories.
    """
    current_dir = Path(os.getcwd()).absolute()

    # Search upward through parent directories
    for parent in [current_dir, *current_dir.parents]:
        if (parent / "pyproject.toml").exists():
            return parent

    raise FileNotFoundError(
        "Could not find pyproject.toml in current or parent directories. "
        f"Searching from: {current_dir}"
    )


ROOT_DIR = get_project_root()
