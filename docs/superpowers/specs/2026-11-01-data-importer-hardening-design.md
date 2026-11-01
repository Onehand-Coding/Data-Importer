# Data-Importer Hardening Design

Date: 2026-11-01
Status: Approved for implementation

## Context

The importer has 47 passing tests but no CI, no Ruff configuration, and a
`core/config.py` that resolves the project root from the current working
directory at import time. `web/app.py` uses that root for temporary uploads, so
the web app fails when launched outside the repository or from an installed
wheel. Database errors also log SQL parameters, which can include personal data.

## Goals

1. Replace project-root assumptions with an XDG/temp data directory and an
   environment override.
2. Stop logging SQL parameters on database errors.
3. Add Ruff, pytest config, CI, README updates, and CONTEXT.
4. Add tests for config paths and database identifier handling.

## Design

- `core/config.py` exposes `get_data_dir()` and `DATA_DIR`:
  - `DATA_IMPORTER_DATA_DIR` override.
  - Default: `tempfile.gettempdir() / "data-importer"`.
- `web/app.py` uses `DATA_DIR / "temp_uploads"` and a data-dir default target DB.
- `DatabaseManager.execute` logs the query and error but not `params`.
- `pyproject.toml`: Python >=3.10, add `ruff` to dev, add Ruff and pytest config.
- CI runs `uv sync --extra dev`, ruff, format check, pytest, and a wheel build.
- Tests:
  - config default/override.
  - `sanitize_name` and dynamic table creation with unsafe names.
  - database errors do not log parameter values.

## Acceptance

- Importing `core.config` no longer depends on the working directory.
- Web temp paths are outside the repo.
- No SQL parameter values in error logs.
- `ruff`, format, and `pytest` pass locally and in CI.
