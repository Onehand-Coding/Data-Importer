# Data-Importer Hardening Plan

1. Replace `core/config.py` root detection with `get_data_dir()`/`DATA_DIR`.
2. Update `web/app.py` to use `DATA_DIR` for temp uploads and default DB.
3. Remove SQL parameter logging from `DatabaseManager.execute`.
4. Add `tests/test_config.py` and `tests/test_database_manager.py`.
5. Update `pyproject.toml` (Python >=3.10, Ruff, dev deps, pytest config).
6. Add CI, README updates, and `CONTEXT.md`.
7. Run ruff, format, pytest, `-W error`, and build.

No commit or push unless the user explicitly asks.
