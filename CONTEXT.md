# Project Context

## Purpose

A local data importer that loads CSV, JSON, Excel, and database sources into
SQLite. It also has a Streamlit web UI and a CLI.

## Architecture

- `core/database.py` owns SQLite connections, identifier sanitization, and
  dynamic table creation.
- `core/importers/` contains one importer per source format plus a shared base.
- `cli/main.py` is the scriptable file-to-SQLite path.
- `web/app.py` is the Streamlit interface.
- `core/config.py` resolves a writable data directory outside the repository.

## Data

- Default data dir: system temp directory under `data-importer/`
- Override: `DATA_IMPORTER_DATA_DIR`
- Temporary uploads: `<data dir>/temp_uploads`
- Default web database: `<data dir>/db/importer_pro.db`

## Commands

```bash
uv sync --extra dev
uv run data-importer-web
uv run data-importer-cli input_file.csv
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Decisions

- Keep SQLite as the target database.
- Sanitize and quote table/column identifiers; never interpolate raw names.
- Do not log SQL parameter values on database errors.
- Use a user/temp data directory instead of the repository working directory.
- Add CI and Ruff without rewriting the import pipeline.

## Gotchas

- Tests use temporary SQLite databases and do not need external services.
- PostgreSQL/MySQL importers need real connection strings; tests mock them.
- Dynamic schema types are sanitized to a conservative character set.
- The web UI writes temp uploads outside the repo now.

## Known Limitations

- No authentication or multi-user isolation for the Streamlit UI.
- Database source imports require external servers.
- Existing data under a project-local `data/` directory is not migrated.
