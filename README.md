# Data Entry Automation Tool

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.44.1-FF4B4B.svg)
![Pandas](https://img.shields.io/badge/Pandas-2.2.3-lightgrey.svg)
![SQLite](https://img.shields.io/badge/SQLite-3.x-blue.svg)

A professional data entry automation tool to import CSV data into SQLite databases with validation and dynamic table creation.

## Features

- **CSV Import**: Process data from CSV files using robust parsing.
- **Dynamic Table Creation**: Automatically creates SQLite tables based on CSV headers and inferred types.
- **Data Validation**: Basic checks for required fields (configurable) and email formats. Handles UNIQUE constraints.
- **Column Mapping**: Flexible mapping of source columns to database fields via the web UI.
- **Web Interface**: User-friendly Streamlit application for easy file upload, configuration, and import execution.
- **Command Line Interface**: Basic CLI for importing (uses default mapping/schema).
- **Error Handling**: Detailed error reporting with row numbers for skipped rows.
- **Database Storage**: SQLite backend managed efficiently.

## Installation

```bash
# Clone repository (if you haven't already)
git clone https://github.com/yourusername/Data-Entry-Automation.git
cd data-entry-automation

# Create virtual environment (Recommended)
python -m venv .venv

# Activate environment
# Linux/Mac:
source .venv/bin/activate
# Windows:
.\.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Web Interface (Recommended)

```bash
streamlit run web/app.py
```

Navigate the interface:
1. **Configure Database**: Set the path for your SQLite file in the sidebar.
2. **Upload File**: Upload your CSV data file.
3. **Configure Import**: Define the target table name and map CSV columns to database fields.
4. **Execute Import**: Run the import process. Results and errors will be displayed.

### Command Line Interface

```bash
python cli/main.py path/to/yourfile.csv -t your_table_name -d path/to/your_database.db
```

- `path/to/yourfile.csv`: (Required) The CSV file to import.
- `-t your_table_name`: (Optional) Specify the target table name. If omitted, it's derived from the CSV filename.
- `-d path/to/your_database.db`: (Optional) Specify the database file path. Defaults to `data/cli_database.db`.

**Note**: The CLI uses default mapping (CSV header -> sanitized header) and basic schema inference.

## Required CSV Format

- Must have a header row.
- Standard CSV dialects should work (comma-separated is default, others may be detected).

Example:

```csv
name,email,phone,company
John Doe,john@example.com,123456,Acme Inc
Jane Smith,jane@example.com,,Startup Co
```

## Project Structure

```
data-entry-automation/
├── core/             # Core logic (database, importers)
│   ├── __init__.py
│   ├── database.py
│   └── importers/
│       ├── __init__.py
│       ├── base_importer.py
│       └── csv_importer.py
├── web/              # Streamlit web interface
│   └── app.py
├── cli/              # Command line interface
│   └── main.py
├── tests/            # Unit tests
│   ├── __init__.py
│   └── test_csv_importer.py
├── sample_input/     # Sample data files
├── temp_uploads/     # Temporary storage for uploads (can be gitignored)
├── data/             # Default location for database files (can be gitignored)
├── .gitignore
├── requirements.txt
└── README.md
```

## Requirements

- Python 3.8+
- See `requirements.txt` for specific libraries (pandas, streamlit).

## License

Distributed under the MIT License.
