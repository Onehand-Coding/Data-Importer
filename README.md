# Data Entry Automation Tool

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.44.1-FF4B4B.svg)
![Pandas](https://img.shields.io/badge/Pandas-2.2.3-lightgrey.svg)
![SQLite](https://img.shields.io/badge/SQLite-3.x-blue.svg)
![Openpyxl](https://img.shields.io/badge/Openpyxl-✓-green.svg)

A professional data entry automation tool that imports data from CSV, JSON, and Excel (XLSX) files into SQLite databases with validation and dynamic table creation.

## Features

- **Multi-Format Import**: Process data from CSV, JSON, and Excel (.xlsx) files
- **Dynamic Table Creation**: Automatically creates SQLite tables based on source file headers and inferred data types
- **Data Validation**: Basic checks for required fields (configurable via UI) and email formats with handling for database-level UNIQUE constraints
- **Column Mapping**: Flexible mapping of source columns to database fields via the web UI
- **Web Interface**: User-friendly Streamlit application for easy file upload, configuration, preview, and import execution
- **Command Line Interface**: CLI for importing CSV, JSON, and Excel files (uses default mapping/schema)
- **Error Handling**: Detailed error reporting with row numbers for skipped rows, available for download in the web UI
- **Database Storage**: Efficient SQLite backend management
- **Temporary File Management**: Automatic cleanup of temporary upload files
- **Caching**: Utilizes Streamlit caching for improved performance of database connections and file processing steps

## Installation

```bash
# Clone repository
git clone https://github.com/yourusername/Data-Entry-Automation.git
cd data-entry-automation

# Create virtual environment (recommended)
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

1. **Configure Database**: Set the path for your SQLite file in the sidebar. View existing tables.
2. **Upload File**: Upload your CSV, JSON, or XLSX data file.
3. **Configure Import**: Define the target table name, map source columns to database fields, and adjust inferred data types or constraints (e.g., UNIQUE for email).
4. **Preview Data**: Review a preview of the data based on your mapping.
5. **Execute Import**: Run the import process. Results and any errors (with download option) will be displayed.

### Command Line Interface

```bash
python cli/main.py path/to/yourfile.[csv|json|xlsx] -t your_table_name -d path/to/your_database.db
```

- `path/to/yourfile.[csv|json|xlsx]`: (Required) The data file to import.
- `-t your_table_name`: (Optional) Specify the target table name. If omitted, it's derived from the input filename.
- `-d path/to/your_database.db`: (Optional) Specify the database file path. Defaults to `data/db/cli_database.db`.
- `-v, --verbose`: (Optional) Show detailed (DEBUG) logging.

**Note**: The CLI uses default mapping (source header → sanitized header) and basic schema inference (e.g., email columns are set as TEXT UNIQUE). For complex mapping or schema definitions, use the web interface.

## Required Input File Formats

- **General**: Must have a header row (for CSV/Excel) or be a list of flat JSON objects.
- **CSV**: Standard CSV dialects should work. Delimiter and quote character are auto-detected.
- **JSON**: Expects a list of flat JSON objects (key-value pairs).
- **Excel (.xlsx)**: Reads data from the first active sheet. The first row is assumed to be headers.

Example (CSV):
```
Full Name,Email,Phone Number,Company
John Doe,john@example.com,123-456-7890,Doe Ltd.
Jane Smith,jane.s@example.com,,Smith & Co
```

## Project Structure

```
data-entry-automation/
├── .gitignore
├── README.md
├── requirements.txt
├── cli/
│   └── main.py
├── core/
│   ├── __init__.py
│   ├── database.py
│   └── importers/
│       ├── __init__.py
│       ├── base_importer.py
│       ├── csv_importer.py
│       ├── excel_importer.py
│       └── json_importer.py
├── data/
│   ├── db/                  # Default location for database files (gitignored)
│   └── temp_uploads/        # Temporary storage for uploads (gitignored)
├── sample_input/            # Sample data files for testing
│   ├── sample_data_entry_csv.csv
│   ├── sample_data_entry_json.json
│   └── sample_data_entry_xlsx.xlsx
├── tests/
│   ├── __init__.py
│   ├── test_csv_importer.py
│   ├── test_excel_importer.py
│   └── test_json_importer.py
└── web/
    └── app.py
```

## Requirements

- Python 3.8+
- See `requirements.txt` for specific libraries (pandas, streamlit, openpyxl, etc.)

## License

Distributed under the MIT License.
