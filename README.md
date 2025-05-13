# Data Entry Automation Tool

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.44.1-FF4B4B.svg)](https://streamlit.io/)
[![Pandas](https://img.shields.io/badge/Pandas-2.2.3-150458.svg)](https://pandas.pydata.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-✓-orange.svg)](https://www.sqlalchemy.org/)
[![SQLite](https://img.shields.io/badge/SQLite-3.x-003B57.svg)](https://www.sqlite.org/)
[![Openpyxl](https://img.shields.io/badge/Openpyxl-✓-green.svg)](https://openpyxl.readthedocs.io/)

A professional data entry automation tool that imports data from various sources (CSV, JSON, Excel, and relational databases) into SQLite databases with validation, dynamic table creation, and export capabilities.

## 🚀 Features

- **Multi-Source Data Import**
  - Process data from CSV, JSON, and Excel (.xlsx) files
  - Import directly from SQLite, PostgreSQL, and MySQL databases using connection strings

- **Intelligent Data Handling**
  - Dynamic table creation based on source headers and inferred data types
  - Data validation for required fields and email formats
  - Handling for database-level UNIQUE constraints during import

- **User-Friendly Interfaces**
  - **Web Interface (Streamlit)**
    - Easy file upload or database connection configuration
    - Visual column mapping and data type adjustment
    - Data preview before execution
    - Detailed error reporting with downloadable reports

  - **Command Line Interface (CLI)**
    - Quick file-based imports with sensible defaults

- **Flexible Output Options**
  - Import to SQLite database (primary function)
  - Export as CSV, JSON, or Excel files (web interface only)

## 💻 Installation

```bash
# Clone repository
git clone https://github.com/yourusername/data-entry-automation.git
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

## 📋 Usage

### Web Interface (Recommended)

```bash
streamlit run web/app.py
```

#### Step-by-Step Guide:

1. **Select Data Source Type**
   - Choose between "File Upload" or "Database"

2. **Configure Source**
   - **For File Upload:** Upload CSV, JSON, or XLSX file
   - **For Database:**
     - Select database type (SQLite, PostgreSQL, MySQL)
     - Enter connection string
     - Choose between table import or custom SQL query

3. **Configure Target**
   - Set the SQLite database path in the sidebar
   - Define the target table name
   - Map source columns to database fields
   - Adjust inferred data types or constraints (e.g., TEXT UNIQUE for email)

4. **Choose Output Destination**
   - "Import to SQLite Database" (primary function)
   - "Download as File" (CSV, JSON, Excel)

5. **Preview & Execute**
   - Review data preview based on your mapping
   - Run the import/export process

6. **Review Results**
   - View operation summary (rows processed, inserted, skipped)
   - Download error report for any issues

### Command Line Interface

The CLI supports file imports with default mapping:

```bash
python cli/main.py path/to/yourfile.[csv|json|xlsx] -t your_table_name -d path/to/your_database.db
```

#### Parameters:
- `path/to/yourfile.[csv|json|xlsx]`: (Required) Data file to import
- `-t your_table_name`: (Optional) Target table name (defaults to filename)
- `-d path/to/your_database.db`: (Optional) Database path (defaults to `data/db/cli_database.db`)
- `-v, --verbose`: (Optional) Show detailed logging

> **Note:** Database source imports and file exports are currently only available via the web interface. The CLI uses default mapping and basic schema inference.

## 📄 Input File Requirements

- **General**: Must have a header row (for CSV/Excel) or be a list of flat JSON objects
- **CSV**: Standard CSV dialects with auto-detected delimiters and quote characters
- **JSON**: Expects a list of flat JSON objects (key-value pairs)
- **Excel (.xlsx)**: Reads data from the first active sheet, first row as headers

Example (CSV):
```
Full Name,Email,Phone Number,Company
John Doe,john@example.com,123-456-7890,Doe Ltd.
Jane Smith,jane.s@example.com,,Smith & Co
```

## 📂 Project Structure

```
data-entry-automation/
├── .gitignore
├── README.md
├── requirements.txt
├── cli/
│   └── main.py              # Command line interface
├── core/
│   ├── __init__.py
│   ├── database.py          # Database connection and operations
│   └── importers/
│       ├── __init__.py
│       ├── base_importer.py
│       ├── csv_importer.py
│       ├── excel_importer.py
│       ├── json_importer.py
│       └── database_source_importer.py  # DB source importer
├── data/
│   ├── db/                  # Default database location (gitignored)
│   └── temp_uploads/        # Temporary file storage (gitignored)
├── sample_input/            # Sample files for testing
│   ├── sample_data_entry_csv.csv
│   ├── sample_data_entry_json.json
│   └── sample_data_entry_xlsx.xlsx
├── tests/
│   ├── __init__.py
│   ├── test_csv_importer.py
│   ├── test_database_source_importer.py
│   ├── test_excel_importer.py
│   └── test_json_importer.py
└── web/
    └── app.py               # Streamlit web interface
```

## ⚙️ Requirements

- Python 3.8+
- Streamlit 1.44.1+
- Pandas 2.2.3+
- SQLAlchemy
- SQLite 3.x
- Openpyxl
- Database connectors (for PostgreSQL/MySQL functionality):
  - `psycopg2-binary` (PostgreSQL)
  - `mysql-connector-python` (MySQL)

## ✅ Testing

The project includes comprehensive unit tests:

```bash
python -m unittest discover tests
```

All tests are currently passing, ensuring reliability of the importers and core logic.

## 📜 License

Distributed under the MIT License.
