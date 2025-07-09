# Data Entry Automation Tool

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.44.1-FF4B4B.svg)](https://streamlit.io/)
[![Pandas](https://img.shields.io/badge/Pandas-2.2.3-150458.svg)](https://pandas.pydata.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-✓-orange.svg)](https://www.sqlalchemy.org/)
[![SQLite](https://img.shields.io/badge/SQLite-3.x-003B57.svg)](https://www.sqlite.org/)
[![Openpyxl](https://img.shields.io/badge/Openpyxl-✓-green.svg)](https://openpyxl.readthedocs.io/)
[![Tests](https://img.shields.io/badge/tests-47%20passed-green.svg)](https://github.com/Onehand-Coding/data-importer)

A professional data entry automation tool that streamlines the process of importing data from various sources (CSV, JSON, Excel, and relational databases) into SQLite databases with intelligent validation, dynamic table creation, and flexible export capabilities.

## 🚀 Features

### Multi-Source Data Import
- **File Support**: CSV, JSON, and Excel (.xlsx) files with automatic format detection
- **Database Integration**: Direct import from SQLite, PostgreSQL, and MySQL databases using connection strings
- **Custom SQL Support**: Execute custom queries for complex data extraction

### Intelligent Data Processing
- **Dynamic Schema Creation**: Automatically infers data types and creates tables based on source headers
- **Advanced Validation**: Built-in validation for required fields, email formats, and data consistency
- **Constraint Handling**: Intelligent handling of database-level UNIQUE constraints during import
- **Error Recovery**: Continues processing when encountering data issues with detailed error reporting

### Dual Interface Options

#### Web Interface (Streamlit) - **Recommended**
- **Intuitive File Upload**: Drag-and-drop interface for easy file handling
- **Visual Column Mapping**: Interactive mapping between source and target columns
- **Real-time Data Preview**: See exactly how your data will be processed before execution
- **Advanced Configuration**: Fine-tune data types, constraints, and validation rules
- **Comprehensive Reporting**: Detailed success/error reports with downloadable summaries
- **Export Options**: Generate CSV, JSON, or Excel files as alternatives to database import

#### Command Line Interface (CLI)
- **Batch Processing**: Perfect for automated workflows and scripting
- **Quick Imports**: Streamlined file-based imports with sensible defaults
- **Scriptable**: Easy integration into existing data pipelines

### Flexible Output Options
- **Primary Function**: Import to SQLite database with full schema control
- **Export Alternatives**: Generate CSV, JSON, or Excel files (web interface only)
- **Format Conversion**: Convert between different data formats seamlessly

## 💻 Installation

### Quick Start
```bash
# Clone the repository
git clone https://github.com/Onehand-Coding/data-importer.git
cd data-importer

# Set up environment and install dependencies
uv sync
```

### Development Setup
```bash
# Install with development dependencies
uv sync --extra dev

# Run tests to verify installation
uv run pytest
```

## 📋 Usage

### Web Interface (Recommended)

Launch the web application:
```bash
uv run data-importer-web
```

The application will be available at `http://localhost:8501`

#### Complete Workflow:

1. **Select Data Source Type**
   - **File Upload**: Support for CSV, JSON, and XLSX files
   - **Database Connection**: Connect to existing SQLite, PostgreSQL, or MySQL databases

2. **Configure Your Source**
   - **File Upload Path**:
     - Upload your data file using the drag-and-drop interface
     - Preview your data structure automatically
   - **Database Path**:
     - Select database type (SQLite, PostgreSQL, MySQL)
     - Enter connection string (e.g., `sqlite:///path/to/database.db`)
     - Choose between full table import or custom SQL query execution

3. **Configure Target Database**
   - Set the SQLite database path in the sidebar
   - Define the target table name (auto-suggested based on source)
   - Map source columns to database fields with visual interface
   - Adjust inferred data types and add constraints (e.g., `TEXT UNIQUE` for email fields)

4. **Choose Output Destination**
   - **"Import to SQLite Database"**: Primary function with full validation and constraint handling
   - **"Download as File"**: Export as CSV, JSON, or Excel for external use

5. **Preview & Execute**
   - Review comprehensive data preview based on your mapping configuration
   - Validate your settings before processing
   - Execute the import/export process with real-time progress tracking

6. **Review Results & Reports**
   - View detailed operation summary (rows processed, inserted, skipped, errors)
   - Download comprehensive error reports for troubleshooting
   - Export success logs for audit trails

### Command Line Interface

The CLI supports file imports with default mapping:

```bash
uv run data-importer-cli input_file [-t TABLE] [-d DATABASE] [-v]
```

#### Parameters:
- `input_file`: (Required) Path to the input file (e.g., data.csv, data.json, data.xlsx)
- `-t TABLE, --table TABLE`: (Optional) Target table name (defaults to derived from filename)
- `-d DATABASE, --database DATABASE`: (Optional) Path to SQLite database file (defaults to `data/db/cli_database.db`)
- `-v, --verbose`: (Optional) Show detailed (DEBUG) logging

#### Examples:
```bash
# Basic import with default settings
uv run data-importer-cli data/sample_input/sample_data_entry_csv.csv

# Specify custom table name and database
uv run data-importer-cli data/sample_input/contacts.json -t contacts -d my_database.db

# Enable verbose logging
uv run data-importer-cli data/sample_input/products.xlsx -v
```

> **Note**: Database source imports and file exports are currently exclusive to the web interface. The CLI focuses on efficient file-to-database imports with automatic schema inference.

## 📄 Input File Requirements

### General Requirements
- **Headers**: Must include a header row (CSV/Excel) or be a list of flat JSON objects
- **Structure**: Flat data structure (no nested objects or arrays)
- **Encoding**: UTF-8 encoding recommended for international characters

### Format-Specific Requirements

#### CSV Files
- Standard CSV dialects with auto-detected delimiters and quote characters
- Supports common separators: comma, semicolon, tab, pipe
- Handles quoted fields and escaped characters automatically

#### JSON Files
- Must be a JSON array of objects
- Each object should have consistent key-value pairs
- Nested objects are flattened with dot notation

#### Excel Files (.xlsx)
- Reads from the first worksheet only
- First row treated as headers
- Supports standard Excel data types (text, numbers, dates)

### Example Data Formats

**CSV Example:**
```csv
Full Name,Email,Phone Number,Company,Registration Date
John Doe,john@example.com,123-456-7890,Doe Ltd.,2024-01-15
Jane Smith,jane.s@example.com,,Smith & Co,2024-01-16
```

**JSON Example:**
```json
[
  {
    "Full Name": "John Doe",
    "Email": "john@example.com",
    "Phone Number": "123-456-7890",
    "Company": "Doe Ltd.",
    "Registration Date": "2024-01-15"
  },
  {
    "Full Name": "Jane Smith",
    "Email": "jane.s@example.com",
    "Phone Number": null,
    "Company": "Smith & Co",
    "Registration Date": "2024-01-16"
  }
]
```

## 📂 Project Structure

```
data-importer/
├── .gitignore
├── LICENSE
├── pyproject.toml          # Project configuration and dependencies
├── README.md
├── uv.lock                 # Dependency lock file
├── data/
│   ├── db/                 # SQLite database files (gitignored)
│   │   └── sqlite_test.db
│   ├── sample_input/       # Sample files for testing and examples
│   │   ├── contacts_mixed.json
│   │   ├── products.json
│   │   ├── sample_data_entry_csv.csv
│   │   ├── sample_data_entry_json.json
│   │   ├── sample_data_entry_xlsx.xlsx
│   │   └── test_*.csv      # Test files for validation
│   └── temp_uploads/       # Temporary upload storage (gitignored)
├── src/
│   └── data_importer/      # Main application package
│       ├── __init__.py
│       ├── cli/
│       │   └── main.py     # Command line interface
│       ├── core/
│       │   ├── config.py   # Application configuration
│       │   ├── database.py # Database connection and operations
│       │   ├── validators.py # Data validation logic
│       │   └── importers/  # Data import modules
│       │       ├── base_importer.py
│       │       ├── csv_importer.py
│       │       ├── excel_importer.py
│       │       ├── json_importer.py
│       │       └── database_source_importer.py
│       └── web/
│           ├── app.py      # Streamlit web interface
│           └── launcher.py # Web app launcher
└── tests/
    ├── test_csv_importer.py
    ├── test_database_source_importer.py
    ├── test_excel_importer.py
    └── test_json_importer.py
```

## ⚙️ Technical Requirements

### Core Dependencies
- **Python 3.8+**: Modern Python with full asyncio support
- **Streamlit 1.44.1+**: Web interface framework
- **Pandas 2.2.3+**: Data manipulation and analysis
- **SQLAlchemy**: Database toolkit and ORM
- **SQLite 3.x**: Built-in database engine
- **Openpyxl**: Excel file processing

### Optional Database Connectors
For PostgreSQL and MySQL support:
- **`psycopg2-binary`**: PostgreSQL adapter
- **`mysql-connector-python`**: MySQL connector

### Development Dependencies
- **pytest**: Testing framework
- **Coverage tools**: Code coverage analysis

## ✅ Testing & Quality Assurance

The project maintains high code quality with comprehensive test coverage:

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src/data_importer

# Run specific test modules
uv run pytest tests/test_csv_importer.py -v
```

**Current Test Status**: All 47 tests passing ✅

### Test Coverage
- **CSV Import**: Data parsing, validation, and error handling
- **Excel Import**: Workbook processing and data extraction
- **JSON Import**: Array processing and object validation
- **Database Sources**: Connection handling and query execution
- **Data Validation**: Email validation, required field checking
- **Error Handling**: Graceful failure and recovery scenarios

## 🔧 Configuration

### Environment Variables
```bash
# Optional: Set default database path
export DATA_IMPORTER_DB_PATH="/path/to/default/database.db"

# Optional: Set upload directory
export DATA_IMPORTER_UPLOAD_DIR="/path/to/uploads"
```

### Database Connection Strings
```python
# SQLite
sqlite:///path/to/database.db

# PostgreSQL
postgresql://user:password@host:port/database

# MySQL
mysql://user:password@host:port/database
```

## 🚀 Advanced Usage

### Custom Data Validation
The web interface allows you to add custom validation rules:
- **Required Fields**: Mark columns as mandatory
- **Unique Constraints**: Prevent duplicate entries
- **Email Validation**: Automatic email format checking
- **Data Type Enforcement**: Ensure data integrity

### Batch Processing
For large datasets, the tool provides:
- **Chunked Processing**: Handle large files without memory issues
- **Progress Tracking**: Real-time progress updates
- **Resume Capability**: Continue interrupted imports
- **Error Isolation**: Skip problematic rows while processing valid data

## 🤝 Contributing

We welcome contributions! Please see our contributing guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes and add tests
4. Ensure all tests pass (`uv run pytest`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/Onehand-Coding/data-importer/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Onehand-Coding/data-importer/discussions)
- **Documentation**: [Project Wiki](https://github.com/Onehand-Coding/data-importer/wiki)

## 🔄 Version History

- **v1.0.0**: Initial release with full feature set
- **Current**: Stable release with 47 passing tests and comprehensive validation

---

*Built with ❤️ by [Onehand-Coding](https://github.com/Onehand-Coding)*
