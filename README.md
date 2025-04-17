# Data Entry Automation Tool
![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.14.0-FF4B4B.svg)
![SQLite](https://img.shields.io/badge/SQLite-3.39-lightgrey.svg)

A professional data entry automation tool to import CSV data into SQLite databases with validation.
will add support more format soon...

## Features
- **CSV Import**: Process data from CSV files
- **Data Validation**: Checks for required fields and email formats
- **Dual Interface**: Both web (Streamlit) and CLI versions
- **Error Handling**: Detailed error reporting with row numbers
- **Database Storage**: SQLite backend with proper table structure

## Installation
```bash
# Clone repository
git clone https://github.com/Phoenix1025/Data-Entry-Automation.git
cd data-entry-automation

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.\.venv\Scripts\activate  # Windows

# Install dependencies
pip install streamlit pandas
```

## Usage

### Web Interface (Recommended)
```bash
streamlit run web/app.py
```
Then upload your CSV file through the browser interface.

### Command Line Interface
```bash
python cli/main.py path/to/yourfile.csv
```

## Required CSV Format
```csv
name,email,phone,company
John Doe,john@example.com,123456,Acme Inc
Jane Smith,jane@example.com,,Startup Co
```

## Project Structure
```
data-entry-automation/
├── core/          # Database and import logic
│   ├── database.py
│   └── importers/
├── web/           # Streamlit web interface
│   └── app.py
├── cli/           # Command line interface
│   └── main.py
├── .gitignore
└── README.md
```

## Requirements
- Python 3.8+
- Streamlit (for web interface)
- Pandas (for data handling)

## Screenshot
*Web Interface Preview*:
![Screenshot](https://example.com/screenshot.png)

*CLI Interface Preview*:
![Screenshot](https://example.com/screenshot.png)

## License
Distributed under the MIT License.
