import sqlite3
from typing import Optional, Dict, Any
from pathlib import Path

class DatabaseManager:
    """Manages database connections and table creation."""

    def __init__(self, db_path: str = 'contacts.db'):
        self.db_path = db_path
        self.connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self) -> bool:
        """Establish a database connection."""
        try:
            self.connection = sqlite3.connect(self.db_path)
            print(f"Connected to database: {self.db_path} (SQLite v{sqlite3.sqlite_version})")
            return True
        except sqlite3.Error as e:
            print(f"Database connection error: {e}")
            return False

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute(self, query: str, params: tuple = (), commit: bool = False) -> Optional[sqlite3.Cursor]:
        """Execute a SQL query."""
        if not self.connection:
            raise ConnectionError("Database not connected")

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            if commit:
                self.connection.commit()
            return cursor
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return None

    def create_tables(self):
        """Create required tables if they don't exist."""
        sql = """
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            phone TEXT,
            company TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.execute(sql, commit=True)
        print("Tables verified/created successfully")
