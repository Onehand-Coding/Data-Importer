import logging

from data_importer.core.database import DatabaseManager


def test_sanitize_name():
    db = DatabaseManager(":memory:")
    assert db.sanitize_name("Full Name!") == "full_name_"
    assert db.sanitize_name("123 table") == "_123_table"
    assert db.sanitize_name("") is None


def test_create_dynamic_table_sanitizes_identifiers():
    with DatabaseManager(":memory:") as db:
        assert db.create_dynamic_table(
            "bad name; DROP TABLE users",
            {"Full Name!": "TEXT", "123": "TEXT"},
        )
        row = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchone()
        assert row is not None
        assert "drop" in row[0]
        assert db.execute('SELECT * FROM "bad_name__drop_table_users"') is not None


def test_database_error_does_not_log_parameter_values(caplog):
    caplog.set_level(logging.ERROR)
    with DatabaseManager(":memory:") as db:
        db.execute("SELECT * FROM missing WHERE value = ?", ("secret-value",))
    assert "secret-value" not in caplog.text
