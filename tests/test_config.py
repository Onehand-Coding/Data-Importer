from pathlib import Path

from data_importer.core import config


def test_data_dir_default(monkeypatch):
    monkeypatch.delenv("DATA_IMPORTER_DATA_DIR", raising=False)
    assert config.get_data_dir().name == "data-importer"


def test_data_dir_override(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_IMPORTER_DATA_DIR", str(tmp_path))
    assert config.get_data_dir() == Path(tmp_path)
