import logging

import pytest

from bank_pipeline.config import load_settings
from bank_pipeline.database.sql_files import split_batches
from bank_pipeline.errors import AlreadyRunningError, ConfigError
from bank_pipeline.utils.helpers import batched
from bank_pipeline.utils.lock import PipelineLock
from bank_pipeline.utils.logger import SecretFormatter


def test_batched():
    assert list(batched(range(7), 3)) == [[0, 1, 2], [3, 4, 5], [6]]
    assert list(batched([], 3)) == []


def test_second_lock_is_refused(tmp_path):
    lock_file = tmp_path / "pipeline.lock"
    with PipelineLock(lock_file):
        with pytest.raises(AlreadyRunningError):
            with PipelineLock(lock_file):
                pass
    with PipelineLock(lock_file):  # free again after the first run ends
        pass


def test_secrets_are_masked_in_logs():
    formatter = SecretFormatter(secrets=["MyS3cretToken", "db-pass"])
    record = logging.LogRecord(
        "test", logging.ERROR, __file__, 1,
        "token=%s password=%s header=Bearer abc.def", ("MyS3cretToken", "db-pass"), None,
    )
    text = formatter.format(record)
    assert "MyS3cretToken" not in text and "db-pass" not in text and "abc.def" not in text
    assert text.endswith("token=*** password=*** header=Bearer ***")


def test_split_batches_on_go():
    sql = "SELECT 1;\nGO\n\n  go  \nSELECT 2;\nGO"
    assert split_batches(sql) == ["SELECT 1;", "SELECT 2;"]


def test_invalid_source_mode(monkeypatch):
    monkeypatch.setenv("SOURCE_MODE", "ftp")
    with pytest.raises(ConfigError):
        load_settings()


def test_api_mode_needs_token(monkeypatch):
    monkeypatch.setenv("SOURCE_MODE", "api")
    monkeypatch.setenv("SOURCE_TOKEN", " ")  # empty counts as missing
    with pytest.raises(ConfigError, match="SOURCE_TOKEN"):
        load_settings()


def test_token_is_not_in_repr(monkeypatch):
    monkeypatch.setenv("SOURCE_TOKEN", "super-secret-value")
    assert "super-secret-value" not in repr(load_settings())
