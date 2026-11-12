"""Extract tests use fake HTTP responses - they never call the real API."""

from unittest import mock

import pytest
import requests

from bank_pipeline.errors import ExtractError, SourceUnavailableError
from bank_pipeline.extract import get_dataset
from bank_pipeline.extract.api_client import download_dataset


class FakeResponse:
    def __init__(self, status=200, body=b'{"status": "200"}'):
        self.status_code = status
        self.body = body

    def iter_content(self, chunk_size):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i:i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def test_download_writes_file_and_sends_bearer_token(tmp_path):
    dest = tmp_path / "raw.json"
    with mock.patch("requests.get", return_value=FakeResponse(body=b'{"a": 1}')) as get:
        size = download_dataset("http://api", "secret-token", dest, timeout=5)

    assert size == 8 and dest.read_bytes() == b'{"a": 1}'
    assert get.call_args.kwargs["headers"]["Authorization"] == "Bearer secret-token"
    assert get.call_args.kwargs["timeout"] == 5


def test_server_error_is_retried_with_backoff(tmp_path):
    responses = [FakeResponse(503), FakeResponse(502), FakeResponse(200)]
    sleeps = []
    with mock.patch("requests.get", side_effect=responses) as get:
        download_dataset("http://api", "t", tmp_path / "raw.json", retries=3, backoff=2, sleep=sleeps.append)

    assert get.call_count == 3
    assert sleeps == [2, 4]  # exponential backoff


def test_connection_errors_give_up_after_all_retries(tmp_path):
    sleeps = []
    with mock.patch("requests.get", side_effect=requests.ConnectionError("down")) as get:
        with pytest.raises(SourceUnavailableError, match="3 attempts"):
            download_dataset("http://api", "t", tmp_path / "raw.json", retries=2, backoff=1, sleep=sleeps.append)

    assert get.call_count == 3
    assert sleeps == [1, 2]
    assert not (tmp_path / "raw.json").exists()


def test_bad_token_is_not_retried(tmp_path):
    with mock.patch("requests.get", return_value=FakeResponse(401)) as get:
        with pytest.raises(ExtractError, match="401"):
            download_dataset("http://api", "wrong", tmp_path / "raw.json", retries=3, sleep=lambda s: None)
    assert get.call_count == 1


def test_too_large_download_is_aborted(tmp_path):
    big = FakeResponse(body=b"x" * (2 * 1024 * 1024))
    with mock.patch("requests.get", return_value=big):
        with pytest.raises(ExtractError, match="MAX_DOWNLOAD_MB"):
            download_dataset("http://api", "t", tmp_path / "raw.json", max_mb=1)
    assert not (tmp_path / "raw.json").exists()


def make_settings(tmp_path, mode, local_file):
    return mock.Mock(
        source_mode=mode, source_url="http://api", source_token="t", local_file=local_file,
        http_timeout=1, http_retries=0, http_backoff=0, max_download_mb=10,
    )


def test_auto_mode_falls_back_to_local_file(tmp_path, envelope_file):
    settings = make_settings(tmp_path, "auto", envelope_file)
    with mock.patch("requests.get", side_effect=requests.ConnectTimeout("timeout")):
        result = get_dataset(settings, tmp_path / "raw", "run_1")

    assert result.source == "file" and result.used_fallback
    assert result.path.read_bytes() == envelope_file.read_bytes()


def test_api_mode_fails_when_api_is_down(tmp_path, envelope_file):
    settings = make_settings(tmp_path, "api", envelope_file)
    with mock.patch("requests.get", side_effect=requests.ConnectTimeout("timeout")):
        with pytest.raises(SourceUnavailableError):
            get_dataset(settings, tmp_path / "raw", "run_1")


def test_file_mode_does_not_call_the_api(tmp_path, envelope_file):
    settings = make_settings(tmp_path, "file", envelope_file)
    with mock.patch("requests.get") as get:
        result = get_dataset(settings, tmp_path / "raw", "run_1")
    get.assert_not_called()
    assert result.source == "file" and not result.used_fallback


def test_missing_local_file(tmp_path):
    settings = make_settings(tmp_path, "file", tmp_path / "nope.json")
    with pytest.raises(ExtractError, match="not found"):
        get_dataset(settings, tmp_path / "raw", "run_1")
