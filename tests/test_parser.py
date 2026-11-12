import csv
import json

import pytest

from bank_pipeline.errors import InvalidDataError
from bank_pipeline.transform.parser import COLUMNS, parse_raw_file
from conftest import SAMPLE_ROWS, write_envelope


def read_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_envelope_is_converted_to_rows(envelope_file, tmp_path):
    out = tmp_path / "records.csv"
    count = parse_raw_file(envelope_file, out)

    rows = read_rows(out)
    assert count == len(SAMPLE_ROWS) == len(rows)
    assert list(rows[0].keys()) == COLUMNS
    assert rows[0] == {
        "source_row_id": "0", "city": "Delhi, India", "date": "29-Oct-14", "card_type": "Gold",
        "exp_type": "Bills", "gender": "F", "amount": "82475",
    }


def test_csv_input_with_different_header_spelling(tmp_path):
    source = tmp_path / "data.csv"
    source.write_text('CITY,date,card_type,EXP TYPE,Gender,Amount\n"Pune, India",1-Jan-15,Gold,Food,F,100\n')

    out = tmp_path / "records.csv"
    assert parse_raw_file(source, out) == 1
    row = read_rows(out)[0]
    assert row["city"] == "Pune, India"
    assert row["source_row_id"] == "0"  # generated from the line number


def test_missing_column_is_reported(tmp_path):
    rows = [{k: v for k, v in r.items() if k != "Amount"} for r in SAMPLE_ROWS]
    path = write_envelope(tmp_path / "bad.json", rows)
    with pytest.raises(InvalidDataError, match="amount"):
        parse_raw_file(path, tmp_path / "out.csv")


def test_invalid_json_is_reported(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text('{"status": "200", "json": ')
    with pytest.raises(InvalidDataError, match="not valid JSON"):
        parse_raw_file(path, tmp_path / "out.csv")


def test_error_status_in_envelope(tmp_path):
    path = tmp_path / "error.json"
    path.write_text(json.dumps({"status": "401", "message": "unauthorized", "json": "{}"}))
    with pytest.raises(InvalidDataError, match="401"):
        parse_raw_file(path, tmp_path / "out.csv")


def test_envelope_without_json_field(tmp_path):
    path = tmp_path / "other.json"
    path.write_text(json.dumps([{"City": "Delhi, India"}]))
    with pytest.raises(InvalidDataError, match="'json'"):
        parse_raw_file(path, tmp_path / "out.csv")
