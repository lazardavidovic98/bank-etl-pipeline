"""Turns the raw API response into a simple row-per-line CSV file.

The API returns an "envelope" whose `json` field is a JSON *string* in pandas
column format:

    {"status": "200", "message": "ok",
     "json": "{\"index\": {\"0\": 0, ...}, \"City\": {\"0\": \"Delhi, India\", ...}, ...}"}

Because the data is column-oriented and embedded as a string, it has to be
decoded once as a whole. We do that here, write the rows straight to a CSV file
(one row at a time) and then drop the decoded data. All later steps read that
CSV in chunks, so they never hold the whole dataset in memory.

A local CSV file with the same columns is also accepted.
"""

import csv
import json
import logging

from bank_pipeline.errors import InvalidDataError

logger = logging.getLogger(__name__)

# Column names used inside the pipeline.
COLUMNS = ["source_row_id", "city", "date", "card_type", "exp_type", "gender", "amount"]

# Source column name (lower-case) -> pipeline column name.
SOURCE_COLUMNS = {
    "index": "source_row_id",
    "city": "city",
    "date": "date",
    "card type": "card_type",
    "exp type": "exp_type",
    "gender": "gender",
    "amount": "amount",
}


def normalize_name(name):
    """'Card Type' / 'card_type' / ' CARD  TYPE ' -> 'card type'"""
    return " ".join(str(name).replace("_", " ").lower().split())


def map_columns(names):
    """Return {source name: pipeline name} and check that all required columns exist."""
    mapping = {}
    for name in names:
        target = SOURCE_COLUMNS.get(normalize_name(name))
        if target:
            mapping[name] = target
    missing = set(COLUMNS) - set(mapping.values()) - {"source_row_id"}
    if missing:
        raise InvalidDataError(f"Missing column(s) in the data: {', '.join(sorted(missing))}")
    return mapping


def parse_raw_file(raw_path, output_csv):
    """Convert the raw file (.json envelope or .csv) to `output_csv`. Returns the row count."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    if raw_path.suffix.lower() == ".csv":
        rows = read_csv_rows(raw_path)
    else:
        rows = read_envelope_rows(raw_path)

    count = 0
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1

    logger.info("Parsed %d records into %s", count, output_csv.name)
    return count


def read_envelope_rows(path):
    """Yield one dict per transaction from the API envelope."""
    try:
        with open(path, encoding="utf-8-sig") as f:
            envelope = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidDataError(f"Response is not valid JSON: {exc}") from exc

    if not isinstance(envelope, dict) or "json" not in envelope:
        raise InvalidDataError("Unexpected response format: field 'json' is missing")
    if str(envelope.get("status", "200")) != "200":
        raise InvalidDataError(f"API reported status {envelope.get('status')}: {envelope.get('message')}")

    try:
        data = envelope["json"]
        if isinstance(data, str):
            data = json.loads(data)
    except json.JSONDecodeError as exc:
        raise InvalidDataError(f"Field 'json' is not valid JSON: {exc}") from exc
    del envelope  # the big JSON string is no longer needed

    if not isinstance(data, dict) or not all(isinstance(v, dict) for v in data.values()):
        raise InvalidDataError("Field 'json' is not in pandas column format {column: {row: value}}")

    mapping = map_columns(data.keys())
    columns = {target: data[source] for source, target in mapping.items()}
    row_keys = next(iter(columns.values())).keys()

    for key in row_keys:
        row = {target: values.get(key) for target, values in columns.items()}
        if row.get("source_row_id") is None:
            row["source_row_id"] = key
        yield row


def read_csv_rows(path):
    """Yield one dict per row from a local CSV file."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        mapping = map_columns(reader.fieldnames or [])
        for line_number, record in enumerate(reader):
            row = {target: record.get(source) for source, target in mapping.items()}
            row.setdefault("source_row_id", line_number)
            yield row
