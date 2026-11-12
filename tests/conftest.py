import json

import pytest

# A few transactions in the same format as the original dataset.
SAMPLE_ROWS = [
    {"index": 0, "City": "Delhi, India", "Date": "29-Oct-14", "Card Type": "Gold", "Exp Type": "Bills", "Gender": "F", "Amount": 82475},
    {"index": 1, "City": "Greater Mumbai, India", "Date": "22-Aug-14", "Card Type": "Platinum", "Exp Type": "Bills", "Gender": "F", "Amount": 32555},
    {"index": 2, "City": "Bengaluru, India", "Date": "27-Aug-14", "Card Type": "Silver", "Exp Type": "Food", "Gender": "M", "Amount": 101738},
    {"index": 3, "City": "Delhi, India", "Date": "5-May-15", "Card Type": "Signature", "Exp Type": "Fuel", "Gender": "M", "Amount": 171574},
    {"index": 4, "City": "Delhi, India", "Date": "12-Apr-14", "Card Type": "Gold", "Exp Type": "Food", "Gender": "F", "Amount": 1500},
]


def write_envelope(path, rows):
    """Write rows the way the API returns them: an envelope with a pandas column-format JSON string."""
    columns = {}
    for i, row in enumerate(rows):
        for key, value in row.items():
            columns.setdefault(key, {})[str(i)] = value
    envelope = {"status": "200", "message": "ok", "json": json.dumps(columns)}
    path.write_text(json.dumps(envelope), encoding="utf-8")
    return path


@pytest.fixture
def envelope_file(tmp_path):
    return write_envelope(tmp_path / "dataset.json", SAMPLE_ROWS)
