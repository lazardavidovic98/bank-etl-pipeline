"""Validates and cleans the records, one chunk at a time.

The parsed CSV is read with pandas in chunks of CHUNK_SIZE rows, so memory
use stays roughly the same no matter how big the dataset is. All checks are
vectorised pandas operations (no Python loop over rows).

Each invalid row is kept together with the reason(s) it was rejected, so it
can be stored and inspected later. One bad row never stops the pipeline.
"""

from datetime import date

import pandas as pd

from bank_pipeline.transform.parser import COLUMNS

MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
          "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
GENDERS = {"F": "F", "M": "M", "FEMALE": "F", "MALE": "M"}


def to_number(series):
    """Text -> float; anything that is not a number becomes NaN."""
    return pd.to_numeric(series.astype(object), errors="coerce").astype("float64")


def clean_text(series):
    """Trim spaces, collapse double spaces, and turn empty strings into missing values."""
    series = series.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    return series.mask(series == "")


def parse_dates(series):
    """Parse dates like '29-Oct-14' without depending on the system locale."""
    parts = series.str.extract(r"^(\d{1,2})-([A-Za-z]{3})-(\d{2})$")
    day = to_number(parts[0])
    month = parts[1].str.lower().map(MONTHS).astype("float64")
    year = 2000 + to_number(parts[2])
    frame = pd.DataFrame({"year": year, "month": month, "day": day}).astype("float64")
    return pd.to_datetime(frame, errors="coerce")  # invalid dates such as 31-Feb become NaT


class RecordCleaner:
    """Cleans chunks and remembers which source ids were already accepted (duplicate check)."""

    def __init__(self, today=None):
        self.today = pd.Timestamp(today or date.today())
        self.seen_ids = set()

    def clean_chunk(self, chunk):
        """Return (valid_rows, rejected_rows) for one chunk."""
        chunk = chunk.reset_index(drop=True)
        text = {col: clean_text(chunk[col]) for col in COLUMNS}
        problems = {}

        for col in COLUMNS:
            problems[f"missing_{col}"] = text[col].isna()

        ids = to_number(text["source_row_id"])
        problems["invalid_id"] = text["source_row_id"].notna() & ~((ids >= 0) & (ids % 1 == 0))

        # "Delhi, India" -> city "Delhi", country "India"  (atomic values for 1NF)
        city_parts = text["city"].str.rsplit(",", n=1, expand=True).reindex(columns=[0, 1])
        city = clean_text(city_parts[0])
        country = clean_text(city_parts[1])
        problems["city_without_country"] = text["city"].notna() & (city.isna() | country.isna())

        dates = parse_dates(text["date"])
        problems["invalid_date"] = text["date"].notna() & dates.isna()
        problems["future_date"] = dates > self.today

        gender = text["gender"].str.upper().map(GENDERS)
        problems["invalid_gender"] = text["gender"].notna() & gender.isna()

        amount = to_number(text["amount"])
        problems["invalid_amount"] = text["amount"].notna() & ~(amount > 0)

        problems = pd.DataFrame(problems).fillna(False).astype(bool)
        is_invalid = problems.any(axis=1)

        # Duplicates: the first valid row with a given source id wins.
        valid_ids = ids.where(~is_invalid)
        duplicate = valid_ids.notna() & (valid_ids.duplicated() | valid_ids.isin(self.seen_ids))
        duplicate = duplicate.astype(bool)
        problems["duplicate_id"] = duplicate
        is_invalid = is_invalid | duplicate

        ok = ~is_invalid
        valid = pd.DataFrame({
            "source_row_id": ids[ok].astype("int64"),
            "city_name": city[ok].astype(object),
            "country_name": country[ok].astype(object),
            "transaction_date": dates[ok].dt.date,
            "card_type_name": text["card_type"][ok].str.title().astype(object),
            "expense_type_name": text["exp_type"][ok].str.title().astype(object),
            "gender_code": gender[ok].astype(object),
            "amount": amount[ok].round(2),
        })
        self.seen_ids.update(valid["source_row_id"].tolist())

        rejected = chunk[is_invalid].copy()
        rejected["reasons"] = [
            ";".join(name for name, failed in row.items() if failed)
            for _, row in problems[is_invalid].iterrows()
        ]
        return valid, rejected


def read_clean_chunks(csv_path, chunk_size, cleaner=None):
    """Read `csv_path` in chunks and yield (valid_rows, rejected_rows) for each chunk."""
    cleaner = cleaner or RecordCleaner()
    # dtype=str: read everything as text and convert explicitly in clean_chunk
    # keep_default_na=False: only empty cells count as missing (not the text "NA")
    reader = pd.read_csv(csv_path, dtype=str, keep_default_na=False, chunksize=chunk_size)
    with reader:
        for chunk in reader:
            yield cleaner.clean_chunk(chunk)
