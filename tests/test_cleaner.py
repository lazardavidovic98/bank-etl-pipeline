from datetime import date

import pandas as pd

from bank_pipeline.transform.cleaner import RecordCleaner, read_clean_chunks
from bank_pipeline.transform.parser import COLUMNS


def make_chunk(*rows):
    return pd.DataFrame([dict(zip(COLUMNS, row)) for row in rows], dtype=str)


GOOD = ("1", "Delhi, India", "29-Oct-14", "Gold", "Bills", "F", "82475")


def test_valid_row_is_cleaned_and_split():
    valid, rejected = RecordCleaner().clean_chunk(make_chunk(GOOD))

    assert rejected.empty
    row = valid.iloc[0]
    assert row.source_row_id == 1
    assert (row.city_name, row.country_name) == ("Delhi", "India")
    assert row.transaction_date == date(2014, 10, 29)
    assert (row.card_type_name, row.expense_type_name, row.gender_code) == ("Gold", "Bills", "F")
    assert row.amount == 82475.0


def test_text_is_normalized():
    messy = ("2", "  Greater   Mumbai ,  India ", "5-may-15", "gold", " exp ", "female", "10.5")
    valid, rejected = RecordCleaner().clean_chunk(make_chunk(messy))

    assert rejected.empty
    row = valid.iloc[0]
    assert (row.city_name, row.country_name) == ("Greater Mumbai", "India")
    assert row.transaction_date == date(2015, 5, 5)
    assert (row.card_type_name, row.expense_type_name, row.gender_code) == ("Gold", "Exp", "F")


def test_missing_values_are_rejected_with_reason():
    valid, rejected = RecordCleaner().clean_chunk(make_chunk(("3", "", "1-Jan-15", "Gold", "Food", "M", "")))

    assert valid.empty
    assert rejected.iloc[0]["reasons"] == "missing_city;missing_amount"


def test_invalid_values_are_rejected():
    rows = [
        ("10", "Delhi", "1-Jan-15", "Gold", "Food", "M", "100"),        # no country
        ("11", "Delhi, India", "31-Feb-15", "Gold", "Food", "M", "100"),  # date does not exist
        ("12", "Delhi, India", "2015/01/01", "Gold", "Food", "M", "100"),  # wrong date format
        ("13", "Delhi, India", "1-Jan-15", "Gold", "Food", "X", "100"),   # unknown gender
        ("14", "Delhi, India", "1-Jan-15", "Gold", "Food", "M", "-5"),    # negative amount
        ("15", "Delhi, India", "1-Jan-15", "Gold", "Food", "M", "abc"),   # not a number
        ("x", "Delhi, India", "1-Jan-15", "Gold", "Food", "M", "100"),    # bad id
    ]
    valid, rejected = RecordCleaner().clean_chunk(make_chunk(*rows))

    assert valid.empty
    assert rejected["reasons"].tolist() == [
        "city_without_country",
        "invalid_date",
        "invalid_date",
        "invalid_gender",
        "invalid_amount",
        "invalid_amount",
        "invalid_id",
    ]


def test_future_date_is_rejected():
    cleaner = RecordCleaner(today=date(2014, 1, 1))
    valid, rejected = cleaner.clean_chunk(make_chunk(GOOD))
    assert valid.empty
    assert rejected.iloc[0]["reasons"] == "future_date"


def test_duplicates_within_and_between_chunks():
    cleaner = RecordCleaner()
    first = ("1", "Delhi, India", "1-Jan-15", "Gold", "Food", "M", "100")
    same_id = ("1", "Pune, India", "2-Jan-15", "Gold", "Food", "M", "200")

    valid1, rejected1 = cleaner.clean_chunk(make_chunk(first, same_id))
    valid2, rejected2 = cleaner.clean_chunk(make_chunk(same_id))  # next chunk, same id again

    assert valid1["city_name"].tolist() == ["Delhi"]  # first one wins
    assert rejected1["reasons"].tolist() == ["duplicate_id"]
    assert valid2.empty and rejected2["reasons"].tolist() == ["duplicate_id"]


def test_invalid_first_row_does_not_block_valid_duplicate():
    bad = ("7", "Delhi, India", "1-Jan-15", "Gold", "Food", "M", "-1")
    good = ("7", "Delhi, India", "1-Jan-15", "Gold", "Food", "M", "50")
    valid, rejected = RecordCleaner().clean_chunk(make_chunk(bad, good))
    assert valid["amount"].tolist() == [50.0]
    assert rejected["reasons"].tolist() == ["invalid_amount"]


def test_chunk_size_does_not_change_the_result(tmp_path):
    rows = [(str(i), f"City{i % 7}, India", "1-Jan-15", "Gold", "Food", "F", str(100 + i)) for i in range(50)]
    rows += [("3", "Delhi, India", "1-Jan-15", "Gold", "Food", "F", "1")]  # duplicate of id 3
    rows += [("99", "Delhi", "1-Jan-15", "Gold", "Food", "F", "1")]  # invalid
    path = tmp_path / "records.csv"
    make_chunk(*rows).to_csv(path, index=False)

    def run(chunk_size):
        chunks = list(read_clean_chunks(path, chunk_size))
        valid = pd.concat([v for v, _ in chunks], ignore_index=True)
        rejected = pd.concat([r for _, r in chunks], ignore_index=True)
        return len(chunks), valid, rejected

    n_big, valid_big, rejected_big = run(1000)
    n_small, valid_small, rejected_small = run(7)

    assert (n_big, n_small) == (1, 8)
    assert len(valid_big) == 50 and len(rejected_big) == 2
    pd.testing.assert_frame_equal(valid_big, valid_small)
    assert rejected_big["reasons"].tolist() == rejected_small["reasons"].tolist()
