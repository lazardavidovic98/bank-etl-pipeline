"""Writes cleaned rows to the staging table and rejected rows to etl.rejected_record."""

import json
import logging
from decimal import Decimal

from bank_pipeline.utils.helpers import batched

logger = logging.getLogger(__name__)

INSERT_STAGING = """
INSERT INTO stg.transactions
    (run_id, source_row_id, city_name, country_name, transaction_date,
     card_type_name, expense_type_name, gender_code, amount)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_REJECTED = """
INSERT INTO etl.rejected_record (run_id, source_row_id, reasons, raw_data)
VALUES (?, ?, ?, ?)
"""


def clear_staging(engine):
    # TRUNCATE instead of DELETE: the whole table is emptied, nothing references
    # it with a foreign key and it keeps no history, so the faster TRUNCATE is safe.
    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE stg.transactions")


def insert_valid_rows(engine, run_id, valid, batch_size):
    """Insert a DataFrame of clean rows in batches. Returns the number of rows inserted."""
    rows = (
        (run_id, int(r.source_row_id), r.city_name, r.country_name, r.transaction_date,
         r.card_type_name, r.expense_type_name, r.gender_code, Decimal(f"{r.amount:.2f}"))
        for r in valid.itertuples(index=False)
    )
    count = 0
    for batch in batched(rows, batch_size):
        # Parameterized query + executemany; each batch is its own small transaction.
        with engine.begin() as conn:
            conn.exec_driver_sql(INSERT_STAGING, batch)
        count += len(batch)
    return count


def insert_rejected_rows(engine, run_id, rejected, batch_size):
    if rejected.empty:
        return 0
    rows = []
    for record in rejected.to_dict("records"):
        reasons = record.pop("reasons")
        raw = {key: (None if value != value else value) for key, value in record.items()}  # NaN -> null
        rows.append((run_id, str(raw.get("source_row_id"))[:50], reasons[:300], json.dumps(raw, default=str)))
    for batch in batched(rows, batch_size):
        with engine.begin() as conn:
            conn.exec_driver_sql(INSERT_REJECTED, batch)
    return len(rows)
