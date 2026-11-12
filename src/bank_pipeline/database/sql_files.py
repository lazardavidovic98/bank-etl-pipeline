"""Helpers for running the .sql files from the sql/ folder."""

import logging
import re

from sqlalchemy import text

from bank_pipeline.config import PROJECT_ROOT

logger = logging.getLogger(__name__)

SQL_DIR = PROJECT_ROOT / "sql"
GO_LINE = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)
STEP_NAME = re.compile(r"^--\s*step:\s*(\w+)", re.MULTILINE)


def split_batches(sql):
    """Split a script on 'GO' lines (like SSMS does) and drop empty parts."""
    return [part.strip() for part in GO_LINE.split(sql) if part.strip()]


def run_sql_file(conn, relative_path, params=None):
    """
    Run every batch of a .sql file on `conn` (the caller controls the transaction).
    Returns {step name: affected rows} for batches marked with '-- step: <name>'.
    """
    sql = (SQL_DIR / relative_path).read_text(encoding="utf-8")
    results = {}
    for batch in split_batches(sql):
        result = conn.execute(text(batch), params or {})
        step = STEP_NAME.search(batch)
        if step:
            results[step.group(1)] = result.rowcount
            logger.debug("%s: %s -> %d rows", relative_path, step.group(1), result.rowcount)
    return results
