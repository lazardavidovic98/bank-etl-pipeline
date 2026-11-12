"""Loads staging data into the normalized core tables and refreshes the Data Mart."""

import logging

from sqlalchemy.exc import SQLAlchemyError

from bank_pipeline.database.sql_files import run_sql_file
from bank_pipeline.errors import LoadError

logger = logging.getLogger(__name__)


def load_core(engine, run_id):
    """Run sql/transformations/load_core.sql in one transaction.

    engine.begin() commits when the block finishes and rolls back if any
    statement raises, so the core tables are never left half-loaded.
    """
    try:
        with engine.begin() as conn:
            steps = run_sql_file(conn, "transformations/load_core.sql", {"run_id": run_id})
    except SQLAlchemyError as exc:
        raise LoadError(f"Core load failed and was rolled back: {exc}") from exc
    logger.info(
        "Core load: %d inserted, %d updated (new cities: %d, new countries: %d)",
        steps["inserted"], steps["updated"], steps["new_cities"], steps["new_countries"],
    )
    return steps


def refresh_datamart(engine):
    """Rebuild the Data Mart in one transaction and check that the totals match core."""
    try:
        with engine.begin() as conn:
            steps = run_sql_file(conn, "datamart/refresh_datamart.sql")

            core_rows = conn.exec_driver_sql("SELECT COUNT(*) FROM core.transactions").scalar()
            city_total = conn.exec_driver_sql(
                "SELECT ISNULL(SUM(transaction_count), 0) FROM mart.agg_city_summary"
            ).scalar()
            if not (core_rows == steps["fact_rows"] == city_total):
                # raising inside the `with` block rolls the whole refresh back
                raise LoadError(
                    f"Data Mart totals do not match: core={core_rows}, "
                    f"fact={steps['fact_rows']}, city summary={city_total}"
                )
    except SQLAlchemyError as exc:
        raise LoadError(f"Data Mart refresh failed and was rolled back: {exc}") from exc
    logger.info("Data Mart refreshed: %d fact rows, %d cities", steps["fact_rows"], steps["city_rows"])
    return steps
