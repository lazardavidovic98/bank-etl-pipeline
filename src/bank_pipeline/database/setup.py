"""Creates the database and all tables/views (safe to run many times)."""

import logging

from bank_pipeline.database.engine import create_db_engine
from bank_pipeline.database.sql_files import SQL_DIR, run_sql_file

logger = logging.getLogger(__name__)


def create_database_if_missing(settings):
    """Connect to `master` and create the database if it does not exist yet."""
    engine = create_db_engine(settings, database="master")
    try:
        # CREATE DATABASE cannot run inside a transaction -> autocommit
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.exec_driver_sql("SELECT DB_ID(?)", (settings.db_name,)).scalar()
            if exists is None:
                logger.info("Creating database %s", settings.db_name)
                conn.exec_driver_sql(f"CREATE DATABASE [{settings.db_name}]")  # name is validated in config
                return True
            return False
    finally:
        engine.dispose()


def create_tables(engine):
    """Run sql/schema/*.sql in name order. Every script checks IF ... IS NULL first."""
    for path in sorted((SQL_DIR / "schema").glob("*.sql")):
        logger.info("Running %s", path.name)
        with engine.begin() as conn:
            run_sql_file(conn, f"schema/{path.name}")


def tables_exist(engine):
    with engine.connect() as conn:
        return conn.exec_driver_sql("SELECT OBJECT_ID('mart.agg_city_summary')").scalar() is not None
