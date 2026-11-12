"""Database connection (SQLAlchemy engine for SQL Server via pyodbc)."""

from sqlalchemy import create_engine


def create_db_engine(settings, database=None):
    # fast_executemany: pyodbc sends a whole batch of rows in one round trip
    # pool_pre_ping:    a dropped connection is replaced instead of failing
    return create_engine(
        settings.sqlalchemy_url(database),
        fast_executemany=True,
        pool_size=5,
        pool_pre_ping=True,
    )
