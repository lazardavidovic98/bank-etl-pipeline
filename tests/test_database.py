"""Integration tests against a real SQL Server.

They use a separate database (bank_test), which is dropped and re-created for
every test. If SQL Server is not reachable the tests are skipped.
"""

from dataclasses import replace

import pytest
from sqlalchemy import text

from bank_pipeline.config import load_settings
from bank_pipeline.database.engine import create_db_engine
from bank_pipeline.database.setup import create_database_if_missing, create_tables
from bank_pipeline.errors import InvalidDataError, LoadError, SourceUnavailableError
from bank_pipeline.load.core import load_core
from bank_pipeline.pipeline import run_pipeline
from conftest import SAMPLE_ROWS, write_envelope

pytestmark = pytest.mark.database

TEST_DB = "bank_test"


@pytest.fixture
def settings(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_NAME", TEST_DB)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("SOURCE_MODE", "file")
    settings = load_settings()

    master = create_db_engine(settings, database="master")
    try:
        with master.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.exec_driver_sql(
                f"IF DB_ID('{TEST_DB}') IS NOT NULL BEGIN "
                f"ALTER DATABASE [{TEST_DB}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"DROP DATABASE [{TEST_DB}]; END"
            )
    except Exception as exc:  # no server -> skip instead of failing
        pytest.skip(f"SQL Server not available: {exc}")
    finally:
        master.dispose()
    return settings


@pytest.fixture
def engine(settings):
    engine = create_db_engine(settings)
    yield engine
    engine.dispose()


def scalar(engine, sql):
    with engine.connect() as conn:
        return conn.execute(text(sql)).scalar()


def run_with(settings, tmp_path, rows, name="data.json"):
    return run_pipeline(settings, local_file=write_envelope(tmp_path / name, rows))


def test_full_pipeline_loads_core_and_datamart(settings, engine, tmp_path):
    stats = run_with(settings, tmp_path, SAMPLE_ROWS)

    assert stats["extracted"] == stats["valid"] == stats["inserted"] == 5
    assert stats["cities"] == 3
    assert scalar(engine, "SELECT COUNT(*) FROM core.transactions") == 5
    assert scalar(engine, "SELECT COUNT(*) FROM core.country") == 1
    assert scalar(engine, "SELECT COUNT(*) FROM mart.fact_transactions") == 5

    with engine.connect() as conn:
        top = conn.execute(text(
            "SELECT city_name, transaction_count, percent_of_transactions, rank_by_count, total_amount "
            "FROM mart.agg_city_summary WHERE rank_by_count = 1"
        )).one()
        status = conn.execute(text("SELECT status, rows_inserted FROM etl.pipeline_run")).one()
    assert (top.city_name, top.transaction_count, float(top.percent_of_transactions)) == ("Delhi", 3, 60.0)
    assert float(top.total_amount) == 82475 + 171574 + 1500
    assert tuple(status) == ("success", 5)


def test_second_run_is_idempotent(settings, engine, tmp_path):
    run_with(settings, tmp_path, SAMPLE_ROWS)
    stats = run_with(settings, tmp_path, SAMPLE_ROWS)

    assert (stats["inserted"], stats["updated"]) == (0, 0)
    assert scalar(engine, "SELECT COUNT(*) FROM core.transactions") == 5
    assert scalar(engine, "SELECT COUNT(*) FROM core.city") == 3
    assert scalar(engine, "SELECT COUNT(*) FROM mart.fact_transactions") == 5


def test_changed_and_new_rows(settings, engine, tmp_path):
    run_with(settings, tmp_path, SAMPLE_ROWS)
    changed = [dict(r) for r in SAMPLE_ROWS]
    changed[0]["Amount"] = 99999
    changed.append({**SAMPLE_ROWS[0], "index": 5, "City": "Pune, India"})

    stats = run_with(settings, tmp_path, changed, "changed.json")

    assert (stats["inserted"], stats["updated"]) == (1, 1)
    assert scalar(engine, "SELECT amount FROM core.transactions WHERE source_row_id = 0") == 99999
    assert scalar(engine, "SELECT COUNT(*) FROM mart.agg_city_summary") == 4


def test_invalid_rows_are_stored(settings, engine, tmp_path):
    # 20 valid rows + 1 invalid row (under the default MAX_REJECT_RATIO of 10 %)
    rows = [{**row, "index": i} for i, row in enumerate(SAMPLE_ROWS * 4)]
    rows.append({**SAMPLE_ROWS[0], "index": 100, "Gender": "X"})

    stats = run_with(settings, tmp_path, rows)

    assert (stats["valid"], stats["rejected"]) == (20, 1)
    assert scalar(engine, "SELECT reasons FROM etl.rejected_record") == "invalid_gender"
    assert list((settings.data_dir / "rejected").glob("*.csv"))


def test_too_many_invalid_rows_stop_the_run(settings, engine, tmp_path):
    rows = [{**r, "Amount": -1} for r in SAMPLE_ROWS[:3]] + SAMPLE_ROWS[3:]
    with pytest.raises(InvalidDataError, match="invalid"):
        run_with(settings, tmp_path, rows)

    assert scalar(engine, "SELECT COUNT(*) FROM core.transactions") == 0  # core was not touched
    assert scalar(engine, "SELECT status FROM etl.pipeline_run") == "failed"


def test_api_down_marks_run_as_failed(settings, engine, monkeypatch):
    monkeypatch.setattr(
        "bank_pipeline.extract.download_dataset",
        lambda *a, **k: (_ for _ in ()).throw(SourceUnavailableError("API not available")),
    )
    settings = replace(settings, source_url="http://api", source_token="t")
    with pytest.raises(SourceUnavailableError):
        run_pipeline(settings, source_mode="api")

    with engine.connect() as conn:
        row = conn.execute(text("SELECT status, error_message FROM etl.pipeline_run")).one()
    assert row.status == "failed" and "API not available" in row.error_message


def test_core_load_is_rolled_back_on_error(settings, engine):
    create_database_if_missing(settings)
    create_tables(engine)
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO etl.pipeline_run (status) VALUES ('running')"))
        # 'X' is not in core.gender -> the final INSERT fails on the foreign key,
        # after the new country and city were already inserted in the same transaction.
        conn.execute(text(
            "INSERT INTO stg.transactions VALUES (1, 1, 'Testville', 'Testland', '2015-01-01', "
            "'Gold', 'Food', 'X', 10)"
        ))

    with pytest.raises(LoadError, match="rolled back"):
        load_core(engine, 1)

    assert scalar(engine, "SELECT COUNT(*) FROM core.country WHERE country_name = 'Testland'") == 0
    assert scalar(engine, "SELECT COUNT(*) FROM core.city") == 0


def test_schema_is_normalized(settings, engine, tmp_path):
    run_with(settings, tmp_path, SAMPLE_ROWS)

    # 3NF: the country is stored once per city, not on every transaction
    columns = scalar(engine, "SELECT STRING_AGG(name, ',') FROM sys.columns WHERE object_id = OBJECT_ID('core.transactions')")
    assert "country" not in columns and "city_name" not in columns
    # every city belongs to exactly one country and city names are not repeated per country
    assert scalar(engine, "SELECT COUNT(*) FROM (SELECT city_name, country_id FROM core.city "
                          "GROUP BY city_name, country_id HAVING COUNT(*) > 1) d") == 0
    # all relations are enforced by foreign keys
    assert scalar(engine, "SELECT COUNT(*) FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('core.transactions')") == 4
