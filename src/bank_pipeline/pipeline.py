"""Runs the whole ETL process: extract -> transform -> load -> data mart."""

import logging
import time
from contextlib import contextmanager
from datetime import datetime

from bank_pipeline.database.engine import create_db_engine
from bank_pipeline.database.setup import create_database_if_missing, create_tables
from bank_pipeline.errors import InvalidDataError, PipelineError
from bank_pipeline.extract import get_dataset
from bank_pipeline.load.core import load_core, refresh_datamart
from bank_pipeline.load.run_log import finish_run, start_run
from bank_pipeline.load.staging import clear_staging, insert_rejected_rows, insert_valid_rows
from bank_pipeline.transform import parse_raw_file, read_clean_chunks
from bank_pipeline.utils.helpers import remove_old_runs

logger = logging.getLogger("bank_pipeline")


@contextmanager
def step(name):
    """Log the start, end and duration of a pipeline step."""
    logger.info("--- %s: started", name)
    started = time.perf_counter()
    try:
        yield
    except Exception:
        logger.error("--- %s: FAILED after %.2f s", name, time.perf_counter() - started)
        raise
    logger.info("--- %s: finished in %.2f s", name, time.perf_counter() - started)


def run_pipeline(settings, source_mode=None, local_file=None):
    """Run all steps once. Returns a dict with the run statistics."""
    started = time.perf_counter()
    logger.info("===== Pipeline started =====")

    try:
        create_database_if_missing(settings)
        engine = create_db_engine(settings)
        create_tables(engine)  # does nothing if the tables already exist
        run_id = start_run(engine)
    except Exception as exc:
        # Without a database there is nowhere to load the data - stop here.
        logger.critical("===== Pipeline FAILED: cannot prepare the database: %s =====", exc, exc_info=True)
        raise

    run_name = f"run_{run_id}_{datetime.now():%Y%m%d_%H%M%S}"
    stats = {"run_id": run_id}
    raw_dir = settings.data_dir / "raw"
    processed_dir = settings.data_dir / "processed"
    rejected_dir = settings.data_dir / "rejected"

    try:
        with step("Extract"):
            result = get_dataset(settings, raw_dir, run_name, mode=source_mode, local_file=local_file)
            stats.update(source=result.source, source_location=result.location, used_fallback=result.used_fallback)
            if result.used_fallback:
                logger.warning("API was not available - this run uses the local file %s", result.location)

        with step("Parse"):
            records_csv = processed_dir / f"{run_name}.csv"
            stats["extracted"] = parse_raw_file(result.path, records_csv)

        with step("Transform + load staging"):
            clear_staging(engine)
            stats["valid"] = stats["rejected"] = 0
            rejected_csv = rejected_dir / f"{run_name}.csv"
            for valid, rejected in read_clean_chunks(records_csv, settings.chunk_size):
                stats["valid"] += insert_valid_rows(engine, run_id, valid, settings.batch_size)
                if not rejected.empty:
                    stats["rejected"] += insert_rejected_rows(engine, run_id, rejected, settings.batch_size)
                    rejected_dir.mkdir(parents=True, exist_ok=True)
                    rejected.to_csv(rejected_csv, mode="a", index=False, header=not rejected_csv.exists())
                    logger.warning("%d invalid record(s) in this chunk, saved to %s", len(rejected), rejected_csv.name)
                logger.debug("Chunk done: %d valid, %d rejected", len(valid), len(rejected))
            logger.info("Records: %d extracted, %d valid, %d rejected",
                        stats["extracted"], stats["valid"], stats["rejected"])

        # Stop before touching the core tables if the data looks broken.
        if stats["valid"] == 0:
            raise InvalidDataError("No valid records - nothing to load")
        reject_ratio = stats["rejected"] / stats["extracted"]
        if reject_ratio > settings.max_reject_ratio:
            raise InvalidDataError(
                f"{reject_ratio:.1%} of records are invalid (limit MAX_REJECT_RATIO={settings.max_reject_ratio:.0%})"
            )

        with step("Load core (3NF)"):
            core = load_core(engine, run_id)
            stats["inserted"], stats["updated"] = core["inserted"], core["updated"]

        with step("Refresh Data Mart"):
            mart = refresh_datamart(engine)
            stats["cities"] = mart["city_rows"]

        duration = time.perf_counter() - started
        finish_run(engine, run_id, "success", stats, duration)
        stats["duration"] = round(duration, 2)
        logger.info("===== Pipeline finished successfully in %.2f s =====", duration)
        return stats

    except Exception as exc:
        duration = time.perf_counter() - started
        # Expected errors (API down, bad data...) get a short message, unexpected ones a full traceback.
        logger.critical("===== Pipeline FAILED: %s =====", exc, exc_info=not isinstance(exc, PipelineError))
        try:
            finish_run(engine, run_id, "failed", stats, duration, error=str(exc))
        except Exception:
            logger.error("Could not save the failed status to etl.pipeline_run", exc_info=True)
        raise

    finally:
        for directory in (raw_dir, processed_dir, rejected_dir):
            remove_old_runs(directory, settings.keep_runs)
        engine.dispose()
