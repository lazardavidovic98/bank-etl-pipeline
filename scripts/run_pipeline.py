"""Entry point: runs the whole pipeline once.

    python scripts/run_pipeline.py                  # source from SOURCE_MODE in .env
    python scripts/run_pipeline.py --source file    # use the local dataset
    python scripts/run_pipeline.py --check-source   # only check if the API responds

Exit codes: 0 = success, 1 = pipeline failed, 2 = configuration error, 3 = already running.
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bank_pipeline.config import load_settings  # noqa: E402
from bank_pipeline.errors import AlreadyRunningError, ConfigError  # noqa: E402
from bank_pipeline.extract.api_client import check_source  # noqa: E402
from bank_pipeline.pipeline import run_pipeline  # noqa: E402
from bank_pipeline.utils.lock import PipelineLock  # noqa: E402
from bank_pipeline.utils.logger import setup_logging  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Bank database ETL pipeline")
    parser.add_argument("--source", choices=["api", "file", "auto"], help="override SOURCE_MODE")
    parser.add_argument("--file", type=Path, help="local .json or .csv file to load (implies --source file)")
    parser.add_argument("--check-source", action="store_true", help="only check if the API is reachable")
    args = parser.parse_args()

    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    setup_logging(settings.log_dir, settings.log_level, secrets=settings.secrets())
    logger = logging.getLogger("bank_pipeline")

    if args.check_source:
        if not (settings.source_url and settings.source_token):
            print("SOURCE_URL / SOURCE_TOKEN are not set in .env")
            return 2
        print(f"{settings.source_url}: {check_source(settings.source_url, settings.source_token)}")
        return 0

    source_mode = "file" if args.file else args.source
    try:
        with PipelineLock(settings.log_dir / "pipeline.lock"):
            stats = run_pipeline(settings, source_mode=source_mode, local_file=args.file)
    except AlreadyRunningError as exc:
        logger.warning("%s - exiting", exc)
        return 3
    except Exception:
        return 1  # details are already in logs/pipeline.log and logs/errors.log

    print(
        f"\nRun {stats['run_id']} OK | source: {stats['source']}"
        f"{' (fallback)' if stats['used_fallback'] else ''} | extracted: {stats['extracted']} | "
        f"valid: {stats['valid']} | rejected: {stats['rejected']} | inserted: {stats['inserted']} | "
        f"updated: {stats['updated']} | cities: {stats['cities']} | {stats['duration']} s"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
