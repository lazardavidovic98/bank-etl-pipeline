"""Creates the database (if needed) and all tables and views.

    python scripts/setup_database.py

run_pipeline.py also does this automatically, so this script is only needed
when you want to prepare the database without loading data.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bank_pipeline.config import load_settings  # noqa: E402
from bank_pipeline.database.engine import create_db_engine  # noqa: E402
from bank_pipeline.database.setup import create_database_if_missing, create_tables  # noqa: E402
from bank_pipeline.utils.logger import setup_logging  # noqa: E402


def main():
    settings = load_settings()
    setup_logging(settings.log_dir, settings.log_level, secrets=settings.secrets())
    logger = logging.getLogger("bank_pipeline")

    try:
        created = create_database_if_missing(settings)
        engine = create_db_engine(settings)
        create_tables(engine)
        engine.dispose()
    except Exception:
        logger.exception("Database setup failed")
        return 1

    logger.info("Database %s is ready%s", settings.db_name, " (newly created)" if created else "")
    return 0


if __name__ == "__main__":
    sys.exit(main())
