"""Logging setup: console + rotating log files, with secrets masked."""

import logging
import re
from logging.handlers import RotatingFileHandler

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
BEARER_PATTERN = re.compile(r"(Bearer\s+)\S+", re.IGNORECASE)


class SecretFormatter(logging.Formatter):
    """Formatter that replaces secrets (token, password, Bearer header) with ***."""

    def __init__(self, secrets=()):
        super().__init__(LOG_FORMAT)
        self.secrets = [s for s in secrets if s]

    def format(self, record):
        text = super().format(record)
        for secret in self.secrets:
            text = text.replace(secret, "***")
        return BEARER_PATTERN.sub(r"\1***", text)


def setup_logging(log_dir, level="INFO", secrets=(), max_bytes=5_000_000, backup_count=10):
    """
    logs/pipeline.log - all messages at `level` and above
    logs/errors.log   - only WARNING and above
    Files are rotated when they reach `max_bytes`, keeping `backup_count` old files.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = SecretFormatter(secrets)

    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    main_file = RotatingFileHandler(
        log_dir / "pipeline.log", maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    error_file = RotatingFileHandler(
        log_dir / "errors.log", maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    error_file.setLevel(logging.WARNING)
    console = logging.StreamHandler()

    for handler in (main_file, error_file, console):
        handler.setFormatter(formatter)
        root.addHandler(handler)

    # third-party libraries are too chatty at DEBUG level
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
