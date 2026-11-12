"""Configuration loaded from environment variables / the .env file.

Secrets (API token, DB password) have no default values and are never
printed: they are excluded from repr() and filtered out of the logs.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

from bank_pipeline.errors import ConfigError

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _env(name, default=None):
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _env_int(name, default):
    value = _env(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got {value!r}") from None


def _env_bool(name, default):
    value = _env(name)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")


def _env_path(name, default):
    path = Path(_env(name, default))
    return path if path.is_absolute() else PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    # data source
    source_mode: str  # "api", "file" or "auto"
    source_url: str | None
    source_token: str | None = field(repr=False)
    local_file: Path
    http_timeout: int
    http_retries: int
    http_backoff: float
    max_download_mb: int

    # database
    db_driver: str
    db_server: str
    db_name: str
    db_trusted_connection: bool
    db_user: str | None
    db_password: str | None = field(repr=False)
    db_encrypt: bool
    db_trust_server_certificate: bool

    # processing
    chunk_size: int
    batch_size: int
    max_reject_ratio: float

    # paths and logging
    data_dir: Path
    log_dir: Path
    log_level: str
    keep_runs: int

    def connection_string(self, database=None):
        """ODBC connection string for pyodbc."""
        parts = [
            f"DRIVER={{{self.db_driver}}}",
            f"SERVER={self.db_server}",
            f"DATABASE={database or self.db_name}",
            f"Encrypt={'yes' if self.db_encrypt else 'no'}",
            f"TrustServerCertificate={'yes' if self.db_trust_server_certificate else 'no'}",
        ]
        if self.db_trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={self.db_user}")
            parts.append(f"PWD={{{self.db_password}}}")
        return ";".join(parts) + ";"

    def sqlalchemy_url(self, database=None):
        return "mssql+pyodbc:///?odbc_connect=" + quote_plus(self.connection_string(database))

    def secrets(self):
        """Values that must never be written to the logs."""
        return [s for s in (self.source_token, self.db_password) if s]


def load_settings():
    load_dotenv(PROJECT_ROOT / ".env", override=False)

    source_mode = _env("SOURCE_MODE", "auto").lower()
    if source_mode not in ("api", "file", "auto"):
        raise ConfigError("SOURCE_MODE must be 'api', 'file' or 'auto'")

    settings = Settings(
        source_mode=source_mode,
        source_url=_env("SOURCE_URL"),
        source_token=_env("SOURCE_TOKEN"),
        local_file=_env_path("LOCAL_DATA_FILE", "data/sample/transactions_snapshot.json"),
        http_timeout=_env_int("HTTP_TIMEOUT", 30),
        http_retries=_env_int("HTTP_RETRIES", 3),
        http_backoff=float(_env("HTTP_BACKOFF_SECONDS", "2")),
        max_download_mb=_env_int("MAX_DOWNLOAD_MB", 200),
        db_driver=_env("DB_DRIVER", "ODBC Driver 18 for SQL Server"),
        db_server=_env("DB_SERVER", r"localhost\SQLEXPRESS"),
        db_name=_env("DB_NAME", "bank"),
        db_trusted_connection=_env_bool("DB_TRUSTED_CONNECTION", True),
        db_user=_env("DB_USER"),
        db_password=_env("DB_PASSWORD"),
        db_encrypt=_env_bool("DB_ENCRYPT", True),
        db_trust_server_certificate=_env_bool("DB_TRUST_SERVER_CERTIFICATE", True),
        chunk_size=_env_int("CHUNK_SIZE", 5000),
        batch_size=_env_int("BATCH_SIZE", 1000),
        max_reject_ratio=float(_env("MAX_REJECT_RATIO", "0.10")),
        data_dir=_env_path("DATA_DIR", "data"),
        log_dir=_env_path("LOG_DIR", "logs"),
        log_level=_env("LOG_LEVEL", "INFO").upper(),
        keep_runs=_env_int("KEEP_RUNS", 7),
    )

    if settings.source_mode == "api" and not (settings.source_url and settings.source_token):
        raise ConfigError("SOURCE_MODE=api requires SOURCE_URL and SOURCE_TOKEN")
    if not settings.db_trusted_connection and not (settings.db_user and settings.db_password):
        raise ConfigError("DB_USER and DB_PASSWORD are required when DB_TRUSTED_CONNECTION=false")
    if not settings.db_name.replace("_", "").isalnum():
        raise ConfigError("DB_NAME may contain only letters, digits and '_'")
    if settings.chunk_size < 1 or settings.batch_size < 1:
        raise ConfigError("CHUNK_SIZE and BATCH_SIZE must be positive")
    return settings
