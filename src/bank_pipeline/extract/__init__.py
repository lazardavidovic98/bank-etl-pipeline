"""Extract step: get the raw dataset into data/raw/.

SOURCE_MODE decides where the data comes from:
  api  - only the HTTP API; if it fails, the run fails
  file - only the local file (LOCAL_DATA_FILE)
  auto - try the API first; if it fails, log a warning and use the local file
"""

import logging
import shutil
from dataclasses import dataclass

from bank_pipeline.errors import ExtractError
from bank_pipeline.extract.api_client import download_dataset

logger = logging.getLogger(__name__)


@dataclass
class ExtractResult:
    path: object  # Path of the raw file in data/raw/
    source: str  # "api" or "file"
    location: str  # URL or file path (never contains the token)
    size_bytes: int
    used_fallback: bool = False


def get_dataset(settings, raw_dir, run_name, mode=None, local_file=None):
    mode = mode or settings.source_mode
    local_file = local_file or settings.local_file
    raw_dir.mkdir(parents=True, exist_ok=True)

    if mode in ("api", "auto"):
        if settings.source_url and settings.source_token:
            dest = raw_dir / f"{run_name}.json"
            try:
                size = download_dataset(
                    settings.source_url,
                    settings.source_token,
                    dest,
                    timeout=settings.http_timeout,
                    retries=settings.http_retries,
                    backoff=settings.http_backoff,
                    max_mb=settings.max_download_mb,
                )
                return ExtractResult(dest, "api", settings.source_url, size)
            except ExtractError as exc:
                if mode == "api":
                    raise
                logger.warning("API download failed: %s", exc)
        elif mode == "api":
            raise ExtractError("SOURCE_URL and SOURCE_TOKEN must be set for SOURCE_MODE=api")
        logger.warning("Falling back to the local file %s", local_file)

    result = copy_local_file(local_file, raw_dir, run_name)
    result.used_fallback = mode == "auto"
    return result


def copy_local_file(path, raw_dir, run_name):
    if not path.is_file():
        raise ExtractError(f"Local data file not found: {path}")
    if path.suffix.lower() not in (".json", ".csv"):
        raise ExtractError(f"Unsupported file type: {path.suffix} (expected .json or .csv)")
    dest = raw_dir / f"{run_name}{path.suffix.lower()}"
    shutil.copyfile(path, dest)
    size = dest.stat().st_size
    logger.info("Using local file %s (%d bytes)", path, size)
    return ExtractResult(dest, "file", str(path), size)
