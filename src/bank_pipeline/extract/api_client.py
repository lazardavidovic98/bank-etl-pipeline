"""Downloads the dataset from the HTTP API (Bearer token authentication)."""

import logging
import time

import requests

from bank_pipeline.errors import ExtractError, SourceUnavailableError

logger = logging.getLogger(__name__)

# Statuses where trying again later can help (server overloaded, gateway errors...).
RETRY_STATUSES = {429, 500, 502, 503, 504}


def download_dataset(url, token, dest_path, timeout=30, retries=3, backoff=2.0, max_mb=200, sleep=time.sleep):
    """
    Download the dataset to `dest_path` and return the number of bytes written.

    - the response is streamed to disk in 64 KB blocks (not kept in memory)
    - network errors and RETRY_STATUSES are retried with exponential backoff:
      wait backoff, 2*backoff, 4*backoff ... seconds between attempts
    - 401/403 (bad token) and other 4xx errors are not retried
    - the download is aborted if it gets bigger than `max_mb`
    """
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    max_bytes = max_mb * 1024 * 1024
    attempts = retries + 1

    for attempt in range(1, attempts + 1):
        logger.info("Downloading %s (attempt %d/%d)", url, attempt, attempts)
        try:
            with requests.get(url, headers=headers, timeout=timeout, stream=True) as response:
                if response.status_code in (401, 403):
                    raise ExtractError(f"Access denied by the API (HTTP {response.status_code}) - check SOURCE_TOKEN")
                if response.status_code in RETRY_STATUSES:
                    raise requests.HTTPError(f"HTTP {response.status_code}")
                if response.status_code >= 400:
                    raise ExtractError(f"API returned HTTP {response.status_code}")

                size = 0
                with open(dest_path, "wb") as f:
                    for block in response.iter_content(chunk_size=64 * 1024):
                        size += len(block)
                        if size > max_bytes:
                            raise ExtractError(f"Download is larger than MAX_DOWNLOAD_MB={max_mb}")
                        f.write(block)

            if size == 0:
                raise ExtractError("API returned an empty response")
            logger.info("Downloaded %d bytes", size)
            return size

        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            dest_path.unlink(missing_ok=True)
            if attempt == attempts:
                raise SourceUnavailableError(f"API not available after {attempts} attempts: {exc}") from exc
            wait = backoff * 2 ** (attempt - 1)
            logger.warning("Attempt %d failed (%s), retrying in %.0f s", attempt, exc, wait)
            sleep(wait)
        except ExtractError:
            dest_path.unlink(missing_ok=True)
            raise


def check_source(url, token, timeout=10):
    """One quick request (no retries) - used by `run_pipeline.py --check-source`."""
    try:
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=timeout, stream=True)
        response.close()
        return f"reachable, HTTP {response.status_code}"
    except requests.RequestException as exc:
        return f"NOT reachable ({type(exc).__name__})"
