"""Small helper functions."""

import hashlib
import shutil
from itertools import islice


def batched(iterable, size):
    """Yield lists of `size` items without loading the whole iterable into memory."""
    iterator = iter(iterable)
    while batch := list(islice(iterator, size)):
        yield batch


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def remove_old_runs(directory, keep):
    """Keep only the `keep` newest entries in `directory` (raw files / run folders)."""
    if not directory.exists():
        return
    entries = [p for p in directory.iterdir() if not p.name.startswith(".")]
    entries.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    for path in entries[keep:]:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)
