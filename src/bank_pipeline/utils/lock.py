"""Prevents two pipeline runs from executing at the same time.

The lock is an OS file lock, so it is released automatically when the
process exits, even after a crash. No stale lock files are left behind.
"""

import sys

from bank_pipeline.errors import AlreadyRunningError


class PipelineLock:
    def __init__(self, path):
        self.path = path
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(self.path, "a+")
        try:
            if sys.platform == "win32":
                import msvcrt

                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise AlreadyRunningError(f"Another run is in progress (lock file: {self.path})") from None
        return self

    def __exit__(self, *exc):
        if sys.platform == "win32":
            import msvcrt

            self.file.seek(0)
            msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
        self.file.close()  # on Linux closing the file releases the flock
