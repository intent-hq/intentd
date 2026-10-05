#!/usr/bin/env python3
"""Check the private sanitized export before upload; never copy fixture inputs.

The Rust startup_failure_evidence writer owns sanitization. This checks the
upload boundary (fresh directory supplied by CI, fixed names, modes and bounds).
No output is uploadable until every file passes. Missing evidence is normal
when coverage fails before a native fixture starts.
"""

import json
import os
from pathlib import Path
import stat
import sys


MAX_FILES = 16
MAX_BYTES = 128 * 1024
SLOTS = {f"startup-failure-{slot:02}.json" for slot in range(MAX_FILES)}


def validate(directory):
    root = Path(directory)
    if not root.is_absolute() or root.resolve() != root:
        raise ValueError("export directory must be absolute and canonical")
    if not root.exists():
        return False
    meta = root.lstat()
    if not stat.S_ISDIR(meta.st_mode) or stat.S_IMODE(meta.st_mode) != 0o700 or meta.st_uid != os.geteuid():
        raise ValueError("export directory must be private and owned")
    entries = list(root.iterdir())
    if len(entries) > MAX_FILES or any(path.name not in SLOTS for path in entries):
        raise ValueError("export contains unexpected files")
    for path in entries:
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
            meta = os.fstat(source.fileno())
            if (not stat.S_ISREG(meta.st_mode) or stat.S_IMODE(meta.st_mode) != 0o600
                    or meta.st_uid != os.geteuid() or meta.st_nlink != 1):
                raise ValueError("export file must be private, regular and owned")
            if meta.st_size > MAX_BYTES:
                raise ValueError("export exceeds byte limit")
            data = source.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise ValueError("export exceeds byte limit")
            report = json.loads(data)
            if (not isinstance(report, dict) or report.get("version") != 1
                    or report.get("success") is not False
                    or report.get("nativeCompletion") != "not-established"):
                raise ValueError("export is not a startup failure report")
    return bool(entries)


if __name__ == "__main__":
    try:
        ready = validate(sys.argv[1])
    except (OSError, ValueError, IndexError):
        # Do not echo rejected paths or raw JSON into CI logs.
        sys.exit("Native startup export failed upload validation")
    print(f"ready={'true' if ready else 'false'}")
