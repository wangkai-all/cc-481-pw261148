"""Small persistence helpers: atomic JSON writes and timestamps."""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any

from .config import PROD_MARKER, RECOVERY_MARKER


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    # microsecond precision so point-in-time selection can distinguish
    # backups created within the same second
    return utcnow().isoformat(timespec="microseconds")


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def read_json(path: str, default: Any = None) -> Any:
    if not os.path.exists(path):
        if default is not None:
            return default
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def atomic_write_json(path: str, payload: Any) -> None:
    """Write JSON via tmp file + os.replace so a crash never leaves half JSON."""
    ensure_dir(os.path.dirname(path) or ".")
    fd, tmp = tempfile.mkstemp(
        prefix=".tmp-", suffix=".json", dir=os.path.dirname(path) or "."
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=False)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def is_production_tree(root: str) -> bool:
    return os.path.isfile(os.path.join(root, PROD_MARKER))


def is_recovery_tree(root: str) -> bool:
    return os.path.isfile(os.path.join(root, RECOVERY_MARKER))


def short_id(prefix: str) -> str:
    import secrets
    import time

    return f"{prefix}{time.strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(2)}"
