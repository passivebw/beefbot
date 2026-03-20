"""time_utils.py"""
from __future__ import annotations
import time
from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def unix_now() -> float:
    return time.time()

def format_expiry_countdown(expiry_ts: float) -> str:
    remaining = expiry_ts - time.time()
    if remaining <= 0:
        return "EXPIRED"
    m, s = divmod(int(remaining), 60)
    return f"{m}m{s:02d}s"

def is_stale(last_update: float, threshold_s: float) -> bool:
    return (time.time() - last_update) >= threshold_s
