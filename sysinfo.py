"""Tiny RSS probe (no psutil dependency)."""
from __future__ import annotations

import os
import resource


def rss_mb() -> float:
    """Current resident set size in MB (Linux /proc), falling back to peak RSS elsewhere."""
    try:
        with open("/proc/self/statm", "rb") as fh:
            pages = int(fh.read().split()[1])
        return round(pages * os.sysconf("SC_PAGE_SIZE") / 1_048_576, 1)
    except (OSError, ValueError, IndexError):
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports KB, macOS reports bytes
        return round(peak / 1024 if peak < 10**9 else peak / 1_048_576, 1)
