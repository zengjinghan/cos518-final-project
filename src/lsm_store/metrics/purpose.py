"""Stable purpose indices shared by physical counters and captured windows."""

from enum import IntEnum


class IoPurpose(IntEnum):
    """Keep foreground requests separate from maintenance and inspection."""

    JOURNAL = 0
    FLUSH = 1
    COMPACTION = 2
    POINT_READ = 3
    SCAN = 4
    RECOVERY = 5
    METADATA = 6
    INSPECTION = 7
