"""Date-range partitioning.

The scraper receives a start/end date and iterates the range in fixed windows
("partitions"). The source's date filter is inclusive on both ends, so windows
are [first_day, last_day] with no gaps and no overlaps: consecutive windows
differ by exactly one day at the boundary.

Window width is configurable (PARTITION_SIZE); monthly is the default and the
only implemented width — chosen for this source's density (~180-310 records/month
at 10 results per listing page). See ARCHITECTURE.md.
"""

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class Partition:
    key: str          # e.g. "2024-01" — becomes each record's partition_date
    start: date       # first day covered (inclusive)
    end: date         # last day covered (inclusive)


def month_partitions(start: date, end: date) -> list[Partition]:
    """Split [start, end] (both inclusive) into calendar-month windows.

    Edge months are clamped to the requested range, so the union of all
    windows is exactly [start, end].
    """
    if start > end:
        raise ValueError(f"start {start} is after end {end}")

    partitions: list[Partition] = []
    cursor = start
    while cursor <= end:
        month_last_day = date(cursor.year, cursor.month, monthrange(cursor.year, cursor.month)[1])
        window_end = min(month_last_day, end)
        partitions.append(
            Partition(key=f"{cursor.year:04d}-{cursor.month:02d}", start=cursor, end=window_end)
        )
        cursor = window_end + timedelta(days=1)
    return partitions


def make_partitions(start: date, end: date, size: str = "monthly") -> list[Partition]:
    """Entry point selected by the PARTITION_SIZE setting."""
    if size == "monthly":
        return month_partitions(start, end)
    raise ValueError(f"unsupported partition size {size!r}; implemented: monthly")
