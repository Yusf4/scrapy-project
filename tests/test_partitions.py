from datetime import date, timedelta

import pytest

from pipeline.partitions import Partition, make_partitions


def test_full_months():
    parts = make_partitions(date(2024, 1, 1), date(2024, 3, 31))
    assert parts == [
        Partition("2024-01", date(2024, 1, 1), date(2024, 1, 31)),
        Partition("2024-02", date(2024, 2, 1), date(2024, 2, 29)),  # leap year
        Partition("2024-03", date(2024, 3, 1), date(2024, 3, 31)),
    ]


def test_edges_clamped_to_requested_range():
    parts = make_partitions(date(2024, 1, 15), date(2024, 2, 10))
    assert parts[0] == Partition("2024-01", date(2024, 1, 15), date(2024, 1, 31))
    assert parts[-1] == Partition("2024-02", date(2024, 2, 1), date(2024, 2, 10))


def test_no_gaps_no_overlaps_across_year_boundary():
    parts = make_partitions(date(2023, 11, 3), date(2024, 2, 20))
    assert [p.key for p in parts] == ["2023-11", "2023-12", "2024-01", "2024-02"]
    for prev, nxt in zip(parts, parts[1:]):
        assert nxt.start == prev.end + timedelta(days=1)


def test_union_covers_exactly_the_requested_range():
    start, end = date(2024, 1, 15), date(2024, 3, 10)
    parts = make_partitions(start, end)
    assert parts[0].start == start
    assert parts[-1].end == end
    covered = sum((p.end - p.start).days + 1 for p in parts)
    assert covered == (end - start).days + 1


def test_single_day_range():
    parts = make_partitions(date(2024, 2, 29), date(2024, 2, 29))
    assert parts == [Partition("2024-02", date(2024, 2, 29), date(2024, 2, 29))]


def test_non_leap_february():
    parts = make_partitions(date(2023, 2, 1), date(2023, 2, 28))
    assert parts == [Partition("2023-02", date(2023, 2, 1), date(2023, 2, 28))]


def test_start_after_end_raises():
    with pytest.raises(ValueError):
        make_partitions(date(2024, 2, 1), date(2024, 1, 1))


def test_unknown_size_raises():
    with pytest.raises(ValueError):
        make_partitions(date(2024, 1, 1), date(2024, 1, 31), size="weekly")


def test_dagster_month_bounds():
    from pipeline.orchestration.definitions import month_bounds
    assert month_bounds("2024-02-01") == ("2024-02-01", "2024-02-29")
    assert month_bounds("2023-12-01") == ("2023-12-01", "2023-12-31")


def test_dagster_definitions_load():
    from pipeline.orchestration.definitions import defs, monthly
    keys = {a.key.to_user_string() for a in defs.assets}
    assert keys == {"landing_docs", "curated_docs"}
    assert "2024-01-01" in monthly.get_partition_keys()
