"""Structured run logging (JSON lines) and reconciliation.

One .jsonl file per run. Every event is one JSON object with a timestamp,
run id and event name — machine-parseable rather than free text
(partition being processed, body being scraped, found vs scraped counts,
failed downloads with URLs and reasons, end-of-run summary).

Reconciliation is the completeness check: for every (partition, body) unit the
site states an exact expected count ("Shows 1 to 10 of N results"); the summary
compares it against records actually stored, so the run ends as
"expected N, scraped N, or N-X with every X named in a download_failed event".
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


class EventLog:
    def __init__(self, path: Path, run_id: str):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("a", encoding="utf-8")
        self.run_id = run_id
        self.path = path

    def emit(self, event: str, **fields) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "run_id": self.run_id,
            "event": event,
            **fields,
        }
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


@dataclass(frozen=True)
class UnitSummary:
    partition: str
    body: str
    expected: int | None
    scraped: int
    failed: int
    status: str  # "complete" | "incomplete" | "unverifiable"


def build_summary(
    expected: dict[tuple[str, str], int | None],
    scraped: dict[tuple[str, str], int],
    failed: dict[tuple[str, str], int],
) -> list[UnitSummary]:
    """Pure reconciliation: one row per (partition, body) crawl unit.

    complete      scraped == expected (enumeration fully accounted; document
                  failures are counted separately in `failed` and each one is
                  individually logged — that satisfies the 200-X rule)
    incomplete    scraped < expected: records vanished without a named reason
    anomalous     scraped > expected: more records than the site declared
                  (mid-crawl publication drift or double enumeration) — never
                  silently "complete"
    unverifiable  the count string was missing — no ground truth

    NOTE: `failed` counts failed DOCUMENT downloads, whose records were already
    enumerated and therefore already counted in `scraped` (failed ⊆ scraped).
    It must never be ADDED to scraped when judging completeness — an earlier
    `scraped + failed >= expected` branch let unenumerated losses hide behind
    unrelated document failures.
    """
    rows = []
    for key in sorted(expected):
        partition, body = key
        exp = expected[key]
        got = scraped.get(key, 0)
        bad = failed.get(key, 0)
        if exp is None:
            status = "unverifiable"
        elif got == exp:
            status = "complete"
        elif got > exp:
            status = "anomalous"
        else:
            status = "incomplete"
        rows.append(UnitSummary(partition, body, exp, got, bad, status))
    return rows
