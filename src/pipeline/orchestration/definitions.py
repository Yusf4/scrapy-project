"""Dagster definitions: the pipeline as two monthly-partitioned assets.

    landing_docs   -> one month of scraped records + raw files (runs the spider)
    curated_docs   -> the transformed derivative of the same month

The dependency is partition-to-partition: curated_docs for 2024-01 waits on
landing_docs for 2024-01 specifically. The spider runs as a subprocess — the
orchestrator is the control plane and never imports the crawling runtime
(Twisted's reactor is also not restartable in-process, so a run worker that
crawled in-process could not retry).

Run concurrency is capped at 1 in dagster_home/dagster.yaml: Scrapy's
politeness limits are per process, and all partitions share one server budget.
"""

import json
import os
import subprocess
import sys
from calendar import monthrange
from datetime import date
from pathlib import Path

import dagster as dg

from pipeline.config import get_settings
from pipeline.transform import transform_range

_settings = get_settings()

monthly = dg.MonthlyPartitionsDefinition(start_date=_settings.partition_start_date)


def month_bounds(partition_key: str) -> tuple[str, str]:
    """Dagster's monthly partition key is the first day of the month
    (e.g. '2024-01-01'); the crawl window is that whole month, inclusive."""
    first = date.fromisoformat(partition_key)
    last = date(first.year, first.month, monthrange(first.year, first.month)[1])
    return first.isoformat(), last.isoformat()


def crawl_command(start: str, end: str, run_id: str) -> list[str]:
    """The exact subprocess invocation — pure, so the handshake is testable."""
    return [sys.executable, "-m", "scrapy", "crawl", "decisions",
            "-a", f"start={start}", "-a", f"end={end}",
            "-a", f"run_id={run_id}", "-L", get_settings().log_level]


def read_run_summary(path: Path) -> dict:
    """Read THIS run's summary — never 'the newest file': a concurrent manual
    crawl (or two runs in the same second sharing an appended file) would make
    'newest' a foreign or interleaved log. The id is minted by the asset and
    passed down, so the file is unambiguous; a missing summary is a loud error,
    not an empty dict."""
    if not path.exists():
        raise RuntimeError(f"crawl exited 0 but wrote no event log at {path}")
    for line in reversed(path.read_text().splitlines()):
        event = json.loads(line)
        if event.get("event") == "run_summary":
            return event
    raise RuntimeError(f"no run_summary event in {path}")


@dg.asset(partitions_def=monthly, group_name="ingestion")
def landing_docs(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """One month of decision records: metadata in MongoDB, raw documents in
    the landing bucket, exactly as received."""
    if not os.getenv("DAGSTER_HOME"):
        # Without DAGSTER_HOME the instance is ephemeral and its run queue
        # defaults to 10 concurrent runs — 10 crawls x 8 connections against a
        # server measured to saturate at 8. The politeness cap lives in
        # dagster_home/dagster.yaml; use `make dev` which exports it.
        context.log.warning(
            "DAGSTER_HOME is not set: the max_concurrent_runs=1 politeness cap "
            "from dagster_home/dagster.yaml is NOT active. Do not backfill.")
    start, end = month_bounds(context.partition_key)
    run_id = f"run_dg_{context.run_id[:18]}"
    cmd = crawl_command(start, end, run_id)
    context.log.info("launching crawl %s..%s run_id=%s", start, end, run_id)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"crawl failed:\n{result.stderr[-2000:]}")

    summary = read_run_summary(Path(_settings.log_dir) / f"{run_id}.jsonl")
    totals = summary.get("totals", {})
    incomplete = [u for u in summary.get("units", [])
                  if u["status"] not in ("complete",)]
    if incomplete:
        raise RuntimeError(f"reconciliation incomplete: {incomplete}")
    mech = summary.get("mechanics", {})
    return dg.MaterializeResult(metadata={
        "expected": totals.get("expected", 0),
        "scraped": totals.get("scraped", 0),
        "failed": totals.get("failed", 0),
        "retries": mech.get("retries", 0),
        "retry_gave_up": mech.get("retry_gave_up", 0),
        "dupefilter_skipped": mech.get("dupefilter_skipped", 0),
        "files_skipped_unchanged": mech.get("files_skipped_unchanged", 0),
        "units": dg.MetadataValue.json(summary.get("units", [])),
        "run_id": summary.get("run_id", ""),
    })


@dg.asset(partitions_def=monthly, deps=[landing_docs], group_name="transformation")
def curated_docs(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """The curated derivative of the same month: content-only files renamed to
    identifier.ext in the curated bucket, metadata in the curated collection."""
    start, end = month_bounds(context.partition_key)
    counts = transform_range(start, end)
    return dg.MaterializeResult(metadata={
        **{k: v for k, v in counts.items()},
    })


defs = dg.Definitions(assets=[landing_docs, curated_docs])
