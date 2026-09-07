import json

from pipeline.events import EventLog, build_summary


def test_event_log_writes_json_lines(tmp_path):
    log = EventLog(tmp_path / "run.jsonl", "run_test")
    log.emit("run_start", partitions=["2024-01"], bodies=["WRC"])
    log.emit("download_failed", url="https://x/y", reason="DownloadTimeoutError", status=None)
    log.close()

    lines = [json.loads(l) for l in (tmp_path / "run.jsonl").read_text().splitlines()]
    assert [l["event"] for l in lines] == ["run_start", "download_failed"]
    assert all(l["run_id"] == "run_test" and "ts" in l for l in lines)
    assert lines[1]["reason"] == "DownloadTimeoutError"


def test_summary_complete_when_all_scraped():
    rows = build_summary({("2024-01", "WRC"): 234}, {("2024-01", "WRC"): 234}, {})
    assert rows[0].status == "complete" and rows[0].failed == 0


def test_summary_complete_with_doc_failures_visible():
    # doc failures are a subset of scraped (their records WERE enumerated);
    # they never reduce enumeration completeness, they ride along as `failed`
    rows = build_summary({("2024-01", "WRC"): 234}, {("2024-01", "WRC"): 234},
                         {("2024-01", "WRC"): 4})
    assert rows[0].status == "complete" and rows[0].failed == 4


def test_summary_incomplete_when_records_vanish_silently():
    rows = build_summary({("2024-01", "WRC"): 234}, {("2024-01", "WRC"): 230}, {})
    assert rows[0].status == "incomplete"


def test_doc_failures_cannot_mask_enumeration_losses():
    """Regression (review F1-critical): failed ⊆ scraped, so scraped+failed
    must never be compared against expected — 10 vanished records padded by 10
    unrelated document failures used to read 'complete'."""
    rows = build_summary({("2024-01", "WRC"): 200}, {("2024-01", "WRC"): 190},
                         {("2024-01", "WRC"): 10})
    assert rows[0].status == "incomplete"


def test_over_enumeration_is_anomalous_not_complete():
    rows = build_summary({("2024-01", "WRC"): 234}, {("2024-01", "WRC"): 236}, {})
    assert rows[0].status == "anomalous"


def test_summary_unverifiable_without_count():
    rows = build_summary({("2024-01", "WRC"): None}, {("2024-01", "WRC"): 10}, {})
    assert rows[0].status == "unverifiable"
