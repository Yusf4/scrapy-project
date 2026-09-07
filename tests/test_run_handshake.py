"""Fixes for review findings #2 and #3: explicit run-id handshake between the
orchestrator and the spider, and seeded reconciliation units so a failed
page-1 listing cannot silently vanish from the summary."""

import asyncio
import json
from pathlib import Path

import pytest
from twisted.python.failure import Failure

from pipeline.orchestration.definitions import crawl_command, read_run_summary
from pipeline.scraping.spiders.decisions import DecisionsSpider


def test_crawl_command_carries_the_run_id():
    cmd = crawl_command("2024-01-01", "2024-01-31", "run_dg_abc123")
    assert "-a" in cmd and "run_id=run_dg_abc123" in cmd


def test_read_run_summary_refuses_missing_file(tmp_path):
    with pytest.raises(RuntimeError, match="no event log"):
        read_run_summary(tmp_path / "run_nope.jsonl")


def test_read_run_summary_refuses_file_without_summary(tmp_path):
    p = tmp_path / "run_x.jsonl"
    p.write_text(json.dumps({"event": "run_start"}) + "\n")
    with pytest.raises(RuntimeError, match="no run_summary"):
        read_run_summary(p)


def test_read_run_summary_returns_the_summary_event(tmp_path):
    p = tmp_path / "run_x.jsonl"
    p.write_text(json.dumps({"event": "run_start"}) + "\n"
                 + json.dumps({"event": "run_summary", "totals": {"expected": 5}}) + "\n")
    assert read_run_summary(p)["totals"]["expected"] == 5


def _drain(agen):
    async def collect():
        return [item async for item in agen]
    return asyncio.run(collect())


def test_spider_uses_explicit_run_id_for_its_event_log():
    sp = DecisionsSpider(start="2024-01-01", end="2024-01-31",
                         bodies="Labour Court", run_id="run_pytest_hs")
    try:
        assert sp.run_id == "run_pytest_hs"
        assert sp.events.path.name == "run_pytest_hs.jsonl"
    finally:
        sp.events.close()
        sp.events.path.unlink(missing_ok=True)


def test_failed_page1_unit_appears_in_summary_as_incomplete():
    """Review finding #3: without seeding, a unit whose first listing page dies
    never enters `expected` and is absent from run_summary — the completeness
    gate would pass a month it never enumerated."""
    sp = DecisionsSpider(start="2024-01-01", end="2024-01-31",
                         bodies="Labour Court", run_id="run_pytest_seed")
    try:
        requests = _drain(sp.start())            # seeds expected for every unit
        assert sp.expected == {("2024-01", "Labour Court"): None}

        failure = Failure(Exception("boom"))
        failure.request = requests[0]            # page-1 request, cb_kwargs attached
        sp.errback_listing(failure)

        sp.closed("finished")                    # writes run_summary and closes log

        events = [json.loads(l) for l in sp.events.path.read_text().splitlines()]
        summary = [e for e in events if e["event"] == "run_summary"][0]
        assert len(summary["units"]) == 1
        unit = summary["units"][0]
        assert unit["partition"] == "2024-01"
        assert unit["body"] == "Labour Court"
        assert unit["expected"] is None
        assert unit["status"] == "incomplete_listing"   # gate will block this
    finally:
        sp.events.path.unlink(missing_ok=True)


def _fake_response(spider, html: str, url: str = "https://www.workplacerelations.ie/en/search/?x"):
    import scrapy
    from scrapy.http import TextResponse
    req = scrapy.Request(url)
    return TextResponse(url=url, request=req, body=html.encode(), encoding="utf-8")


def test_soft_failure_marks_unit_listing_failed():
    """Review F6: an in-range 200-with-no-cards page means unknowable contents —
    the unit must fail, not be left to count arithmetic."""
    sp = DecisionsSpider(start="2024-01-01", end="2024-01-31",
                         bodies="Labour Court", run_id="run_pytest_soft")
    try:
        _drain(sp.start())
        unit = ("2024-01", "Labour Court")
        sp.expected[unit] = 45  # page 1 said 45 results
        resp = _fake_response(sp, '<html><div class="item-list search-list"><ul></ul></div>'
                                  '<div>Shows 1 to 10 of 45 results</div></html>')
        list(sp.parse_listing_page(resp, partition=sp.partitions[0],
                                   body_name="Labour Court", body_id=3, page=3))
        assert unit in sp.listing_failed
    finally:
        sp.events.close()
        sp.events.path.unlink(missing_ok=True)


def test_parser_exception_marks_unit_and_does_not_crash(monkeypatch):
    """Review F5: a parser crash (site drift) must fail the unit and land in the
    structured log instead of dying on stderr only."""
    import pipeline.scraping.spiders.decisions as mod
    sp = DecisionsSpider(start="2024-01-01", end="2024-01-31",
                         bodies="Labour Court", run_id="run_pytest_parsefail")
    try:
        _drain(sp.start())
        monkeypatch.setattr(mod, "parse_listing",
                            lambda *a, **k: (_ for _ in ()).throw(ValueError("drift")))
        resp = _fake_response(sp, "<html>whatever</html>")
        out = list(sp.parse_listing_page(resp, partition=sp.partitions[0],
                                         body_name="Labour Court", body_id=3, page=1))
        assert out == []
        assert ("2024-01", "Labour Court") in sp.listing_failed
        import json
        events = [json.loads(l)["event"] for l in sp.events.path.read_text().splitlines()]
        assert "parse_failed" in events
    finally:
        sp.events.close()
        sp.events.path.unlink(missing_ok=True)


def test_stub_pdf_request_bypasses_dupefilter():
    """Review F4: two records sharing one PDF — the second fetch must not be
    silently dropped, so document requests carry dont_filter."""
    from pathlib import Path
    import scrapy
    sp = DecisionsSpider(start="2008-12-01", end="2008-12-31",
                         bodies="Employment Appeals Tribunal", run_id="run_pytest_df")
    try:
        html = (Path(__file__).parent / "fixtures" / "ud1020_2007_wt341_2007.html").read_text(
            encoding="utf-8", errors="ignore")
        resp = _fake_response(sp, html,
                              url="https://www.workplacerelations.ie/en/cases/2008/december/ud1020_2007_wt341_2007.html")
        out = list(sp.parse_case(resp, record_url=resp.url,
                                 identifier="UD1020/2007, WT341/2007",
                                 pkey="2008-12", body="Employment Appeals Tribunal"))
        pdf_requests = [o for o in out if isinstance(o, scrapy.Request)]
        assert len(pdf_requests) == 1
        assert pdf_requests[0].dont_filter is True
    finally:
        sp.events.close()
        sp.events.path.unlink(missing_ok=True)


def test_errback_document_logs_and_counts():
    """The document errback is critical (failed downloads must be
    logged with URL + reason) but had no direct test."""
    import json
    from twisted.python.failure import Failure
    import scrapy
    sp = DecisionsSpider(start="2024-01-01", end="2024-01-31",
                         bodies="Labour Court", run_id="run_pytest_errdoc")
    try:
        class _Stats:
            def __init__(self): self.d={}
            def inc_value(self,k): self.d[k]=self.d.get(k,0)+1
            def get_value(self,k,d=0): return self.d.get(k,d)
        sp.crawler = type("C",(),{"stats":_Stats()})()
        req = scrapy.Request("https://www.workplacerelations.ie/en/cases/x.html",
                             cb_kwargs={"record_url":"https://…/x.html",
                                        "identifier":"X","pkey":"2024-01",
                                        "body":"Labour Court"})
        f = Failure(Exception("boom")); f.request = req
        sp.errback_document(f)
        assert sp.failed_count[("2024-01","Labour Court")] == 1
        events = [json.loads(l) for l in sp.events.path.read_text().splitlines()]
        df = [e for e in events if e["event"]=="download_failed"]
        assert len(df)==1 and df[0]["url"].endswith("x.html") and "reason" in df[0]
    finally:
        sp.events.close(); sp.events.path.unlink(missing_ok=True)


def test_sanitize_collapses_dash_runs():
    from pipeline.naming import curated_filename
    # identifier and ref differ only in punctuation -> no ugly triple dashes,
    # and the redundant tiebreaker case stays readable
    assert "---" not in curated_filename("IR - SC - 00001515", "IR-SC-00001515", "html")
