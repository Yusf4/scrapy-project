"""Spider for the Decisions and Determinations search.

Access pattern: the site's search form is an ASP.NET POST, but it redirects to
a stateless GET whose query string carries every filter — so the spider skips
the form entirely and generates GET URLs directly:

    /en/search/?decisions=1&from=DD/MM/YYYY&to=DD/MM/YYYY&body=<id>&pageNumber=N

The from/to filter is inclusive on both ends. Results are paginated 10/page;
out-of-range pages return HTTP 200 with zero cards, so pagination is driven by
the expected-count string, never by status codes.

The spider takes a start/end date range, slices it into partitions (monthly by
default) and crawls each (partition x body) unit separately, tagging every
record with its partition_date.
"""

import math
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import scrapy

from pipeline.events import EventLog, build_summary

from pipeline.config import get_settings
from pipeline.partitions import Partition, make_partitions
from pipeline.scraping.items import DecisionItem, FileItem
from pipeline.scraping.parsers import RESULTS_PER_PAGE, find_document_links, parse_listing


class DecisionsSpider(scrapy.Spider):
    name = "decisions"

    def __init__(self, start: str, end: str, bodies: str | None = None,
                 run_id: str | None = None, **kwargs):
        """
        start / end : ISO dates (e.g. 2024-01-01), both inclusive.
        bodies      : optional comma-separated tribunal names; default = all four.
        run_id      : optional explicit id (the orchestrator passes its own so it
                      can read back exactly this run's event log — never "newest").
        """
        super().__init__(**kwargs)
        self.settings_ = get_settings()
        self.partitions = make_partitions(
            date.fromisoformat(start), date.fromisoformat(end), self.settings_.partition_size
        )
        all_bodies = self.settings_.body_ids
        if bodies:
            unknown = [b for b in bodies.split(",") if b.strip() not in all_bodies]
            if unknown:
                raise ValueError(f"unknown bodies {unknown}; known: {list(all_bodies)}")
            self.bodies = {b.strip(): all_bodies[b.strip()] for b in bodies.split(",")}
        else:
            self.bodies = dict(all_bodies)
        # reconciliation state, keyed by (partition_key, body)
        self.expected: dict[tuple[str, str], int | None] = {}
        self.scraped_count: dict[tuple[str, str], int] = defaultdict(int)
        self.failed_count: dict[tuple[str, str], int] = defaultdict(int)
        self.listing_failed: set[tuple[str, str]] = set()

        self.run_id = run_id or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
        self.events = EventLog(
            Path(self.settings_.log_dir) / f"{self.run_id}.jsonl", self.run_id
        )
        self.events.emit(
            "run_start",
            start=start, end=end,
            partitions=[p.key for p in self.partitions],
            bodies=list(self.bodies),
        )

    def _flag_latency(self, response) -> None:
        latency = response.meta.get("download_latency")
        if latency and latency > self.settings_.latency_warn_threshold_s:
            self.events.emit("slow_response", url=response.url,
                             latency_s=round(latency, 2))

    # -- URL construction -------------------------------------------------

    def _listing_url(self, partition: Partition, body_id: int, page: int) -> str:
        frm = partition.start.strftime("%d/%m/%Y")
        to = partition.end.strftime("%d/%m/%Y")
        return (
            f"{self.settings_.base_url}/en/search/?decisions=1"
            f"&from={frm}&to={to}&body={body_id}&pageNumber={page}"
        )

    # -- crawl flow --------------------------------------------------------

    async def start(self):
        # Scrapy >= 2.13 entry point; 2.18's default start() no longer calls
        # the legacy start_requests(), so the async form is required.
        for partition in self.partitions:
            for body_name, body_id in self.bodies.items():
                # Seed every unit as "no ground truth yet": a unit whose page-1
                # request dies would otherwise never enter `expected` and would
                # silently VANISH from the reconciliation summary — the gate
                # would go green on a month it never enumerated.
                self.expected.setdefault((partition.key, body_name), None)
                yield scrapy.Request(
                    self._listing_url(partition, body_id, page=1),
                    callback=self.parse_listing_page,
                    errback=self.errback_listing,
                    cb_kwargs={"partition": partition, "body_name": body_name,
                               "body_id": body_id, "page": 1},
                )

    def parse_listing_page(self, response, partition: Partition, body_name: str,
                           body_id: int, page: int):
        self._flag_latency(response)
        unit = (partition.key, body_name)
        try:
            listing = parse_listing(response.text, self.settings_.base_url)
        except Exception as exc:
            # A parser crash (site drift, malformed date) must land in the
            # structured log, not only on stderr: this page's contents are
            # unknowable, so the whole unit fails — same rule as a lost page.
            self.listing_failed.add(unit)
            self.events.emit("parse_failed", partition=partition.key,
                             body=body_name, page=page, url=response.url,
                             reason=type(exc).__name__, detail=str(exc)[:200])
            self.logger.error("parse failed unit=%s page=%s: %s", unit, page, exc)
            return

        if page == 1:
            self.expected[(partition.key, body_name)] = listing.total_results
            total = listing.total_results or 0
            self.logger.info(
                "listing partition=%s body=%s expected=%s",
                partition.key, body_name, listing.total_results,
            )
            self.events.emit("listing_expected", partition=partition.key,
                             body=body_name, expected=listing.total_results)
            if listing.total_results is None:
                self.logger.error(
                    "count string missing partition=%s body=%s url=%s — possible "
                    "site change; reconciliation impossible for this unit",
                    partition.key, body_name, response.url,
                )
            last_page = math.ceil(total / RESULTS_PER_PAGE)
            for next_page in range(2, last_page + 1):
                yield scrapy.Request(
                    self._listing_url(partition, body_id, next_page),
                    callback=self.parse_listing_page,
                    errback=self.errback_listing,
                    cb_kwargs={"partition": partition, "body_name": body_name,
                               "body_id": body_id, "page": next_page},
                )

        expected = self.expected.get((partition.key, body_name))
        if not listing.cards and expected:
            # 200-with-no-cards on a page that should have results = soft failure
            self.logger.error(
                "soft failure: 0 cards on page %s partition=%s body=%s url=%s",
                page, partition.key, body_name, response.url,
            )
            self.events.emit("soft_failure", partition=partition.key,
                             body=body_name, page=page, url=response.url)
            # A 200-with-no-cards on an in-range page IS a failed listing page:
            # its contents are unknowable, so the unit cannot claim completeness.
            self.listing_failed.add(unit)

        for card in listing.cards:
            yield DecisionItem(
                url=card.url,
                identifier=card.identifier,
                ref_no=card.ref_no,
                description=card.description,
                published_date=card.published_date,
                body=body_name,
                partition_date=partition.key,
            )
            # The document fetch travels as its own request: when the same
            # record surfaces under a second tribunal, the dupefilter drops
            # this duplicate fetch while the metadata item above still flows.
            yield scrapy.Request(
                card.url,
                callback=self.parse_case,
                errback=self.errback_document,
                cb_kwargs={"record_url": card.url, "identifier": card.identifier,
                           "pkey": partition.key, "body": body_name},
            )

    def parse_case(self, response, record_url: str, identifier: str,
                   pkey: str, body: str):
        """Store the case page; legacy EAT stubs additionally link the real PDF,
        which becomes the record's primary document."""
        self._flag_latency(response)
        doc_links = find_document_links(response.text, self.settings_.base_url)
        if doc_links:
            yield FileItem(url=record_url, identifier=identifier,
                           source_url=response.url, kind="stub",
                           doc_type="html", content=response.body)
            yield scrapy.Request(
                doc_links[0],
                callback=self.parse_document,
                errback=self.errback_document,
                # Two records may legitimately share one PDF (the measured
                # duplicate EAT pairs): a dupefilter drop here would silently
                # leave record #2 without its primary document — no errback
                # fires on a drop. One bounded re-fetch per stub buys certainty.
                dont_filter=True,
                cb_kwargs={"record_url": record_url, "identifier": identifier,
                           "pkey": pkey, "body": body},
            )
            if len(doc_links) > 1:
                self.logger.warning("multiple document links url=%s links=%s "
                                    "— storing the first, others logged",
                                    record_url, doc_links)
        else:
            yield FileItem(url=record_url, identifier=identifier,
                           source_url=response.url, kind="primary",
                           doc_type="html", content=response.body)

    def parse_document(self, response, record_url: str, identifier: str,
                       pkey: str, body: str):
        ext = response.url.rsplit(".", 1)[-1].lower()
        yield FileItem(url=record_url, identifier=identifier,
                       source_url=response.url, kind="primary",
                       doc_type=ext, content=response.body)


    # -- failure handling and reconciliation --------------------------------

    @staticmethod
    def _failure_details(failure) -> tuple[str, int | None]:
        response = getattr(failure.value, "response", None)
        return failure.value.__class__.__name__, getattr(response, "status", None)

    def errback_listing(self, failure):
        """A listing page lost after all retries: the unit's completeness is
        unknowable (we cannot know what was on that page), so the whole
        (partition, body) unit is marked failed — per-record logging cannot
        substitute for a page we never enumerated."""
        kw = failure.request.cb_kwargs
        key = (kw["partition"].key, kw["body_name"])
        self.listing_failed.add(key)
        reason, status = self._failure_details(failure)
        self.events.emit("listing_failed", partition=key[0], body=key[1],
                         page=kw["page"], url=failure.request.url,
                         reason=reason, status=status)
        self.logger.error("listing failed unit=%s page=%s reason=%s",
                          key, kw["page"], reason)

    def errback_document(self, failure):
        """A document lost after all retries: log the record individually and
        continue — one bad record must not cost the partition."""
        kw = failure.request.cb_kwargs
        key = (kw["pkey"], kw["body"])
        self.failed_count[key] += 1
        reason, status = self._failure_details(failure)
        self.events.emit("download_failed", partition=kw["pkey"], body=kw["body"],
                         url=failure.request.url, record_url=kw["record_url"],
                         reason=reason, status=status)
        self.logger.error("download failed url=%s reason=%s status=%s",
                          failure.request.url, reason, status)

    def closed(self, reason):
        rows = build_summary(self.expected, self.scraped_count, self.failed_count)
        summary = []
        for r in rows:
            status = "incomplete_listing" if (r.partition, r.body) in self.listing_failed else r.status
            summary.append({"partition": r.partition, "body": r.body,
                            "expected": r.expected, "scraped": r.scraped,
                            "failed": r.failed, "status": status})
        verifiable = [r for r in rows if r.expected is not None]
        totals = {
            "expected": sum(r.expected for r in verifiable),
            "scraped": sum(r.scraped for r in rows),
            "failed": sum(r.failed for r in rows),
            "unverifiable_units": len(rows) - len(verifiable),
        }
        # Observability of the machinery itself: everything skipped or retried
        # is a number here, not an invisible event. (crawler is absent when a
        # test constructs the spider directly.)
        stats_ = getattr(getattr(self, "crawler", None), "stats", None)

        def stat(key: str) -> int:
            return stats_.get_value(key, 0) if stats_ else 0

        mechanics = {
            "retries": stat("retry/count"),
            "retry_gave_up": stat("retry/max_reached"),
            "dupefilter_skipped": stat("dupefilter/filtered"),
            "metadata_inserted": stat("metadata/inserted"),
            "metadata_updated": stat("metadata/updated"),
            "files_stored": stat("files/stored"),
            "files_skipped_unchanged": stat("files/skipped_unchanged"),
            "files_changed": stat("files/changed"),
            "files_orphaned": stat("files/orphaned"),
            "fields_absent_ref_no": stat("fields_absent/ref_no"),
            "fields_absent_description": stat("fields_absent/description"),
        }
        self.events.emit("run_summary", reason=reason, units=summary,
                         totals=totals, mechanics=mechanics)
        self.logger.info("mechanics %s", mechanics)
        for u in summary:
            self.logger.info("reconciliation %s", u)
        self.events.close()
