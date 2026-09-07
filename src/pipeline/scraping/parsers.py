"""Pure HTML parsers — no Scrapy objects, no network, no side effects.

Keeping extraction as plain functions over HTML strings makes it testable
against stored fixtures (tests/fixtures/) and independent of the crawling
runtime; the spider is a thin I/O shell around these.
"""

from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

RESULTS_PER_PAGE = 10

# Site-chrome documents present on every page; never record documents.
_CHROME_DOC_HINTS = ("cookie_policy", "decisions_information_guide")


@dataclass(frozen=True)
class ListingCard:
    """One result card on a search listing page, as displayed."""

    url: str
    identifier: str
    ref_no: str | None
    description: str | None
    published_date: str  # ISO


@dataclass(frozen=True)
class ListingPage:
    total_results: int | None  # from "Shows X to Y of N results"; None if absent
    cards: list[ListingCard]


def _parse_display_date(raw: str) -> str:
    """Site shows dd/mm/yyyy; store ISO so dates sort lexicographically."""
    return datetime.strptime(raw.strip(), "%d/%m/%Y").date().isoformat()


def parse_listing(html: str, base_url: str) -> ListingPage:
    """Extract the result count and all result cards from a search page.

    The count string is the reconciliation anchor: per partition we compare
    the number of records actually stored against this expected total.
    A page past the end returns HTTP 200 with zero cards, so callers must
    use the count (not status codes) to detect the end of results.
    """
    soup = BeautifulSoup(html, "html.parser")

    total: int | None = None
    shows = soup.find(string=lambda s: s and "results" in s and "Shows" in s)
    if shows:
        # "Shows 1 to 10 of 65731 results" -> 65731
        words = shows.split()
        try:
            total = int(words[words.index("of") + 1].replace(",", ""))
        except (ValueError, IndexError):
            total = None
    if total is None and soup.select_one("div.item-list") is not None:
        # Zero-result pages render no count string at all, just an empty
        # item-list container: a legitimately empty window, not an anomaly.
        # Only a page with NO item-list at all stays None (unverifiable).
        if not soup.select("div.item-list li.each-item"):
            total = 0

    cards: list[ListingCard] = []
    for li in soup.select("div.item-list li.each-item"):
        title_link = li.select_one("h2.title a")
        if title_link is None:
            continue
        date_el = li.select_one("span.date")
        ref_el = li.select_one("span.refNO")
        desc_el = li.select_one("p.description")
        cards.append(
            ListingCard(
                url=urljoin(base_url, title_link["href"]),
                identifier=title_link.get_text(strip=True),
                ref_no=ref_el.get_text(strip=True) if ref_el else None,
                description=desc_el.get_text(strip=True) if desc_el else None,
                published_date=_parse_display_date(date_el.get_text()) if date_el else "",
            )
        )
    return ListingPage(total_results=total, cards=cards)


def find_document_links(html: str, base_url: str) -> list[str]:
    """Return PDF/DOC links belonging to the decision content itself.

    Modern decisions are inline HTML with no document links; legacy EAT pages
    are stubs whose only content is a link to the actual PDF. Site chrome
    (cookie policy, help guides) also links PDFs, so those are excluded.
    """
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    # Positive scoping: real decision documents live in the case column (the
    # content container and its adjacent related-files block). Scanning the
    # whole page made the classifier hostage to a denylist — one new chrome
    # PDF (footer report, updated policy) would silently demote every record's
    # decision to "stub" site-wide. The denylist below remains as a backstop.
    containers = soup.select("div.content, div.related-items, div.related-file")
    anchors = [a for c in containers for a in c.select("a[href]")]
    for a in anchors:
        href = a["href"]
        if not href.lower().endswith((".pdf", ".doc", ".docx")):
            continue
        if any(hint in href.lower() for hint in _CHROME_DOC_HINTS):
            continue
        absolute = urljoin(base_url, href)
        if absolute not in links:
            links.append(absolute)
    return links
