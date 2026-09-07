from pathlib import Path

from pipeline.scraping.parsers import parse_listing

FIXTURES = Path(__file__).parent / "fixtures"
BASE_URL = "https://www.workplacerelations.ie"


def _listing():
    html = (FIXTURES / "search.html").read_text(encoding="utf-8", errors="ignore")
    return parse_listing(html, BASE_URL)


def test_total_results_parsed():
    assert _listing().total_results == 65731


def test_ten_cards_per_page():
    assert len(_listing().cards) == 10


def test_card_fields_complete():
    card = _listing().cards[0]
    assert card.identifier == "LCR23320"
    assert card.ref_no == "LCR23320"
    assert card.url == f"{BASE_URL}/en/cases/2026/august/lcr23320.html"
    assert card.published_date == "2026-08-20"
    assert "SK BIOTEK" in card.description


def test_urls_absolute_and_unique():
    cards = _listing().cards
    assert all(c.url.startswith("https://") for c in cards)
    assert len({c.url for c in cards}) == len(cards)


def test_dates_are_iso():
    for c in _listing().cards:
        year, month, day = c.published_date.split("-")
        assert len(year) == 4 and len(month) == 2 and len(day) == 2


def test_empty_page_yields_no_cards_but_no_crash():
    page = parse_listing("<html><body><p>nothing here</p></body></html>", BASE_URL)
    assert page.cards == []
    assert page.total_results is None


def test_eat_stub_links_its_pdf():
    from pipeline.scraping.parsers import find_document_links
    html = (FIXTURES / "ud1020_2007_wt341_2007.html").read_text(encoding="utf-8", errors="ignore")
    links = find_document_links(html, BASE_URL)
    assert len(links) == 1
    assert links[0].startswith(f"{BASE_URL}/en/eat_import/") and links[0].endswith(".pdf")


def test_modern_case_page_has_no_document_links():
    from pipeline.scraping.parsers import find_document_links
    html = (FIXTURES / "adj-00047352.html").read_text(encoding="utf-8", errors="ignore")
    assert find_document_links(html, BASE_URL) == []


def test_zero_result_page_counts_as_zero():
    # zero-result pages render an empty item-list and no "Shows ... results"
    html = '<html><body><div class="item-list"></div></body></html>'
    page = parse_listing(html, BASE_URL)
    assert page.total_results == 0 and page.cards == []


def test_page_without_item_list_is_unverifiable():
    page = parse_listing("<html><body><p>maintenance page</p></body></html>", BASE_URL)
    assert page.total_results is None


def test_chrome_pdf_outside_content_is_ignored():
    """The classifier is positively scoped to the
    case column — a NEW chrome PDF (footer report etc.) must not demote every
    record's decision to a stub."""
    from pipeline.scraping.parsers import find_document_links
    html = """<html><body>
      <header><a href="/en/some/new_annual_report.pdf">report</a></header>
      <div class="content"><p>full decision text here</p></div>
      <footer><a href="/en/another/random_brochure.pdf">brochure</a></footer>
    </body></html>"""
    assert find_document_links(html, BASE_URL) == []
