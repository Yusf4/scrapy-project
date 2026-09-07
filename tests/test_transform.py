from pathlib import Path

from pipeline.transform import extract_content

FIXTURES = Path(__file__).parent / "fixtures"

CHROME_MARKERS = [
    "Return to Search",          # search-results navigation
    "globalCookieBar",           # cookie banner
    "google_translate_element",  # translate widget
    "footer-nav",                # footer navigation
    "language-switch",           # EN/GA switch
]


def test_extracts_decision_content_and_drops_chrome():
    html = (FIXTURES / "adj-00047352.html").read_text(encoding="utf-8", errors="ignore")
    out = extract_content(html)
    assert "ADJ-00047352" in out                      # page title kept
    assert "Industrial Relations Act" in out          # decision text kept
    for marker in CHROME_MARKERS:
        assert marker not in out, f"chrome leaked: {marker}"
    assert len(out) < len(html) / 2                   # substantial reduction


def test_extracts_full_case_report_body():
    html = (FIXTURES / "dec-e2020-007.html").read_text(encoding="utf-8", errors="ignore")
    out = extract_content(html)
    assert "DEC-E2020-007" in out
    for marker in CHROME_MARKERS:
        assert marker not in out


def test_no_scripts_survive():
    html = (FIXTURES / "lcr23320.html").read_text(encoding="utf-8", errors="ignore")
    out = extract_content(html)
    assert "<script" not in out.lower()


def test_degrades_gracefully_on_unknown_page_shape():
    out = extract_content("<html><body><p>unrecognised layout</p></body></html>")
    assert "<body>" in out  # empty but valid shell, no crash


import pytest


@pytest.mark.integration
def test_one_broken_record_does_not_kill_the_run():
    """A record whose landing object is unreadable is logged and counted;
    the remaining records still transform (tip: robust error handling)."""
    from pipeline.config import get_settings
    from pipeline.storage import landing_collection, mongo_client
    from pipeline.transform import transform_range

    s = get_settings()
    client = mongo_client(s)
    coll = landing_collection(client, s)
    good_url = "https://example.invalid/tf-good"
    bad_url = "https://example.invalid/tf-bad"
    coll.delete_many({"url": {"$in": [good_url, bad_url]}})

    import boto3
    from pipeline.storage import ensure_bucket, s3_client
    s3 = s3_client(s)
    ensure_bucket(s3, s.landing_bucket)
    s3.put_object(Bucket=s.landing_bucket, Key="TF-GOOD/deadbeef.html",
                  Body=b"<html><body><h1 class='page-title'>TF-GOOD</h1>"
                       b"<div class='content'><p>" + b"decision text " * 20 + b"</p></div></body></html>")
    base = {"published_date": "1999-01-15", "partition_date": "1999-01",
            "doc_type": "html", "bodies": ["Labour Court"]}
    coll.insert_many([
        {**base, "url": good_url, "identifier": "TF-GOOD", "ref_no": "TF-GOOD",
         "file_path": "TF-GOOD/deadbeef.html"},
        {**base, "url": bad_url, "identifier": "TF-BAD", "ref_no": "TF-BAD",
         "file_path": "TF-BAD/does-not-exist.html"},
    ])

    counts = transform_range("1999-01-01", "1999-01-31", s)
    assert counts["records"] == 2
    assert counts["failed"] == 1          # the broken one, logged not fatal
    assert counts["transformed"] == 1     # the good one still made it

    # cleanup
    coll.delete_many({"url": {"$in": [good_url, bad_url]}})
    client[s.mongo_db][s.curated_collection].delete_many({"identifier": {"$in": ["TF-GOOD", "TF-BAD"]}})
    s3.delete_object(Bucket=s.landing_bucket, Key="TF-GOOD/deadbeef.html")
    s3.delete_object(Bucket=s.curated_bucket, Key="TF-GOOD.html")
    client.close()


@pytest.mark.integration
def test_collision_never_clobbers_the_first_records_object():
    """Two records forced onto one curated filename: the first claimant's
    object must survive byte-for-byte; the second is a logged collision that
    writes NOTHING (claim-then-write order)."""
    import hashlib
    from pipeline.config import get_settings
    from pipeline.storage import (ensure_bucket, landing_collection,
                                  mongo_client, s3_client)
    from pipeline.transform import transform_range

    s = get_settings()
    client = mongo_client(s)
    landing = landing_collection(client, s)
    curated = client[s.mongo_db][s.curated_collection]
    s3 = s3_client(s)
    ensure_bucket(s3, s.landing_bucket)
    ensure_bucket(s3, s.curated_bucket)

    url_a, url_b = "https://example.invalid/col-a", "https://example.invalid/col-b"
    landing.delete_many({"url": {"$in": [url_a, url_b]}})
    curated.delete_many({"url": {"$in": [url_a, url_b]}})

    body_a = b"%PDF-CONTENT-OF-A"
    body_b = b"%PDF-CONTENT-OF-B"
    s3.put_object(Bucket=s.landing_bucket, Key="COL-1/aaaa.pdf", Body=body_a)
    s3.put_object(Bucket=s.landing_bucket, Key="COL-1/bbbb.pdf", Body=body_b)
    base = {"published_date": "1998-02-10", "partition_date": "1998-02",
            "doc_type": "pdf", "bodies": ["Labour Court"],
            "identifier": "COL-1", "ref_no": "COL-1"}   # same name for both
    landing.insert_many([
        {**base, "url": url_a, "file_path": "COL-1/aaaa.pdf"},
        {**base, "url": url_b, "file_path": "COL-1/bbbb.pdf"},
    ])

    counts = transform_range("1998-02-01", "1998-02-28", s)
    assert counts["name_collisions"] == 1

    stored = s3.get_object(Bucket=s.curated_bucket, Key="COL-1.pdf")["Body"].read()
    assert stored == body_a                      # first claimant's bytes survive
    owner = curated.find_one({"file_path": "COL-1.pdf"})
    assert owner["url"] == url_a
    assert owner["file_hash"] == hashlib.sha256(body_a).hexdigest()

    landing.delete_many({"url": {"$in": [url_a, url_b]}})
    curated.delete_many({"url": {"$in": [url_a, url_b]}})
    for k in ("COL-1/aaaa.pdf", "COL-1/bbbb.pdf"):
        s3.delete_object(Bucket=s.landing_bucket, Key=k)
    s3.delete_object(Bucket=s.curated_bucket, Key="COL-1.pdf")
    client.close()
