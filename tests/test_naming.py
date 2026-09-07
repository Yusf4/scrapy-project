from pipeline.naming import curated_filename, landing_key, sanitize


def test_sanitize_strips_and_replaces_unsafe_chars():
    assert sanitize("UD1020/2007, WT341/2007 ") == "UD1020-2007-WT341-2007"


def test_sanitize_keeps_clean_identifiers():
    assert sanitize("ADJ-00047352") == "ADJ-00047352"


def test_curated_plain_when_refno_equals_identifier():
    assert curated_filename("ADJ-00047352", "ADJ-00047352", "html") == "ADJ-00047352.html"


def test_curated_tiebreaker_when_refno_differs():
    # legacy EAT collision pair: same identifier, distinct numeric ref_nos
    a = curated_filename("RP2378/2011, RP2422/2011", "36252", "pdf")
    b = curated_filename("RP2378/2011, RP2422/2011", "31482", "pdf")
    assert a == "RP2378-2011-RP2422-2011__36252.pdf"
    assert a != b


def test_curated_handles_missing_refno():
    assert curated_filename("LCR23320", None, "html") == "LCR23320.html"


def test_landing_key_is_hash_versioned():
    key = landing_key("ADJ-00047352", "abc123", "html")
    assert key == "ADJ-00047352/abc123.html"


def test_fingerprint_stable_across_volatile_fetches():
    """Two real fetches of the same page, seconds apart: raw bytes differ
    (server-debug comment), fingerprints must not."""
    from pathlib import Path
    from pipeline.hashing import content_fingerprint, raw_sha256
    fx = Path(__file__).parent / "fixtures"
    a = (fx / "volatile_fetch_1.html").read_bytes()
    b = (fx / "volatile_fetch_2.html").read_bytes()
    assert raw_sha256(a) != raw_sha256(b)
    assert content_fingerprint(a, "html") == content_fingerprint(b, "html")


def test_fingerprint_detects_real_change():
    from pipeline.hashing import content_fingerprint
    a = b"<html><body>decision text</body></html><!-- Elapsed time: 1 -->"
    b_ = b"<html><body>decision text AMENDED</body></html><!-- Elapsed time: 2 -->"
    assert content_fingerprint(a, "html") != content_fingerprint(b_, "html")
