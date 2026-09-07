"""Hashing and change detection.

Two distinct hashes per stored file:

- file_hash: SHA-256 of the exact stored bytes (integrity of the landing object).
- content_fingerprint: SHA-256 of a normalised form, used to decide whether a
  re-downloaded document actually changed.

The distinction exists because the source embeds volatile server-debug comments
in its HTML (observed: a trailing "<!-- Elapsed time: ... -->" that differs on
every request), so raw bytes differ even when the decision content is
byte-for-byte identical. Normalisation strips HTML comments only — targeted at
the observed volatility, so genuine content edits still change the fingerprint.
Binary documents (PDF/DOC) have no comments; their fingerprint is the raw hash.
"""

import hashlib
import re

_HTML_COMMENT = re.compile(rb"<!--.*?-->", re.DOTALL)


def raw_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def content_fingerprint(content: bytes, doc_type: str) -> str:
    if doc_type == "html":
        content = _HTML_COMMENT.sub(b"", content)
    return hashlib.sha256(content).hexdigest()
