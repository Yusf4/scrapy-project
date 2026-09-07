"""Filename and object-key construction.

Identifiers are display strings and may contain path separators, commas and
trailing whitespace (e.g. "UD1020/2007, WT341/2007"); they are also not unique
across documents in legacy EAT data. Names derived from them are therefore
sanitised, and the curated filename appends the ref_no whenever it differs
from the identifier — a deterministic, order-independent tiebreaker (the
colliding population is exactly the one where ref_no is a distinct number).

Nothing run-scoped (run ids, timestamps) ever goes into a name: names must be
stable across runs or idempotency breaks.
"""

import re

_ALLOWED = re.compile(r"[^A-Za-z0-9._-]+")


def sanitize(value: str) -> str:
    """Reduce a display string to a safe, readable file-name component."""
    collapsed = _ALLOWED.sub("-", value.strip())
    collapsed = re.sub(r"-{2,}", "-", collapsed)   # no runs of dashes
    return collapsed.strip("-")


def landing_key(identifier: str, file_hash: str, ext: str) -> str:
    """Object key in the landing bucket: hash-versioned, so identical content
    re-writes the same key (idempotent) and changed content gets a NEW key
    (the landing zone is never overwritten)."""
    return f"{sanitize(identifier)}/{file_hash}.{ext}"


def curated_filename(identifier: str, ref_no: str | None, ext: str) -> str:
    """Canonical curated name `identifier.ext`, with the ref_no tiebreaker
    when it carries extra information (legacy EAT collisions)."""
    ident = sanitize(identifier)
    ref = sanitize(ref_no) if ref_no else ""
    if ref and ref != ident:
        return f"{ident}__{ref}.{ext}"
    return f"{ident}.{ext}"
