"""Landing -> curated transformation.

Given a start/end date, reads metadata from the landing collection, fetches the
corresponding objects from the landing bucket and produces the curated zone:

  - PDF/DOC documents pass through untouched.
  - HTML pages are reduced to the decision content itself (the page title and
    the content container), dropping site chrome: header, navigation, cookie
    banner, search box, language/translate widgets, footer, scripts.
  - Every file is renamed to identifier.ext (sanitised; ref_no tiebreaker for
    the legacy identifier collisions) and written to the curated bucket.
  - One curated metadata document per record, with the new path and the hash
    of the transformed file.

The landing zone is never modified: this script only reads from it. The
curated zone is derived and regenerable, so re-running overwrites curated
objects in place (same name, deterministic content) — idempotent by
construction. A unique index on the curated file_path turns any residual
filename collision into a loud error instead of a silent overwrite.

Usage: python -m pipeline.transform --start 2024-01-01 --end 2024-01-31
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup
from pymongo.errors import DuplicateKeyError

from pipeline.config import Settings, get_settings
from pipeline.events import EventLog
from pipeline.hashing import raw_sha256
from pipeline.naming import curated_filename
from pipeline.storage import (curated_collection, ensure_bucket,
                              landing_collection, mongo_client, s3_client)

PASSTHROUGH_TYPES = {"pdf", "doc", "docx"}
HTML_TYPES = {"html", "htm"}


def extract_content(html: str | bytes) -> str:
    """Reduce a case page to its decision content.

    Keeps the page title (h1.page-title) and the content container
    (div.content) that holds the decision text on every observed page type;
    everything else on the page is chrome.
    """
    soup = BeautifulSoup(html, "html.parser")
    title = soup.select_one("h1.page-title")
    content = soup.select_one("div.content")

    doc = BeautifulSoup(
        "<html><head><meta charset='utf-8'></head><body></body></html>",
        "html.parser",
    )
    body = doc.body
    if title:
        body.append(title)
    if content:
        for tag in content.select("script, style"):
            tag.decompose()
        body.append(content)
    return str(doc)


def transform_range(start: str, end: str, settings: Settings | None = None) -> dict:
    s = settings or get_settings()
    run_id = datetime.now(timezone.utc).strftime("transform_%Y%m%dT%H%M%SZ")
    events = EventLog(Path(s.log_dir) / f"{run_id}.jsonl", run_id)
    events.emit("transform_start", start=start, end=end)

    client = mongo_client(s)
    landing = landing_collection(client, s)
    curated = curated_collection(client, s)
    s3 = s3_client(s)
    ensure_bucket(s3, s.curated_bucket)

    counts = {"records": 0, "transformed": 0, "passthrough": 0,
              "missing_file": 0, "name_collisions": 0, "failed": 0,
              "thin_content": 0}

    query = {"published_date": {"$gte": start, "$lte": end}}
    for record in landing.find(query):
        counts["records"] += 1
        if not record.get("file_path"):
            counts["missing_file"] += 1
            events.emit("missing_file", url=record["url"],
                        reason="no landing file on record")
            continue

        # One bad record must not cost the run — and that guard covers the
        # WHOLE record (fetch, extract, rename, catalog write, object write),
        # not just the fetch: a transient store hiccup on record 200 of 300
        # is that record's failure, logged with its reason; the loop continues.
        try:
            raw = s3.get_object(Bucket=s.landing_bucket,
                                Key=record["file_path"])["Body"].read()
            doc_type = record.get("doc_type", "html")

            if doc_type not in HTML_TYPES:
                # Only HTML is reduced; anything else (pdf/doc/docx, or an
                # unrecognised type) passes through untouched — never run an
                # unknown binary through the HTML parser.
                kind = "passthrough"
                content = raw
            else:
                kind = "transformed"
                extracted = extract_content(raw)
                text = BeautifulSoup(extracted, "html.parser").get_text(strip=True)
                if len(text) < s.thin_content_min_chars:
                    # Data quality: a decision reduced to (almost) nothing means
                    # an unrecognised page shape — flagged, stored for inspection.
                    counts["thin_content"] += 1
                    events.emit("thin_content", url=record["url"],
                                chars=len(text), file_path=record["file_path"])
                content = extracted.encode("utf-8")

            name = curated_filename(record["identifier"], record.get("ref_no"), doc_type)
            new_hash = raw_sha256(content)

            curated_doc = {
            "identifier": record["identifier"],
            "ref_no": record.get("ref_no"),
            "description": record.get("description"),
            "published_date": record["published_date"],
            "partition_date": record["partition_date"],
            "bodies": record.get("bodies", []),
            "doc_type": doc_type,
            "file_path": name,
            "file_hash": new_hash,
            "source_file_path": record["file_path"],
            "source_file_hash": record.get("file_hash"),
            "transformed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "transform_run_id": run_id,
        }
            # Claim the filename in the catalog FIRST — the unique index on
            # file_path is the reservation system. Only a successful claim may
            # write the object: in the reverse order a colliding record would
            # overwrite the rightful owner's curated file before the
            # DuplicateKeyError ever fired (backstop guarding the wrong store).
            # A crash between claim and write self-heals on rerun (idempotent).
            try:
                curated.update_one({"url": record["url"]},
                                   {"$set": curated_doc}, upsert=True)
            except DuplicateKeyError:
                counts["name_collisions"] += 1
                events.emit("name_collision", url=record["url"], file_path=name)
                continue
            s3.put_object(Bucket=s.curated_bucket, Key=name, Body=content)
            counts[kind] += 1
        except Exception as exc:
            counts["failed"] += 1
            events.emit("transform_failed", url=record["url"],
                        file_path=record.get("file_path"),
                        reason=type(exc).__name__, detail=str(exc)[:200])
            continue

    # data quality: identical content behind two different URLs
    duplicates = list(landing.aggregate([
        {"$match": {**query, "content_fingerprint": {"$ne": None}}},
        {"$group": {"_id": "$content_fingerprint", "urls": {"$push": "$url"},
                    "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}},
    ]))
    for dup in duplicates:
        events.emit("content_duplicate", urls=dup["urls"])

    events.emit("transform_summary", **counts, content_duplicates=len(duplicates))
    events.close()
    client.close()
    print(f"{run_id}: {counts} content_duplicates={len(duplicates)}")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="ISO date, inclusive")
    parser.add_argument("--end", required=True, help="ISO date, inclusive")
    args = parser.parse_args()
    transform_range(args.start, args.end)


if __name__ == "__main__":
    main()
