"""Unit tests for the pure upsert-document builder, and an integration test of
the idempotency contract against the local MongoDB from docker-compose.

Run-twice invariant (idempotency): re-processing the same record
must not create a duplicate, must not change first-seen facts, must refresh
dynamic state, and must accumulate `bodies` when a record is seen under a
second tribunal.
"""

import os
from datetime import datetime, timezone

import pytest

from pipeline.scraping.pipelines import build_metadata_update

ITEM = {
    "url": "https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html",
    "identifier": "ADJ-00047352",
    "ref_no": "ADJ-00047352",
    "description": "Declan Holden V Ger Brennan Construction",
    "published_date": "2024-01-31",
    "body": "Workplace Relations Commission",
    "partition_date": "2024-01",
}


def test_filter_is_url_only():
    filter_, _, _ = build_metadata_update(ITEM, "2026-09-05T10:00:00+00:00", "run_1")
    assert filter_ == {"url": ITEM["url"]}


def test_static_fields_only_on_insert():
    _, update, _ = build_metadata_update(ITEM, "2026-09-05T10:00:00+00:00", "run_1")
    assert set(update["$setOnInsert"]) == {"first_scraped_at", "partition_date"}


def test_dynamic_fields_refreshed():
    _, update, _ = build_metadata_update(ITEM, "2026-09-05T10:00:00+00:00", "run_1")
    for f in ("identifier", "ref_no", "description", "published_date",
              "last_seen_at", "last_run_id"):
        assert f in update["$set"]


def test_optional_fields_never_clobbered_with_none():
    """Regression (review): a card without description/ref_no (measured on
    legacy EAT listings) must not null values captured under another body —
    a nulled ref_no would flip the curated filename order-dependently."""
    bare = {**ITEM, "description": None, "ref_no": None}
    _, update, absent = build_metadata_update(bare, "2026-09-06T10:00:00+00:00", "run_1")
    assert "description" not in update["$set"]
    assert "ref_no" not in update["$set"]
    assert sorted(absent) == ["description", "ref_no"]   # the skip is REPORTED
    _, update2, absent2 = build_metadata_update(ITEM, "2026-09-06T10:00:00+00:00", "run_1")
    assert update2["$set"]["ref_no"] == ITEM["ref_no"]
    assert absent2 == []


def test_bodies_accumulate_not_overwrite():
    _, update, _ = build_metadata_update(ITEM, "2026-09-05T10:00:00+00:00", "run_1")
    assert update["$addToSet"] == {"bodies": ITEM["body"]}
    assert "bodies" not in update["$set"]
    assert "body" not in update["$set"]


@pytest.mark.integration
def test_run_twice_is_idempotent_against_real_mongo():
    from pipeline.config import get_settings
    from pipeline.storage import landing_collection, mongo_client

    s = get_settings()
    client = mongo_client(s)
    coll = client[s.mongo_db]["test_idempotency"]
    coll.drop()
    coll.create_index("url", unique=True)

    def apply(item, run_id):
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        f, u, _ = build_metadata_update(item, now, run_id)
        return coll.update_one(f, u, upsert=True)

    r1 = apply(ITEM, "run_1")
    assert r1.upserted_id is not None

    first = coll.find_one({"url": ITEM["url"]})

    # same record, second run, seen under a second tribunal
    item2 = {**ITEM, "body": "Employment Appeals Tribunal",
             "description": "Declan Holden V Ger Brennan Construction [amended]"}
    r2 = apply(item2, "run_2")
    assert r2.upserted_id is None and r2.matched_count == 1

    assert coll.count_documents({}) == 1                       # no duplicate
    doc = coll.find_one({"url": ITEM["url"]})
    assert doc["first_scraped_at"] == first["first_scraped_at"]  # static kept
    assert doc["partition_date"] == "2024-01"
    assert doc["description"].endswith("[amended]")              # dynamic refreshed
    assert sorted(doc["bodies"]) == ["Employment Appeals Tribunal",
                                     "Workplace Relations Commission"]
    assert doc["last_run_id"] == "run_2"

    coll.drop()
    client.close()


@pytest.mark.integration
def test_file_storage_skip_and_version(tmp_path):
    """Unchanged bytes are skipped; changed bytes get a NEW key and the old
    landing object survives (append-only zone)."""
    from pipeline.config import get_settings
    from pipeline.scraping.items import FileItem
    from pipeline.scraping.pipelines import FileStoragePipeline
    from pipeline.storage import landing_collection, mongo_client, s3_client, ensure_bucket

    class _Stats:
        def __init__(self): self.d = {}
        def inc_value(self, k): self.d[k] = self.d.get(k, 0) + 1
        def get_value(self, k, default=0): return self.d.get(k, default)

    class _Crawler:
        stats = _Stats()

    class _Spider:
        crawler = _Crawler()
        import logging
        logger = logging.getLogger("test")

    s = get_settings()
    client = mongo_client(s)
    coll = landing_collection(client, s)
    url = "https://example.invalid/test-file-versioning"
    coll.delete_one({"url": url})
    coll.insert_one({"url": url, "identifier": "TEST-1"})

    pipe = FileStoragePipeline()
    spider = _Spider()
    pipe.open_spider(spider)
    item = FileItem(url=url, identifier="TEST-1", source_url=url,
                    kind="primary", doc_type="html", content=b"version one")

    pipe.process_item(item, spider)                    # store
    pipe.process_item(item, spider)                    # identical -> skip
    changed = FileItem(url=url, identifier="TEST-1", source_url=url,
                       kind="primary", doc_type="html", content=b"version two")
    pipe.process_item(changed, spider)                 # changed -> new key

    st = spider.crawler.stats
    assert st.get_value("files/stored") == 2
    assert st.get_value("files/skipped_unchanged") == 1
    assert st.get_value("files/changed") == 1

    doc = coll.find_one({"url": url})
    s3 = s3_client(s)
    keys = [o["Key"] for o in s3.list_objects_v2(
        Bucket=s.landing_bucket, Prefix="TEST-1/").get("Contents", [])]
    assert len(keys) == 2                              # both versions kept
    assert doc["file_path"] in keys                    # record points at latest
    import hashlib
    assert doc["file_hash"] == hashlib.sha256(b"version two").hexdigest()

    for k in keys:
        s3.delete_object(Bucket=s.landing_bucket, Key=k)
    coll.delete_one({"url": url})
    pipe.close_spider(spider)
    client.close()
