"""Item pipelines: persist listing metadata to MongoDB idempotently.

Field update semantics follow the static/dynamic classification:
  - $setOnInsert : facts fixed at first sight (first_scraped_at, partition_date)
  - $set         : refreshable state (description was observed amended in place
                   on the live site; file fields change when content changes)
  - $addToSet    : set-valued facts — `bodies` accumulates rather than
                   overwrites. Modelled as a set because a decision *could* be
                   listed under more than one tribunal's filter (the site
                   carries cross-body case types); not observed in sampled data
                   (285 records, all single-body), so this is defensive
                   modelling of a possible future, not a measured requirement —
                   and it is free: $addToSet on a one-element set is a no-op.

One atomic upsert per record; check-then-insert would race under concurrent
partition runs (TOCTOU), the unique index + upsert cannot.
"""

from datetime import datetime, timezone

from itemadapter import ItemAdapter

from pipeline.config import get_settings
from pipeline.hashing import content_fingerprint, raw_sha256
from pipeline.naming import landing_key
from pipeline.scraping.items import DecisionItem, FileItem
from pipeline.storage import ensure_bucket, landing_collection, mongo_client, s3_client


def build_metadata_update(item: dict, now_iso: str, run_id: str) -> tuple[dict, dict, list]:
    """Pure function: (filter, update) for one record's upsert.

    Separated from the pipeline so the update semantics are unit-testable
    without a database or a spider.
    """
    filter_ = {"url": item["url"]}
    update = {
        "$setOnInsert": {
            "first_scraped_at": now_iso,
            "partition_date": item["partition_date"],
        },
        "$set": {
            "identifier": item["identifier"],
            "published_date": item["published_date"],
            "last_seen_at": now_iso,
            "last_run_id": run_id,
        },
        "$addToSet": {"bodies": item["body"]},
    }
    # Last-non-null-wins: some bodies' cards omit description/ref_no (measured:
    # legacy EAT cards have no description). Writing None here would destroy a
    # value captured under another body — and a nulled ref_no silently flips
    # the curated filename, re-creating the collision the tiebreaker prevents.
    # Listing pages are never stored, so these fields exist ONLY in Mongo: an
    # erased value is unrecoverable, a stale one heals on the next crawl —
    # when forced to guess, guess toward the reversible mistake. The skip is
    # counted and logged by the pipeline so the pattern stays visible: one
    # occurrence is a glitch; a whole partition means the site changed its cards.
    absent = []
    for optional in ("ref_no", "description"):
        if item.get(optional) is not None:
            update["$set"][optional] = item[optional]
        else:
            absent.append(optional)
    return filter_, update, absent


class MongoMetadataPipeline:
    def open_spider(self, spider):
        self.settings = get_settings()
        self.client = mongo_client(self.settings)
        self.collection = landing_collection(self.client, self.settings)
        # one run id for the whole run, owned by the spider
        self.run_id = getattr(spider, "run_id",
                              datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ"))

    def close_spider(self, spider):
        stats = spider.crawler.stats
        spider.logger.info(
            "metadata summary run=%s inserted=%s updated=%s",
            self.run_id,
            stats.get_value("metadata/inserted", 0),
            stats.get_value("metadata/updated", 0),
        )
        self.client.close()

    def process_item(self, item, spider):
        if not isinstance(item, DecisionItem):
            return item
        record = ItemAdapter(item).asdict()
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        filter_, update, absent = build_metadata_update(record, now_iso, self.run_id)
        for field in absent:
            spider.crawler.stats.inc_value(f"fields_absent/{field}")
            if hasattr(spider, "events"):
                spider.events.emit("field_absent", url=record["url"], field=field,
                                   partition=record["partition_date"],
                                   body=record["body"])
        result = self.collection.update_one(filter_, update, upsert=True)
        key = "metadata/inserted" if result.upserted_id is not None else "metadata/updated"
        spider.crawler.stats.inc_value(key)
        unit = (record["partition_date"], record["body"])
        if hasattr(spider, "scraped_count"):
            spider.scraped_count[unit] += 1
        return item


class FileStoragePipeline:
    """Persist fetched documents to the landing bucket, idempotently.

    Change detection is download-and-hash: the site's HTML sends no
    ETag/Last-Modified, so comparing SHA-256 against the stored hash is the
    only possible detector. An unchanged file is skipped (no write, no record
    churn); a changed file goes to a NEW hash-keyed object — the landing zone
    is append-only, so the previous version remains untouched.
    """

    KIND_FIELDS = {
        "primary": ("file_path", "file_hash", "content_fingerprint"),
        "stub": ("stub_file_path", "stub_file_hash", "stub_content_fingerprint"),
    }

    def open_spider(self, spider):
        self.settings = get_settings()
        self.client = mongo_client(self.settings)
        self.collection = landing_collection(self.client, self.settings)
        self.s3 = s3_client(self.settings)
        ensure_bucket(self.s3, self.settings.landing_bucket)

    def close_spider(self, spider):
        stats = spider.crawler.stats
        spider.logger.info(
            "files summary stored=%s skipped_unchanged=%s changed=%s orphaned=%s",
            stats.get_value("files/stored", 0),
            stats.get_value("files/skipped_unchanged", 0),
            stats.get_value("files/changed", 0),
            stats.get_value("files/orphaned", 0),
        )
        self.client.close()

    def process_item(self, item, spider):
        if not isinstance(item, FileItem):
            return item
        stats = spider.crawler.stats

        path_field, hash_field, fp_field = self.KIND_FIELDS[item.kind]
        new_hash = raw_sha256(item.content)
        fingerprint = content_fingerprint(item.content, item.doc_type)

        existing = self.collection.find_one({"url": item.url}, {fp_field: 1})
        if existing is None:
            # Metadata normally lands before its file; absence is an
            # out-of-order anomaly. Store NOTHING: bytes without a catalog
            # entry are unreachable (a half-write). Count it, log it, and let
            # reconciliation surface the gap; the rerun heals it.
            stats.inc_value("files/orphaned")
            spider.logger.warning("file before metadata — skipped url=%s", item.url)
            return item
        if existing.get(fp_field) == fingerprint:
            # Same content once volatile chrome (server-debug comments) is
            # ignored: skip the write and the record churn entirely.
            stats.inc_value("files/skipped_unchanged")
            return item
        elif existing.get(fp_field) is not None:
            stats.inc_value("files/changed")
            spider.logger.info("content changed url=%s kind=%s", item.url, item.kind)

        key = landing_key(item.identifier, new_hash, item.doc_type)
        self.s3.put_object(
            Bucket=self.settings.landing_bucket, Key=key, Body=item.content
        )
        update = {path_field: key, hash_field: new_hash, fp_field: fingerprint}
        if item.kind == "primary":
            update["doc_type"] = item.doc_type
            update["file_source_url"] = item.source_url
        self.collection.update_one({"url": item.url}, {"$set": update})
        stats.inc_value("files/stored")
        return item
