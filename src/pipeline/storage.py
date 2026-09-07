"""Storage clients and schema guarantees.

MongoDB holds one metadata document per case URL. The unique index on `url`
is the idempotency backstop: even buggy application code cannot create
duplicate records, and concurrent upserts on the same URL resolve atomically
instead of racing.
"""

import boto3
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from pipeline.config import Settings


def mongo_client(s: Settings) -> MongoClient:
    return MongoClient(
        host=s.mongo_host,
        port=s.mongo_port,
        username=s.mongo_user,
        password=s.mongo_password,
        serverSelectionTimeoutMS=s.mongo_timeout_ms,
    )


def landing_collection(client: MongoClient, s: Settings) -> Collection:
    coll = client[s.mongo_db][s.landing_collection]
    coll.create_index([("url", ASCENDING)], unique=True)
    coll.create_index([("partition_date", ASCENDING)])
    coll.create_index([("published_date", ASCENDING)])
    return coll


def curated_collection(client: MongoClient, s: Settings) -> Collection:
    coll = client[s.mongo_db][s.curated_collection]
    coll.create_index([("url", ASCENDING)], unique=True)
    # D5 backstop: a filename collision must fail loudly, never overwrite
    coll.create_index([("file_path", ASCENDING)], unique=True)
    coll.create_index([("published_date", ASCENDING)])
    return coll


def s3_client(s: Settings):
    return boto3.client(
        "s3",
        endpoint_url=s.minio_endpoint,
        aws_access_key_id=s.minio_root_user,
        aws_secret_access_key=s.minio_root_password,
    )


def ensure_bucket(s3, name: str) -> None:
    from botocore.exceptions import ClientError
    try:
        s3.head_bucket(Bucket=name)
        return
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchBucket"):
            raise  # a 403/transient error is not "missing" — don't mask it
    try:
        s3.create_bucket(Bucket=name)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "BucketAlreadyOwnedByYou":
            raise
