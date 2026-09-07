"""Smoke check: settings load, MongoDB answers, MinIO answers.

Usage:  python scripts/check_env.py
Exits non-zero if any dependency is unreachable.
"""

import sys

import boto3
from pymongo import MongoClient

sys.path.insert(0, "src")
from pipeline.config import get_settings


def main() -> int:
    s = get_settings()
    print(f"settings loaded: db={s.mongo_db} buckets={s.landing_bucket},{s.curated_bucket} "
          f"partition={s.partition_size} concurrency={s.concurrent_requests}")

    client = MongoClient(
        host=s.mongo_host,
        port=s.mongo_port,
        username=s.mongo_user,
        password=s.mongo_password,
        serverSelectionTimeoutMS=5000,
    )
    client.admin.command("ping")
    print(f"mongo ok: {s.mongo_host}:{s.mongo_port}")

    s3 = boto3.client(
        "s3",
        endpoint_url=s.minio_endpoint,
        aws_access_key_id=s.minio_root_user,
        aws_secret_access_key=s.minio_root_password,
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    print(f"minio ok: {s.minio_endpoint} buckets={buckets}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
