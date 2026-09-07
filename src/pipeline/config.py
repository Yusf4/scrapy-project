"""Central configuration. Every tunable comes from the environment (.env locally,
real env vars in deployment) — no value is hardcoded anywhere else in the codebase.

Date range and body selection are deliberately NOT settings: they are run inputs,
passed per execution (CLI arguments / Dagster partition keys).
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # MongoDB — the URI is assembled from parts so credentials are defined once
    mongo_user: str
    mongo_password: str
    mongo_host: str = "localhost"
    mongo_port: int = 27017
    mongo_db: str = "legal_docs"
    landing_collection: str = "landing_metadata"
    curated_collection: str = "curated_metadata"

    # MinIO / S3
    minio_endpoint: str = "http://localhost:9000"
    minio_root_user: str
    minio_root_password: str
    landing_bucket: str = "landing"
    curated_bucket: str = "curated"

    # Partitioning
    partition_size: str = "monthly"
    partition_start_date: str = "2008-01-01"

    # Scraping politeness and resilience (measured values; see ARCHITECTURE.md)
    concurrent_requests: int = 8
    download_timeout: int = 60
    retry_times: int = 3
    autothrottle_target: float = 6.0
    latency_warn_threshold_s: float = 10.0
    thin_content_min_chars: int = 60          # below this, flag thin_content
    mongo_timeout_ms: int = 5000

    # Source
    base_url: str = "https://www.workplacerelations.ie"
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
    )
    # Body name -> the site's internal filter id (discovered from the search form)
    body_ids: dict[str, int] = {
        "Equality Tribunal": 1,
        "Employment Appeals Tribunal": 2,
        "Labour Court": 3,
        "Workplace Relations Commission": 15376,
    }

    # Logging
    log_level: str = "INFO"
    log_dir: str = "logs"


def get_settings() -> Settings:
    return Settings()
