# Workplace Relations decisions pipeline

A partitioned scraping pipeline for the Irish [Workplace Relations](https://www.workplacerelations.ie) Decisions and Determinations database (~65,000 employment-law rulings across four tribunals). It scrapes the records into an immutable **landing zone**, then derives a cleaned **curated zone** — orchestrated by Dagster, with idempotent re-runs and per-partition completeness reconciliation.

- **Ingestion** (Scrapy): metadata → MongoDB, raw documents (HTML/PDF) → MinIO object storage.
- **Transformation**: HTML reduced to decision content, PDFs passed through untouched, files renamed to
  their case identifier, written to a separate curated bucket + collection.
- **Orchestration** (Dagster): two monthly-partitioned assets, `landing_docs → curated_docs`, with a
  partition-to-partition dependency and a reconciliation gate.

Verified end-to-end against a full year (**2024**): **3,245 documents** across 12 monthly partitions and 4 tribunals, zero failures, no duplicate URLs, every record with a stored file and hash — a count that independently matches the site's own stated total for the year.

## Requirements

- **Docker** (for MongoDB + MinIO) and **Docker Compose**
- **Python 3.11 or 3.12** (the project targets `>=3.11`; 3.13+ is untested)
- Optionally [`uv`](https://github.com/astral-sh/uv) for faster installs

## Setup

```bash
# 1. Configuration — copy the template and set real values (see notes below)
cp .env.example .env          # then edit MONGO_PASSWORD and MINIO_ROOT_PASSWORD

# 2. Start the two stores (compose FAILS FAST if .env is missing — that is intended)
docker compose up -d
docker compose ps             # wait for both services to report (healthy)

# 3. Install the pipeline into a virtual environment
python3 -m venv .venv && source .venv/bin/activate   # use a 3.11 or 3.12 interpreter
pip install -e ".[dev]"       # or:  uv pip install -e ".[dev]"

# 4. Smoke-check: settings load and both stores answer
python scripts/check_env.py
```

`.env` notes: the app builds its MongoDB connection string from the individual `MONGO_*` parts (one credential source), and MinIO/S3 credentials from `MINIO_ROOT_*`. Every tunable — connection details, bucket/collection names, partition size, and the scraping parameters (concurrency, timeout, retries, AutoThrottle target) — is an environment variable; there are no hardcoded values in the code.

## Running

The pipeline stages run standalone from the command line, or together under Dagster.

### Command line

```bash
# Ingest a date range (inclusive), sliced into monthly partitions, all 4 bodies by default.
# Add -a bodies="Workplace Relations Commission,Labour Court" to subset.
scrapy crawl decisions -a start=2024-01-01 -a end=2024-12-31

# Transform a range: landing zone -> curated zone
python -m pipeline.transform --start 2024-01-01 --end 2024-12-31
```

### Dagster (orchestrated)

```bash
make dev          # exports DAGSTER_HOME and starts the UI + daemon → http://localhost:3000
```

In the UI: **Assets → Materialize** a partition of `landing_docs` (runs the crawl for that month), then `curated_docs` (runs the transform). The partition grid shows which months exist, each materialization carries reconciliation metadata (expected / scraped / failed), and a month will not materialize green unless every (partition × body) unit reconciles.

> **Important:** always start Dagster via `make dev`. It exports `DAGSTER_HOME` so the
> `max_concurrent_runs: 1` politeness cap in `dagster_home/dagster.yaml` is active. Without it, Dagster
> uses an ephemeral instance whose run queue defaults to 10 — which would run multiple crawls at once and
> exceed the site's measured concurrency budget.

## Inspecting the data

- **MinIO console:** http://localhost:9001 (credentials from `.env`) — browse the `landing` and `curated`
  buckets.
- **MongoDB:** `docker compose exec mongo mongosh -u <user> -p <password>` — the `legal_docs` database
  holds the `landing_metadata` and `curated_metadata` collections.
- **Run logs:** `logs/<run_id>.jsonl` — one structured JSON event log per run, ending in a reconciliation
  summary.

## Tests

```bash
pytest -m "not integration"   # unit tests only — no containers needed (56 tests)
pytest                        # full suite — requires the compose stores running (60 tests)
```

Integration tests exercise the real MongoDB/MinIO round-trip (idempotency, file versioning, the run-twice contract); they are marked so they can be excluded when the stores are not up.

## Layout

```
src/pipeline/
  config.py            central env-driven settings (no hardcoded values)
  partitions.py        date range → inclusive monthly windows
  naming.py            filename sanitisation + curated identifier.ext
  hashing.py           raw SHA-256 + change-detection fingerprint
  events.py            JSON event log + reconciliation
  storage.py           Mongo/MinIO clients + indexes/buckets
  transform.py         landing → curated
  scraping/            spider, parsers (pure), items, storage pipelines, settings
  orchestration/       Dagster assets (landing_docs → curated_docs)
tests/                 unit + integration tests, with real page fixtures
```

Design decisions — partition size, retry/rate-limiting, deduplication, and scaling to 50+ sources — are documented in [ARCHITECTURE.md](ARCHITECTURE.md).
