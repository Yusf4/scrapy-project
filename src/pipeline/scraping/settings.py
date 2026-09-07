"""Scrapy settings, sourced from the central environment-driven config.

Values and their justification (measured saturation curve, observed latency
spikes, retry-loss arithmetic) are documented in ARCHITECTURE.md.
"""

from pipeline.config import get_settings

_s = get_settings()

BOT_NAME = "decisions"
SPIDER_MODULES = ["pipeline.scraping.spiders"]

USER_AGENT = _s.user_agent

# robots.txt disallows the case-document paths this project is required to
# fetch. Overriding it is a deliberate, documented decision (ARCHITECTURE.md):
# public legal records; a deliberate, documented choice; strict politeness caps below.
ROBOTSTXT_OBEY = False

# Politeness: cap at the highest concurrency measured healthy on this server;
# AutoThrottle floats below the cap based on live latency.
CONCURRENT_REQUESTS = _s.concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = _s.concurrent_requests
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_TARGET_CONCURRENCY = _s.autothrottle_target
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 60.0

# Resilience: worst observed transient spike was 37 s (succeeded on retry),
# so 60 s separates "slow but alive" from "dead" without holding a slot long.
DOWNLOAD_TIMEOUT = _s.download_timeout
RETRY_TIMES = _s.retry_times
# RETRY_HTTP_CODES / RETRY_EXCEPTIONS / RETRY_PRIORITY_ADJUST: Scrapy defaults
# (5xx/408/429 + transport errors always retried; retries queue behind fresh
# requests so one sick URL never stalls partition progress).

LOG_LEVEL = _s.log_level

ITEM_PIPELINES = {
    "pipeline.scraping.pipelines.MongoMetadataPipeline": 300,
    "pipeline.scraping.pipelines.FileStoragePipeline": 400,
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
