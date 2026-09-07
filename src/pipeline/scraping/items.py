"""Typed items produced by the spider. Scrapy supports dataclass items natively
(via itemadapter), which gives us typing and IDE support over dict items."""

from dataclasses import dataclass


@dataclass
class DecisionItem:
    """One decision/determination record as listed in the search results.

    `url` is the record's identity everywhere (unique index in MongoDB);
    `identifier` is the human-facing case number and is NOT unique.
    File-related fields are filled in by the download stage.
    """

    url: str
    identifier: str
    ref_no: str | None
    description: str | None
    published_date: str          # ISO date (source shows dd/mm/yyyy)
    body: str                    # tribunal name this record was found under
    partition_date: str          # partition key, e.g. "2024-01"
    file_path: str | None = None
    file_hash: str | None = None
    doc_type: str | None = None  # "html" | "pdf" | "doc"


@dataclass
class FileItem:
    """Raw bytes of one fetched document, en route to object storage.

    kind="primary": the document that IS the decision (case page HTML, or the
    PDF behind a legacy EAT stub). kind="stub": the stub page itself, kept
    because the pipeline stores the HTML of every record's page.
    """

    url: str          # record identity (the case URL in the metadata store)
    identifier: str
    source_url: str   # what was actually fetched (case page or PDF)
    kind: str         # "primary" | "stub"
    doc_type: str     # "html" | "pdf" | "doc" | "docx"
    content: bytes
