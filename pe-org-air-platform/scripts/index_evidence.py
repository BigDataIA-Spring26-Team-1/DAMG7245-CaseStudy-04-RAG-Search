from __future__ import annotations

import argparse
from typing import Any, Dict, List

from app.services.integration.evidence_client import EvidenceClient
from app.services.retrieval.dimension_mapper import map_dimension
from app.services.search.vector_store import DocumentChunk, VectorStore


def build_chunk_id(document_id: str, chunk_id: str) -> str:
    # Stable + globally unique ID for Chroma
    return f"{document_id}:{chunk_id}"


def row_to_docchunk(row) -> DocumentChunk:
    dimension = map_dimension(row.source_type, row.signal_category, row.chunk_text)

    metadata: Dict[str, Any] = {
        "company_id": row.company_id,
        "document_id": row.document_id,
        "chunk_id": row.chunk_id,
        "chunk_index": row.chunk_index,
        "dimension": dimension,
        "source_type": row.source_type,
        "signal_category": row.signal_category,
        "confidence": row.confidence if row.confidence is not None else 0.0,
        "source_url": row.source_url,
        "fiscal_year": row.fiscal_year,
        "title": row.title,
        "doc_type": row.doc_type,
        "published_at": row.published_at,
    }

    return DocumentChunk(
        id=build_chunk_id(row.document_id, row.chunk_id),
        text=row.chunk_text,
        metadata=metadata,
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--company-id", required=True)
    ap.add_argument("--schema", default=None, help="Optional Snowflake schema, e.g. PUBLIC")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--min-confidence", type=float, default=None)
    ap.add_argument("--reindex", action="store_true", help="Delete existing index entries for this company first")
    args = ap.parse_args()

    store = VectorStore()
    client = EvidenceClient(schema=args.schema)

    if args.reindex:
        deleted = store.delete_by_filter({"company_id": args.company_id})
        print(f"Deleted {deleted} existing vectors for company_id={args.company_id}")

    total = 0
    for batch in client.iter_chunks_for_company(
        company_id=args.company_id,
        batch_size=args.batch_size,
        min_confidence=args.min_confidence,
    ):
        chunks: List[DocumentChunk] = [row_to_docchunk(r) for r in batch if (r.chunk_text or "").strip()]
        upserted = store.upsert(chunks)
        total += upserted
        print(f"Upserted {upserted} chunks (running total={total})")

    print(f"Done. Indexed total chunks={total} for company_id={args.company_id}")


if __name__ == "__main__":
    main()
