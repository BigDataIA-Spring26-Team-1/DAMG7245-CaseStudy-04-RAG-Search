from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from app.services.retrieval.hybrid import HybridRetriever
from app.services.search.vector_store import VectorStore

router = APIRouter(prefix="/api/v1/search", tags=["search"])

_vector_store: Optional[VectorStore] = None
_hybrid: Optional[HybridRetriever] = None


def get_vector_store() -> VectorStore:
    global _vector_store
    if _vector_store is None:
        _vector_store = VectorStore()
    return _vector_store


def get_hybrid() -> HybridRetriever:
    global _hybrid
    if _hybrid is None:
        _hybrid = HybridRetriever()
    return _hybrid


@router.get("")
def search(
    q: str = Query(..., description="Search query text"),
    mode: str = Query("semantic", description="Retrieval mode: semantic | bm25 | hybrid"),
    top_k: int = Query(5, ge=1, le=20),
    company_id: Optional[str] = Query(None),
    dimension: Optional[str] = Query(None),
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
) -> Dict[str, Any]:
    """
    Search over evidence chunks.
    - semantic: VectorStore (Chroma)
    - hybrid: RRF fuse(semantic, bm25) + Snowflake metadata enrichment for BM25-only hits
    - bm25: BM25-only (requires company_id)
    """
    mode = (mode or "semantic").strip().lower()

    # ---------------------------
    # Hybrid / BM25 path
    # ---------------------------
    if mode in {"hybrid", "bm25"}:
        if mode == "bm25" and not company_id:
            return {
                "query": q,
                "mode": mode,
                "top_k": top_k,
                "filters": {"company_id": company_id, "dimension": dimension, "min_confidence": min_confidence},
                "results": [],
                "warning": "mode=bm25 requires company_id",
            }

        hits = get_hybrid().search(
            query=q,
            top_k=top_k,
            company_id=company_id,
            dimension=dimension,
            min_confidence=min_confidence,
        )

        # If user explicitly asks bm25-only, keep only those with bm25_score
        if mode == "bm25":
            hits = [h for h in hits if h.bm25_score is not None][:top_k]

        return {
            "query": q,
            "mode": mode,
            "top_k": top_k,
            "filters": {"company_id": company_id, "dimension": dimension, "min_confidence": min_confidence},
            "results": [
                {
                    "id": h.id,
                    "score": h.score,
                    "semantic_score": h.semantic_score,
                    "bm25_score": h.bm25_score,
                    "text": h.text,
                    "metadata": h.metadata,
                }
                for h in hits
            ],
        }

    # ---------------------------
    # Semantic-only path
    # ---------------------------
    filter_clauses = []

    if company_id:
        filter_clauses.append({"company_id": company_id})
    if dimension:
        filter_clauses.append({"dimension": dimension})
    if min_confidence is not None:
        filter_clauses.append({"confidence": {"$gte": float(min_confidence)}})

    where: Optional[Dict[str, Any]] = None
    if len(filter_clauses) == 1:
        where = filter_clauses[0]
    elif len(filter_clauses) > 1:
        where = {"$and": filter_clauses}

    hits = get_vector_store().query(query_text=q, top_k=top_k, where=where)

    return {
        "query": q,
        "mode": "semantic",
        "top_k": top_k,
        "filters": {"company_id": company_id, "dimension": dimension, "min_confidence": min_confidence},
        "results": [{"id": h.id, "score": h.score, "text": h.text, "metadata": h.metadata} for h in hits],
    }