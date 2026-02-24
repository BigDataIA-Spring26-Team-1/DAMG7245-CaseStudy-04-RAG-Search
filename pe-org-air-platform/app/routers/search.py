from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from app.services.search.vector_store import VectorStore

router = APIRouter(prefix="/api/v1/search", tags=["search"])

_vector_store: Optional[VectorStore] = None


def get_vector_store() -> VectorStore:
    global _vector_store
    if _vector_store is None:
        _vector_store = VectorStore()
    return _vector_store


@router.get("")
def search(
    q: str = Query(..., description="Search query text"),
    top_k: int = Query(5, ge=1, le=20),
    company_id: Optional[str] = Query(None),
    dimension: Optional[str] = Query(None),
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
) -> Dict[str, Any]:
    """
    Semantic search over evidence chunks with optional metadata filters.
    """
    where: Dict[str, Any] = {}

    if company_id:
        where["company_id"] = company_id
    if dimension:
        where["dimension"] = dimension

    if min_confidence is not None:
        where["confidence"] = {"$gte": float(min_confidence)}

    hits = get_vector_store().query(query_text=q, top_k=top_k, where=where if where else None)

    return {
        "query": q,
        "top_k": top_k,
        "filters": {"company_id": company_id, "dimension": dimension, "min_confidence": min_confidence},
        "results": [
            {"id": h.id, "score": h.score, "text": h.text, "metadata": h.metadata}
            for h in hits
        ],
    }