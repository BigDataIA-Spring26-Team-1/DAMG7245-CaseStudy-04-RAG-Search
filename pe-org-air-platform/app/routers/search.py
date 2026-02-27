from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.search.vector_store import VectorStore
from app.services.retrieval.hybrid import HybridRetriever

router = APIRouter(prefix="/api/v1/search", tags=["search"])
logger = logging.getLogger("uvicorn.error")

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

    mode: str = Query("hybrid", description="semantic | hybrid"),

    top_k: int = Query(5, ge=1, le=20),

    company_id: Optional[str] = Query(None),

    dimension: Optional[str] = Query(None),

    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),

) -> Dict[str, Any]:

    """

    Evidence search endpoint.

    semantic: Chroma only

    hybrid: Chroma + BM25 fused with RRF

    """

    # Strip accidental whitespace from company_id passed via Swagger UI
    if company_id:
        company_id = company_id.strip()

    # ---- Preserve your filter logic ----

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

    mode = (mode or "hybrid").lower().strip()

    # ---- Semantic only ----

    if mode == "semantic" or (mode == "hybrid" and not company_id):

        try:
            hits = get_vector_store().query(query_text=q, top_k=top_k, where=where)
        except Exception as exc:
            logger.error("search_semantic_failed q=%s err=%s", q, exc)
            raise HTTPException(status_code=503, detail=f"Search unavailable: {exc}")

        return {

            "query": q,

            "mode": "semantic" if mode == "semantic" else "semantic_fallback",

            "top_k": top_k,

            "filters": {"company_id": company_id, "dimension": dimension, "min_confidence": min_confidence},

            "results": [

                {"id": h.id, "score": h.score, "text": h.text, "metadata": h.metadata}

                for h in hits

            ],

        }

    # ---- Hybrid mode ----

    if mode != "hybrid":

        return {"error": "Invalid mode. Use mode=semantic or mode=hybrid."}

    try:
        fused_hits = get_hybrid().search(

            query=q,

            top_k=top_k,

            company_id=company_id,

            dimension=dimension,

            min_confidence=min_confidence,

        )
    except Exception as exc:
        logger.error("search_hybrid_failed q=%s company_id=%s err=%s", q, company_id, exc)
        raise HTTPException(status_code=503, detail=f"Hybrid search unavailable: {exc}")

    return {

        "query": q,

        "mode": "hybrid",

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

            for h in fused_hits

        ],

    }
