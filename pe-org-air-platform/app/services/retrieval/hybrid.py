from __future__ import annotations
 
from dataclasses import dataclass

from typing import Any, Dict, List, Optional
 
from app.services.retrieval.bm25_store import BM25Store

from app.services.search.vector_store import SearchHit, VectorStore
 
 
@dataclass(frozen=True)

class HybridHit:

    id: str

    text: str

    score: float  # final fused score (RRF)

    metadata: Dict[str, Any]

    semantic_score: Optional[float] = None

    bm25_score: Optional[float] = None
 
 
def rrf_fuse(

    semantic_hits: List[SearchHit],

    bm25_hits: List,

    k: int = 60,

) -> List[HybridHit]:

    """

    Reciprocal Rank Fusion.

    Score(doc) = sum( 1 / (k + rank_i(doc)) ) across retrieval lists.

    """

    fused: Dict[str, Dict[str, Any]] = {}
 
    # semantic list

    for rank, h in enumerate(semantic_hits, start=1):

        entry = fused.setdefault(h.id, {"id": h.id, "text": h.text, "metadata": h.metadata})

        entry["semantic_score"] = h.score

        entry["rrf"] = entry.get("rrf", 0.0) + 1.0 / (k + rank)
 
    # bm25 list

    for rank, h in enumerate(bm25_hits, start=1):

        entry = fused.setdefault(h.chunk_uid, {"id": h.chunk_uid, "text": h.text, "metadata": {}})

        entry["bm25_score"] = h.score

        entry["rrf"] = entry.get("rrf", 0.0) + 1.0 / (k + rank)
 
    results: List[HybridHit] = []

    for _, v in fused.items():

        results.append(

            HybridHit(

                id=v["id"],

                text=v.get("text", ""),

                metadata=v.get("metadata", {}) or {},

                score=float(v.get("rrf", 0.0)),

                semantic_score=v.get("semantic_score"),

                bm25_score=v.get("bm25_score"),

            )

        )
 
    results.sort(key=lambda x: x.score, reverse=True)

    return results
 
 
class HybridRetriever:

    """

    Combines semantic retrieval (Chroma) and lexical retrieval (BM25) using RRF fusion.

    """
 
    def __init__(self, schema: Optional[str] = None) -> None:

        self.vector_store = VectorStore()

        self.bm25_store = BM25Store(schema=schema)
 
    def search(

        self,

        query: str,

        top_k: int = 5,

        company_id: Optional[str] = None,

        dimension: Optional[str] = None,

        min_confidence: Optional[float] = None,

        semantic_k: int = 10,

        bm25_k: int = 10,

    ) -> List[HybridHit]:

        # Semantic side supports metadata filtering
        # ChromaDB requires exactly one operator at the top level of a where clause.
        # Multiple conditions must be wrapped in $and.

        clauses: List[Dict[str, Any]] = []

        if company_id:

            clauses.append({"company_id": {"$eq": company_id}})

        if dimension:

            clauses.append({"dimension": {"$eq": dimension}})

        if min_confidence is not None:

            clauses.append({"confidence": {"$gte": float(min_confidence)}})

        if len(clauses) == 0:

            chroma_where: Optional[Dict[str, Any]] = None

        elif len(clauses) == 1:

            chroma_where = clauses[0]

        else:

            chroma_where = {"$and": clauses}

        semantic_hits = self.vector_store.query(

            query_text=query,

            top_k=semantic_k,

            where=chroma_where,

        )
 
        bm25_hits = []

        # BM25 needs company_id (Option A)

        if company_id:

            bm25_hits = self.bm25_store.search(

                company_id=company_id,

                query=query,

                top_k=bm25_k,

                min_confidence=min_confidence,

                dimension=dimension,

            )
 
        fused = rrf_fuse(semantic_hits=semantic_hits, bm25_hits=bm25_hits)

        return fused[:top_k]
 