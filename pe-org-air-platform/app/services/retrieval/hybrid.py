from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.services.integration.evidence_client import EvidenceClient
from app.services.retrieval.bm25_store import BM25Hit, BM25Store
from app.services.search.vector_store import SearchHit, VectorStore


@dataclass(frozen=True)
class HybridHit:
    id: str
    text: str
    score: float  # fused score (RRF)
    metadata: Dict[str, Any]
    semantic_score: Optional[float] = None
    bm25_score: Optional[float] = None


def rrf_fuse(
    semantic_hits: List[SearchHit],
    bm25_hits: List[BM25Hit],
    k: int = 60,
) -> List[HybridHit]:
    """
    Reciprocal Rank Fusion (RRF):
      score(doc) = Σ 1 / (k + rank_i(doc))
    """
    fused: Dict[str, Dict[str, Any]] = {}

    # Semantic list
    for rank, h in enumerate(semantic_hits, start=1):
        entry = fused.setdefault(
            h.id,
            {"id": h.id, "text": h.text, "metadata": h.metadata or {}},
        )
        entry["semantic_score"] = h.score
        entry["rrf"] = entry.get("rrf", 0.0) + 1.0 / (k + rank)

    # BM25 list
    for rank, h in enumerate(bm25_hits, start=1):
        entry = fused.setdefault(
            h.chunk_uid,
            {"id": h.chunk_uid, "text": h.text, "metadata": {}},
        )
        entry["bm25_score"] = h.score
        entry["rrf"] = entry.get("rrf", 0.0) + 1.0 / (k + rank)

    results: List[HybridHit] = []
    for v in fused.values():
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
    Hybrid retrieval = Semantic (Chroma) + Lexical (BM25 over Snowflake chunks), fused via RRF.

    Enriches BM25-only hits with Snowflake metadata so citations work.
    """

    def __init__(self, schema: Optional[str] = None) -> None:
        self.vector_store = VectorStore()
        self.bm25_store = BM25Store(schema=schema)
        self.evidence = EvidenceClient(schema=schema)

    def _build_chroma_where(
        self,
        company_id: Optional[str] = None,
        dimension: Optional[str] = None,
        min_confidence: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        filters: List[Dict[str, Any]] = []

        if company_id:
            filters.append({"company_id": company_id})

        if dimension:
            filters.append({"dimension": dimension})

        if min_confidence is not None:
            filters.append({"confidence": {"$gte": float(min_confidence)}})

        if not filters:
            return None

        if len(filters) == 1:
            return filters[0]

        return {"$and": filters}

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
        # ---- Semantic (Chroma) supports metadata filters ----
        where = self._build_chroma_where(
            company_id=company_id,
            dimension=dimension,
            min_confidence=min_confidence,
        )

        semantic_hits = self.vector_store.query(
            query_text=query,
            top_k=semantic_k,
            where=where,
        )

        # ---- BM25 requires company_id ----
        bm25_hits: List[BM25Hit] = []
        if company_id:
            bm25_hits = self.bm25_store.search(
                company_id=company_id,
                query=query,
                top_k=bm25_k,
                min_confidence=min_confidence,
                dimension=dimension,
            )

        fused = rrf_fuse(semantic_hits=semantic_hits, bm25_hits=bm25_hits)[:top_k]

        # ---- ENRICH: BM25-only hits have empty metadata {} ----
        missing_uids = [h.id for h in fused if not h.metadata]
        if missing_uids:
            meta_map = self.evidence.get_chunk_metadata_by_uids(missing_uids)

            enriched: List[HybridHit] = []
            for h in fused:
                if h.metadata:
                    enriched.append(h)
                    continue

                md = meta_map.get(h.id, {}) or {}
                chunk_text = md.get("_chunk_text", "")

                md_clean = dict(md)
                md_clean.pop("_chunk_text", None)

                enriched.append(
                    HybridHit(
                        id=h.id,
                        text=h.text or chunk_text,
                        score=h.score,
                        metadata=md_clean,
                        semantic_score=h.semantic_score,
                        bm25_score=h.bm25_score,
                    )
                )
            fused = enriched

        return fused