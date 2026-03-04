from __future__ import annotations
from typing import Any, Dict, List, Optional
from fastapi import APIRouter
from pydantic import BaseModel, Field
from app.services.retrieval.hybrid import HybridRetriever

router = APIRouter(prefix="/api/v1/justify", tags=["justifications"])

_hybrid: Optional[HybridRetriever] = None

def get_hybrid() -> HybridRetriever:
    global _hybrid
    if _hybrid is None:
        _hybrid = HybridRetriever()
    return _hybrid

class JustifyRequest(BaseModel):
    query: str = Field(..., description="What are we trying to justify? (user question / dimension question)")
    mode: str = Field("hybrid", description="semantic | bm25 | hybrid")
    top_k: int = Field(5, ge=1, le=20)

    company_id: Optional[str] = None
    dimension: Optional[str] = None
    min_confidence: Optional[float] = Field(None, ge=0.0, le=1.0)

class Citation(BaseModel):
    chunk_uid: str
    source_url: Optional[str] = None
    title: Optional[str] = None
    doc_type: Optional[str] = None
    published_at: Optional[str] = None
    chunk_index: Optional[int] = None

class EvidenceItem(BaseModel):
    id: str
    text: str
    score: float
    semantic_score: Optional[float] = None
    bm25_score: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

@router.get("/health")
def health():
    return {"status": "ok", "message": "Justification endpoint ready."}

@router.post("/")
def justify(req: JustifyRequest) -> Dict[str, Any]:
    """
    Phase 1 (No LLM):
    - Retrieve evidence using hybrid retrieval
    - Return a deterministic justification draft + citations

    Later:
    - Plug in LLM generation using these evidence chunks + rubric criteria
    """
    mode = (req.mode or "hybrid").strip().lower()

    hits = get_hybrid().search(
        query=req.query,
        top_k=req.top_k,
        company_id=req.company_id,
        dimension=req.dimension,
        min_confidence=req.min_confidence,
    )

    if mode == "bm25":
        hits = [h for h in hits if h.bm25_score is not None][: req.top_k]
    elif mode == "semantic":
        hits = [h for h in hits if h.semantic_score is not None][: req.top_k]
    else:
        hits = hits[: req.top_k]

    evidence: List[EvidenceItem] = [
        EvidenceItem(
            id=h.id,
            text=h.text,
            score=h.score,
            semantic_score=h.semantic_score,
            bm25_score=h.bm25_score,
            metadata=h.metadata or {},
        )
        for h in hits
    ]

    citations: List[Citation] = []
    for h in hits:
        md = h.metadata or {}
        citations.append(
            Citation(
                chunk_uid=h.id,
                source_url=md.get("source_url"),
                title=md.get("title"),
                doc_type=md.get("doc_type"),
                published_at=md.get("published_at"),
                chunk_index=md.get("chunk_index"),
            )
        )

    # Deterministic "draft justification" (no hallucinations)
    # We only summarize what we retrieved.
    bullet_points = []
    for i, h in enumerate(hits[: min(5, len(hits))], start=1):
        md = h.metadata or {}
        ref = md.get("source_url") or "source"
        bullet_points.append(f"{i}. Evidence from {ref} supports the claim: {h.text[:220].strip()}...")

    justification_draft = (
        f"Justification draft for dimension='{req.dimension}' using mode='{mode}'.\n"
        f"Top evidence signals:\n" + "\n".join(bullet_points)
        if bullet_points
        else "No evidence retrieved for the given filters."
    )

    return {
        "query": req.query,
        "mode": mode,
        "filters": {
            "company_id": req.company_id,
            "dimension": req.dimension,
            "min_confidence": req.min_confidence,
        },
        "justification_draft": justification_draft,
        "citations": [c.model_dump() for c in citations],
        "evidence": [e.model_dump() for e in evidence],
    }