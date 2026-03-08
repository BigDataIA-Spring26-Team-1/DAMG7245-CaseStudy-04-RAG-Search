from __future__ import annotations
 
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional
 
from app.services.retrieval.hybrid import HybridRetriever
 
 
 
 
@dataclass
class CitedEvidence:
    evidence_id: str
    content: str
    source_type: str
    source_url: Optional[str]
    confidence: float
    matched_keywords: List[str]
    relevance_score: float
    title: Optional[str] = None
    published_at: Optional[str] = None
    chunk_index: Optional[int] = None
 
 
@dataclass
class ScoreJustification:
    company_id: str
    dimension: str
    score: float
    level: int
    level_name: str
    confidence_interval: List[float]
    rubric_criteria: str
    rubric_keywords: List[str]
    supporting_evidence: List[CitedEvidence]
    gaps_identified: List[str]
    generated_summary: str
    evidence_strength: str
 
 
class JustificationGenerator:
    """
    CS4 score justification generator.
 
    Current implementation is grounded and deterministic:
    - retrieves evidence with HybridRetriever
    - matches evidence to rubric-like keywords
    - generates IC-style explanation only from retrieved evidence
    - identifies gaps to next level using heuristic rubric expectations
 
    This avoids hallucination while still delivering the required CS4 structure.
    """
 
    DIMENSION_KEYWORDS: Dict[str, List[str]] = {
        "data_infrastructure": [
            "data lake", "warehouse", "etl", "pipeline", "accessibility",
            "data quality", "source systems", "api", "integration", "clean data",
        ],
        "ai_governance": [
            "governance", "audit trail", "explainability", "bias", "ethics",
            "risk", "compliance", "model risk", "policy", "controls",
        ],
        "technology_stack": [
            "aws", "azure", "gcp", "cloud", "mlops", "deployment",
            "model registry", "experiment tracking", "streaming", "api",
        ],
        "talent": [
            "data engineer", "ml engineer", "analyst", "hiring", "retention",
            "ai talent", "flight risk", "team", "staffing", "capability",
        ],
        "leadership": [
            "executive", "budget", "sponsor", "vision", "strategy",
            "leadership", "champion", "roadmap", "board", "ownership",
        ],
        "use_case_portfolio": [
            "production ai", "use case", "roi", "revenue impact", "pilot",
            "automation", "deployment", "business value", "pipeline", "initiative",
        ],
        "culture": [
            "innovation", "experimentation", "adoption", "change management",
            "collaboration", "ai literacy", "culture", "training", "risk appetite",
        ],
    }
 
    LEVEL_NAMES: Dict[int, str] = {
        1: "Nascent",
        2: "Developing",
        3: "Adequate",
        4: "Good",
        5: "Excellent",
    }
 
    def __init__(self) -> None:
        self.retriever = HybridRetriever()
 
    def generate(
        self,
        company_id: str,
        dimension: str,
        question: Optional[str] = None,
        top_k: int = 5,
        min_confidence: Optional[float] = None,
    ) -> Dict[str, Any]:
        dimension = self._normalize_dimension(dimension)
        if not company_id:
            raise ValueError("company_id is required")
        if dimension not in self.DIMENSION_KEYWORDS:
            raise ValueError(f"Unsupported dimension: {dimension}")
 
        rubric_keywords = self._get_rubric_keywords(dimension)
        query = self._build_query(dimension=dimension, question=question, rubric_keywords=rubric_keywords)
 
        hits = self.retriever.search(
            query=query,
            top_k=max(top_k, 8),
            company_id=company_id,
            dimension=dimension,
            min_confidence=min_confidence,
        ) or []
 
        cited = self._match_to_rubric(hits=hits, rubric_keywords=rubric_keywords, top_k=top_k)
        score = self._estimate_score(cited=cited, dimension=dimension)
        level = self._score_to_level(score)
        level_name = self.LEVEL_NAMES[level]
        confidence_interval = [max(0.0, round(score - 8, 2)), min(100.0, round(score + 8, 2))]
        rubric_criteria = self._build_rubric_criteria(dimension=dimension, level=level)
        gaps = self._identify_gaps(dimension=dimension, level=level, evidence=cited)
        strength = self._assess_strength(cited)
        summary = self._build_summary(
            company_id=company_id,
            dimension=dimension,
            score=score,
            level=level,
            level_name=level_name,
            rubric_criteria=rubric_criteria,
            cited=cited,
            gaps=gaps,
            evidence_strength=strength,
        )
 
        result = ScoreJustification(
            company_id=company_id,
            dimension=dimension,
            score=score,
            level=level,
            level_name=level_name,
            confidence_interval=confidence_interval,
            rubric_criteria=rubric_criteria,
            rubric_keywords=rubric_keywords,
            supporting_evidence=cited,
            gaps_identified=gaps,
            generated_summary=summary,
            evidence_strength=strength,
        )
 
        payload = asdict(result)
        payload["query_used"] = query
        payload["evidence_count"] = len(cited)
        payload["generation_mode"] = "deterministic_grounded"
        return payload
 
    def _normalize_dimension(self, dimension: str) -> str:
        return (dimension or "").strip().lower().replace(" ", "_")
 
    def _get_rubric_keywords(self, dimension: str) -> List[str]:
        return self.DIMENSION_KEYWORDS.get(dimension, []).copy()
 
    def _build_query(self, dimension: str, question: Optional[str], rubric_keywords: List[str]) -> str:
        dimension_text = dimension.replace("_", " ")
        keywords = " ".join(rubric_keywords[:5])
        if question and question.strip():
            return f"{question.strip()} {dimension_text} {keywords}".strip()
        return f"{dimension_text} {keywords}".strip()
 
    def _match_to_rubric(
        self,
        hits: List[Any],
        rubric_keywords: List[str],
        top_k: int,
    ) -> List[CitedEvidence]:
        cited: List[CitedEvidence] = []
 
        for hit in hits:
            evidence_id = str(getattr(hit, "id", "") or "").strip()
            text = (getattr(hit, "text", "") or "").strip()
            if not evidence_id or not text:
                continue
        
            metadata = getattr(hit, "metadata", {}) or {}
            matched_keywords = self._keyword_matches(text=text, keywords=rubric_keywords)
            relevance_score = float(getattr(hit, "score", 0.0) or 0.0)
            confidence = self._coerce_confidence(metadata.get("confidence"))
            source_type = metadata.get("source_type") or metadata.get("doc_type") or "unknown"
 
            if matched_keywords or relevance_score >= 0.45:
                cited.append(
                    CitedEvidence(
                        evidence_id=str(getattr(hit, "id", "")),
                        content=text[:500],
                        source_type=str(metadata.get("source_type", metadata.get("doc_type", "unknown"))),
                        source_url=metadata.get("source_url"),
                        confidence=confidence,
                        matched_keywords=matched_keywords,
                        relevance_score=round(relevance_score, 4),
                        title=metadata.get("title"),
                        published_at=metadata.get("published_at"),
                        chunk_index=metadata.get("chunk_index"),
                    )
                )
 
        cited.sort(
            key=lambda e: (len(e.matched_keywords), e.confidence, e.relevance_score),
            reverse=True,
        )
        return cited[:top_k]
 
    def _keyword_matches(self, text: str, keywords: List[str]) -> List[str]:
        lowered = (text or "").lower()
        matches: List[str] = []
        for kw in keywords:
            if kw.lower() in lowered:
                matches.append(kw)
        return matches
 
    def _coerce_confidence(self, value: Any) -> float:
        try:
            val = float(value)
        except (TypeError, ValueError):
            val = 0.5
        return max(0.0, min(1.0, val))
 
    def _estimate_score(self, cited: List[CitedEvidence], dimension: str) -> float:
        if not cited:
            return 10.0
 
        avg_relevance = sum(e.relevance_score for e in cited) / len(cited)
        avg_conf = sum(e.confidence for e in cited) / len(cited)
        avg_keyword_matches = sum(len(e.matched_keywords) for e in cited) / len(cited)
        coverage_ratio = min(1.0, avg_keyword_matches / max(1.0, len(self.DIMENSION_KEYWORDS[dimension]) / 3))
 
        # Heuristic but bounded and deterministic
        score = (
            20
            + (avg_relevance * 35)
            + (avg_conf * 20)
            + (coverage_ratio * 25)
        )
        return round(max(0.0, min(100.0, score)), 2)
 
    def _score_to_level(self, score: float) -> int:
        if score >= 80:
            return 5
        if score >= 60:
            return 4
        if score >= 40:
            return 3
        if score >= 20:
            return 2
        return 1
 
    def _build_rubric_criteria(self, dimension: str, level: int) -> str:
        dimension_text = dimension.replace("_", " ")
        base_keywords = self.DIMENSION_KEYWORDS.get(dimension, [])
 
        if level == 5:
            qualifier = "clear, repeated, high-confidence evidence of mature and scalable capability"
        elif level == 4:
            qualifier = "multiple credible signals showing solid capability with some gaps remaining"
        elif level == 3:
            qualifier = "mixed evidence indicating partial adoption and developing maturity"
        elif level == 2:
            qualifier = "limited evidence, early-stage capability, and material gaps"
        else:
            qualifier = "minimal or no reliable evidence of operational capability"
 
        keywords_preview = ", ".join(base_keywords[:5])
        return f"{dimension_text.title()} at Level {level} requires {qualifier}. Typical signals include: {keywords_preview}."
 
    def _identify_gaps(self, dimension: str, level: int, evidence: List[CitedEvidence]) -> List[str]:
        if level >= 5:
            return []
 
        present = set()
        for e in evidence:
            for kw in e.matched_keywords:
                present.add(kw.lower())
 
        expected = self.DIMENSION_KEYWORDS.get(dimension, [])
        missing = [kw for kw in expected if kw.lower() not in present]
 
        gaps = [f"No strong evidence of '{kw}' for next-level readiness" for kw in missing[:5]]
        return gaps
 
    def _assess_strength(self, evidence: List[CitedEvidence]) -> str:
        if not evidence:
            return "weak"
 
        avg_conf = sum(e.confidence for e in evidence) / len(evidence)
        avg_matches = sum(len(e.matched_keywords) for e in evidence) / len(evidence)
        avg_relevance = sum(e.relevance_score for e in evidence) / len(evidence)
 
        if avg_conf >= 0.75 and avg_matches >= 2 and avg_relevance >= 0.70:
            return "strong"
        if avg_conf >= 0.55 and avg_matches >= 1 and avg_relevance >= 0.50:
            return "moderate"
        return "weak"
 
    def _build_summary(
        self,
        company_id: str,
        dimension: str,
        score: float,
        level: int,
        level_name: str,
        rubric_criteria: str,
        cited: List[CitedEvidence],
        gaps: List[str],
        evidence_strength: str,
    ) -> str:
        dimension_text = dimension.replace("_", " ").title()
 
        if not cited:
            return (
                f"{company_id} is assessed at {score}/100 for {dimension_text} "
                f"(Level {level} - {level_name}). No strong supporting evidence was retrieved, "
                f"so this justification should be treated as weak and provisional. "
                f"The current rubric expectation is: {rubric_criteria}"
            )
 
        evidence_lines: List[str] = []
        for e in cited[:3]:
            source_label = e.title or e.source_type or "source"
            snippet = (e.content or "").replace("\n", " ").strip()
            snippet = snippet[:180] + ("..." if len(snippet) > 180 else "")
            evidence_lines.append(
                f"{source_label}: {snippet}"
            )
 
        gaps_text = "; ".join(gaps[:3]) if gaps else "No immediate next-level gaps identified from retrieved evidence."
 
        return (
            f"{company_id} is assessed at {score}/100 for {dimension_text} "
            f"(Level {level} - {level_name}). The evidence base is {evidence_strength}. "
            f"The current level is supported by retrieved signals aligned to the rubric, including: "
            f"{' | '.join(evidence_lines)}. "
            f"Rubric interpretation: {rubric_criteria} "
            f"Key gaps to reach the next level: {gaps_text}"
        )
 