from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.services.integration.company_client import CompanyClient
from app.services.justification.generator import JustificationGenerator


@dataclass
class AnalystNoteRecord:
    company_id: str
    company_name: Optional[str]
    dimension: str
    note_title: str
    note_summary: str
    evidence_snapshot: List[Dict[str, Any]]
    key_gaps: List[str]
    confidence_label: str
    created_at: str
    generated_by: str


class AnalystNotesCollector:
    """
    Builds structured analyst notes from justification outputs.

    Current implementation is deterministic and grounded:
    - fetches company metadata
    - runs dimension-level justifications
    - converts them into note-friendly summaries
    - packages a normalized analyst-note record
    """

    SUPPORTED_DIMENSIONS: List[str] = [
        "leadership",
        "talent",
        "culture",
        "ai_governance",
        "data_infrastructure",
        "technology_stack",
        "use_case_portfolio",
    ]

    def __init__(self) -> None:
        self.company_client = CompanyClient()
        self.generator = JustificationGenerator()

    def collect_note(
        self,
        company_id: str,
        dimension: str,
        note_title: Optional[str] = None,
        question: Optional[str] = None,
        top_k: int = 5,
        min_confidence: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not company_id or not company_id.strip():
            raise ValueError("company_id is required")

        normalized_dimension = self._normalize_dimension(dimension)
        if normalized_dimension not in self.SUPPORTED_DIMENSIONS:
            raise ValueError(f"Unsupported dimension: {normalized_dimension}")

        company = self.company_client.get_company(company_id)
        justification = self.generator.generate(
            company_id=company_id,
            dimension=normalized_dimension,
            question=question
            or f"What should an analyst note emphasize about {normalized_dimension.replace('_', ' ')}?",
            top_k=top_k,
            min_confidence=min_confidence,
        )

        note = AnalystNoteRecord(
            company_id=company_id,
            company_name=company.get("name"),
            dimension=normalized_dimension,
            note_title=note_title or self._default_title(company, normalized_dimension),
            note_summary=self._build_note_summary(company, justification),
            evidence_snapshot=self._trim_evidence(justification.get("supporting_evidence", [])),
            key_gaps=list(justification.get("gaps_identified", []))[:5],
            confidence_label=self._confidence_label(
                justification.get("evidence_strength"),
                float(justification.get("score", 0.0) or 0.0),
            ),
            created_at=datetime.now(timezone.utc).isoformat(),
            generated_by="deterministic_analyst_notes_collector",
        )

        return asdict(note)

    def collect_notes_for_dimensions(
        self,
        company_id: str,
        dimensions: Optional[List[str]] = None,
        top_k: int = 5,
        min_confidence: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        selected = dimensions or self.SUPPORTED_DIMENSIONS
        out: List[Dict[str, Any]] = []

        for dimension in selected:
            out.append(
                self.collect_note(
                    company_id=company_id,
                    dimension=dimension,
                    top_k=top_k,
                    min_confidence=min_confidence,
                )
            )

        return out

    def _normalize_dimension(self, dimension: str) -> str:
        return (dimension or "").strip().lower().replace(" ", "_")

    def _default_title(self, company: Dict[str, Any], dimension: str) -> str:
        company_name = company.get("name", "Company")
        return f"{company_name} - {dimension.replace('_', ' ').title()} Analyst Note"

    def _trim_evidence(self, evidence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        trimmed: List[Dict[str, Any]] = []
        for item in evidence[:3]:
            trimmed.append(
                {
                    "evidence_id": item.get("evidence_id"),
                    "title": item.get("title"),
                    "source_type": item.get("source_type"),
                    "source_url": item.get("source_url"),
                    "confidence": item.get("confidence"),
                    "relevance_score": item.get("relevance_score"),
                    "matched_keywords": item.get("matched_keywords", []),
                    "content": (item.get("content") or "")[:220],
                }
            )
        return trimmed

    def _build_note_summary(
        self,
        company: Dict[str, Any],
        justification: Dict[str, Any],
    ) -> str:
        company_name = company.get("name", justification.get("company_id", "Company"))
        dimension = str(justification.get("dimension", "")).replace("_", " ").title()
        score = justification.get("score")
        level = justification.get("level_name")
        evidence_strength = justification.get("evidence_strength", "unknown")
        generated_summary = justification.get("generated_summary", "").strip()

        return (
            f"{company_name} is currently assessed at {score}/100 for {dimension} "
            f"(Level: {level}). The evidence base is {evidence_strength}. "
            f"{generated_summary}"
        ).strip()

    def _confidence_label(self, evidence_strength: Any, score: float) -> str:
        evidence_strength = str(evidence_strength or "").lower()

        if evidence_strength == "strong" and score >= 75:
            return "high"
        if evidence_strength in {"strong", "moderate"} and score >= 50:
            return "medium"
        return "low"