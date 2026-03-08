from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class HyDEResult:
    original_query: str
    expanded_query: str
    hypothetical_document: str
    dimension: Optional[str]
    mode: str


class HyDEGenerator:
    """
    HyDE = Hypothetical Document Embeddings.

    This implementation is deterministic and grounded:
    - it does not call an LLM yet
    - it expands the query into a richer hypothetical retrieval document
    - it is safe to use in local/dev environments without API keys

    Later, this class can be extended to use LiteLLM/OpenAI for
    richer hypothetical document generation.
    """

    DIMENSION_HINTS: Dict[str, List[str]] = {
        "leadership": [
            "executive sponsorship",
            "leadership alignment",
            "strategy ownership",
            "board visibility",
            "budget commitment",
        ],
        "talent": [
            "AI talent",
            "data engineering team",
            "hiring capability",
            "retention risk",
            "ML expertise",
        ],
        "culture": [
            "innovation culture",
            "change management",
            "AI literacy",
            "cross-functional collaboration",
            "experimentation",
        ],
        "ai_governance": [
            "governance controls",
            "model risk management",
            "compliance policy",
            "auditability",
            "explainability",
        ],
        "data_infrastructure": [
            "data pipeline",
            "ETL systems",
            "data warehouse",
            "data quality",
            "source system integration",
        ],
        "technology_stack": [
            "cloud platform",
            "MLOps tooling",
            "deployment stack",
            "model registry",
            "API integration",
        ],
        "use_case_portfolio": [
            "production AI use cases",
            "ROI",
            "business impact",
            "automation initiatives",
            "deployment maturity",
        ],
    }

    def generate(
        self,
        query: str,
        dimension: Optional[str] = None,
        company_id: Optional[str] = None,
    ) -> HyDEResult:
        normalized_query = (query or "").strip()
        if not normalized_query:
            raise ValueError("query is required")

        normalized_dimension = self._normalize_dimension(dimension)
        dimension_hints = self.DIMENSION_HINTS.get(normalized_dimension, [])

        hypothetical_document = self._build_hypothetical_document(
            query=normalized_query,
            dimension=normalized_dimension,
            company_id=company_id,
            dimension_hints=dimension_hints,
        )
        expanded_query = self._build_expanded_query(
            query=normalized_query,
            dimension=normalized_dimension,
            company_id=company_id,
            dimension_hints=dimension_hints,
            hypothetical_document=hypothetical_document,
        )

        return HyDEResult(
            original_query=normalized_query,
            expanded_query=expanded_query,
            hypothetical_document=hypothetical_document,
            dimension=normalized_dimension,
            mode="deterministic_hyde",
        )

    def _normalize_dimension(self, dimension: Optional[str]) -> Optional[str]:
        if not dimension:
            return None
        return dimension.strip().lower().replace(" ", "_")

    def _build_hypothetical_document(
        self,
        query: str,
        dimension: Optional[str],
        company_id: Optional[str],
        dimension_hints: List[str],
    ) -> str:
        dimension_text = dimension.replace("_", " ") if dimension else "general capability"
        hints_text = ", ".join(dimension_hints[:5]) if dimension_hints else "evidence, signals, operational details"

        company_text = f"for company {company_id}" if company_id else "for the target company"

        return (
            f"This document describes strong evidence {company_text} related to {dimension_text}. "
            f"It discusses {hints_text}. "
            f"It answers the retrieval need behind the query: {query}. "
            f"It includes concrete signals, operational examples, and relevant supporting details."
        )

    def _build_expanded_query(
        self,
        query: str,
        dimension: Optional[str],
        company_id: Optional[str],
        dimension_hints: List[str],
        hypothetical_document: str,
    ) -> str:
        parts: List[str] = [query]

        if dimension:
            parts.append(dimension.replace("_", " "))

        if company_id:
            parts.append(company_id)

        if dimension_hints:
            parts.append(" ".join(dimension_hints))

        parts.append(hypothetical_document)

        return " ".join(part.strip() for part in parts if part and part.strip())