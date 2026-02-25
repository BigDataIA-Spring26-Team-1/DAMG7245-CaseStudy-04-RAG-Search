from __future__ import annotations

from typing import Optional

# Keep this aligned with whatever your scoring engine uses as the canonical dimension keys.
# If you already have an enum for dimensions, import it and return its .value strings.
DIMENSIONS = {
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_maturity",
    "risk_compliance",
}

# Example mapping: signal_category/source_type -> dimension
# You should paste the exact mapping table you used in scoring_engine/evidence_mapper.py (existing in your repo)
SIGNAL_TO_DIMENSION = {
    # SEC signals
    "sec_filing": "leadership_vision",
    # job signals
    "job_posting": "talent_skills",
    # patents/innovation
    "patent": "technology_stack",
    # fallback
}


def map_dimension(source_type: str, signal_category: Optional[str] = None) -> str:
    """
    Map chunk metadata to a single dimension for indexing + filtering.

    Review note:
    - CS4 retrieval wants dimension-aware filtering.
    - Start with a deterministic mapping. We can evolve to weighted multi-dimension later.
    """
    key = (signal_category or "").strip().lower()
    st = (source_type or "").strip().lower()

    if key and key in SIGNAL_TO_DIMENSION:
        return SIGNAL_TO_DIMENSION[key]
    if st in SIGNAL_TO_DIMENSION:
        return SIGNAL_TO_DIMENSION[st]

    # safe fallback: keep chunks searchable even if unmapped
    return "technology_stack"