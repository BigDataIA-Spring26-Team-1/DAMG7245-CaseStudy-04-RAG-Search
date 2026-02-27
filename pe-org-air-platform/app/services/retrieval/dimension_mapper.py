from __future__ import annotations
 
from typing import Dict, Optional
 
from app.scoring_engine.evidence_mapper import DIMENSION_KEYWORDS
from app.scoring_engine.mapping_config import DIMENSIONS as SCORING_DIMENSIONS
from app.scoring_engine.mapping_config import SOURCE_PROFILES
 
 
DIMENSIONS = set(SCORING_DIMENSIONS)
DEFAULT_DIMENSION = "technology_stack"
 
 
SOURCE_ALIASES = {
    "technology_hiring": "technology_hiring",
    "jobs": "technology_hiring",
    "job_posting": "technology_hiring",
    "job": "technology_hiring",
    "hiring": "technology_hiring",
    "innovation_activity": "innovation_activity",
    "patents": "innovation_activity",
    "patent": "innovation_activity",
    "digital_presence": "digital_presence",
    "tech": "digital_presence",
    "leadership_signals": "leadership_signals",
    "news": "leadership_signals",
    "sec_item_1": "sec_item_1",
    "sec_item_1a": "sec_item_1a",
    "sec_item_7": "sec_item_7",
    "glassdoor_reviews": "glassdoor_reviews",
    "glassdoor": "glassdoor_reviews",
    "board_composition": "board_composition",
    "board": "board_composition",
    "sec_filing": "sec_filing",
    "10k": "sec_filing",
    "10_q": "sec_filing",
}
 
 
def _normalize(value: str) -> str:
    return (value or "").strip().lower().replace("-", "_").replace(" ", "_")
 
 
def _canonical_signal_key(raw: str) -> Optional[str]:
    key = _normalize(raw)
    if not key:
        return None
 
    if "item_1a" in key or "item1a" in key:
        return "sec_item_1a"
    if "item_7" in key or "item7" in key:
        return "sec_item_7"
    if "item_1" in key or "item1" in key:
        return "sec_item_1"
 
    return SOURCE_ALIASES.get(key)
 
 
def _primary_dimension_for_signal(signal_key: str) -> Optional[str]:
    prof = SOURCE_PROFILES.get(signal_key)
    if not prof:
        return None
 
    weights: Dict[str, float] = {
        dim: float(weight)
        for dim, weight in prof.dim_weights.items()
        if dim in DIMENSIONS
    }
    if not weights:
        return None
    return max(weights.items(), key=lambda kv: kv[1])[0]
 
 
def _keyword_dimension(text: str) -> Optional[str]:
    normalized = (text or "").lower()
    if not normalized:
        return None
 
    best_dim: Optional[str] = None
    best_hits = 0
    for dim, keywords in DIMENSION_KEYWORDS.items():
        if dim not in DIMENSIONS:
            continue
        hits = sum(1 for kw in keywords if kw in normalized)
        if hits > best_hits:
            best_dim = dim
            best_hits = hits
    return best_dim if best_hits > 0 else None
 
 
def map_dimension(source_type: str, signal_category: Optional[str] = None, chunk_text: Optional[str] = None) -> str:
    signal_key = _canonical_signal_key(signal_category or "") or _canonical_signal_key(source_type)
 
    if signal_key in {"sec_item_1", "sec_item_1a", "sec_item_7"}:
        mapped = _primary_dimension_for_signal(signal_key)
        if mapped:
            return mapped
 
    if signal_key and signal_key != "sec_filing":
        mapped = _primary_dimension_for_signal(signal_key)
        if mapped:
            return mapped
 
    inferred = _keyword_dimension(chunk_text or "")
    if inferred:
        return inferred
 
    if signal_key == "sec_filing":
        return "leadership_vision"
 
    return DEFAULT_DIMENSION