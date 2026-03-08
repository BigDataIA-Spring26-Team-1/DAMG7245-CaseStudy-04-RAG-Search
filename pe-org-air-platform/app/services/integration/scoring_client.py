from __future__ import annotations
 
import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
 
from app.config import settings
from app.services.redis_cache import cache_get_json, cache_set_json
from app.services.snowflake import get_snowflake_connection
 
 
@dataclass(frozen=True)
class ScoringRecord:
    company_id: str
    assessment_id: Optional[str]
    scoring_run_id: Optional[str]
    vr_score: float
    synergy_bonus: float
    talent_penalty: float
    sem_lower: Optional[float]
    sem_upper: Optional[float]
    composite_score: float
    score_band: str
    breakdown: Dict[str, Any]
    scored_at: Any
 
 
class ScoringClient:
    """
    Read-only scoring integration client for internal service usage.
 
    Mirrors the latest-score lookup logic already used by routers/scoring.py.
    """
 
    def _parse_breakdown(self, row_variant: Any) -> Dict[str, Any]:
        if row_variant is None:
            return {}
        if isinstance(row_variant, dict):
            return row_variant
        if isinstance(row_variant, list):
            return {"_": row_variant}
        try:
            return json.loads(row_variant)
        except Exception:
            return {}
 
    def get_latest_scores(self, company_id: str) -> Dict[str, Any]:
        if not company_id or not company_id.strip():
            raise ValueError("company_id is required")
 
        cache_key = f"scoring:results:company:{company_id}"
        cached = cache_get_json(cache_key)
        if cached is not None:
            return cached if isinstance(cached, dict) else dict(cached)
 
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                  company_id,
                  assessment_id,
                  scoring_run_id,
                  vr_score,
                  synergy_bonus,
                  talent_penalty,
                  sem_lower,
                  sem_upper,
                  composite_score,
                  score_band,
                  dimension_breakdown_json,
                  scored_at
                FROM org_air_scores
                WHERE company_id = %s
                ORDER BY scored_at DESC, created_at DESC
                LIMIT 1
                """,
                (company_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError("No scores found for company")
 
            record = ScoringRecord(
                company_id=str(row[0]),
                assessment_id=str(row[1]) if row[1] else None,
                scoring_run_id=str(row[2]) if row[2] else None,
                vr_score=float(row[3] or 0),
                synergy_bonus=float(row[4] or 0),
                talent_penalty=float(row[5] or 0),
                sem_lower=float(row[6]) if row[6] is not None else None,
                sem_upper=float(row[7]) if row[7] is not None else None,
                composite_score=float(row[8] or 0),
                score_band=str(row[9] or ""),
                breakdown=self._parse_breakdown(row[10]),
                scored_at=row[11],
            )
 
            payload = asdict(record)
            payload["overall_score"] = payload["composite_score"]
 
            cache_set_json(
                cache_key,
                payload,
                settings.redis_ttl_seconds,
            )
            return payload
        finally:
            cur.close()
            conn.close()
 
    def list_latest_scores(self, limit: int = 50) -> List[Dict[str, Any]]:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                WITH latest AS (
                  SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY scored_at DESC, created_at DESC) AS rn
                  FROM org_air_scores
                )
                SELECT
                  company_id,
                  assessment_id,
                  scoring_run_id,
                  vr_score,
                  synergy_bonus,
                  talent_penalty,
                  sem_lower,
                  sem_upper,
                  composite_score,
                  score_band,
                  dimension_breakdown_json,
                  scored_at
                FROM latest
                WHERE rn = 1
                ORDER BY composite_score DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
 
            out: List[Dict[str, Any]] = []
            for row in rows:
                record = ScoringRecord(
                    company_id=str(row[0]),
                    assessment_id=str(row[1]) if row[1] else None,
                    scoring_run_id=str(row[2]) if row[2] else None,
                    vr_score=float(row[3] or 0),
                    synergy_bonus=float(row[4] or 0),
                    talent_penalty=float(row[5] or 0),
                    sem_lower=float(row[6]) if row[6] is not None else None,
                    sem_upper=float(row[7]) if row[7] is not None else None,
                    composite_score=float(row[8] or 0),
                    score_band=str(row[9] or ""),
                    breakdown=self._parse_breakdown(row[10]),
                    scored_at=row[11],
                )
                payload = asdict(record)
                payload["overall_score"] = payload["composite_score"]
                out.append(payload)
 
            return out
        finally:
            cur.close()
            conn.close()