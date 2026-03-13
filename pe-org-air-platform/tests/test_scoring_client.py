from __future__ import annotations

import json
from datetime import datetime, timezone

from app.services.integration.company_client import CompanyClient
from app.services.integration.scoring_client import ScoringClient


COMPANY_ID = "550e8400-e29b-41d4-a716-446655440001"


def test_company_client_get_company_returns_normalized_payload(fake_sf):
    fake_sf._one = (
        COMPANY_ID,
        "Acme Holdings",
        "ACME",
        None,
        0.35,
        False,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )

    out = CompanyClient().get_company(COMPANY_ID)

    assert out["id"] == COMPANY_ID
    assert out["name"] == "Acme Holdings"
    assert out["ticker"] == "ACME"
    assert out["position_factor"] == 0.35


def test_scoring_client_get_dimension_context_maps_aliases(fake_sf):
    fake_sf._one = (
        COMPANY_ID,
        "assessment-1",
        "run-1",
        68.0,
        3.0,
        0.95,
        60.0,
        74.0,
        70.0,
        "good",
        json.dumps(
            {
                "vr": {
                    "dimension_breakdown": [
                        {
                            "dimension": "leadership_vision",
                            "raw_score": 74.0,
                            "weighted_score": 18.5,
                            "sector_weight": 0.25,
                            "confidence_used": 0.82,
                            "evidence_count": 5,
                        }
                    ]
                }
            }
        ),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    out = ScoringClient().get_dimension_context(COMPANY_ID, "leadership")

    assert out["dimension"] == "leadership"
    assert out["score_dimension"] == "leadership_vision"
    assert out["raw_score"] == 74.0
    assert out["level"] == 4
    assert out["level_name"] == "Good"
    assert out["score_band"] == "good"
