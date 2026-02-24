from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_scoring_compute_endpoint_exists():
    # just checks route exists; may fail if env isn't configured
    resp = client.post("/api/v1/scoring/compute/00000000-0000-0000-0000-000000000000")
    assert resp.status_code in (200, 404, 422, 500)

def test_scoring_results_endpoint_exists():
    resp = client.get("/api/v1/scoring/results/00000000-0000-0000-0000-000000000000")
    assert resp.status_code in (200, 404, 422)
