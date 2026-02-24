from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/justify", tags=["justification"])


@router.get("/health")
def health():
    return {"status": "ok", "message": "Justification endpoint scaffolded. Implementation coming next."}