from fastapi import APIRouter, status

from fastapi.responses import JSONResponse

from app.services.redis_cache import ping_redis

from app.services.snowflake import ping_snowflake

from app.services.s3_storage import ping_s3

router = APIRouter(tags=["health"])


@router.get("/health")

def health():

    """

    Lightweight health check.

    Returns 503 if any critical dependency is down.

    Redis is treated as non-critical (cache only); Snowflake and S3 are critical.

    """

    redis_ok, _ = ping_redis()

    sf_ok, _ = ping_snowflake()

    s3_ok, _ = ping_s3()

    # Redis is a cache — app remains functional if Redis is down
    all_ok = sf_ok and s3_ok

    payload = {

        "status": "ok" if all_ok else "degraded"

    }

    if all_ok:

        return payload

    return JSONResponse(

        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,

        content=payload,

    )


@router.get("/health/detailed")

def health_detailed():

    redis_ok, redis_msg = ping_redis()

    sf_ok, sf_msg = ping_snowflake()

    s3_ok, s3_msg = ping_s3()

    # Redis is non-critical (cache); Snowflake + S3 are critical
    all_ok = sf_ok and s3_ok

    deps = {

        "redis": {"ok": redis_ok, "message": redis_msg, "critical": False},

        "snowflake": {"ok": sf_ok, "message": sf_msg, "critical": True},

        "s3": {"ok": s3_ok, "message": s3_msg, "critical": True},

    }

    payload = {

        "status": "ok" if all_ok else "degraded",

        "dependencies": deps,

    }

    if all_ok:

        return payload

    return JSONResponse(

        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,

        content=payload,

    )
