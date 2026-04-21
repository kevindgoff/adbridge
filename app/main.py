import os
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader
from app.database import init_db
from app.config import get_enabled_apis

load_dotenv()

_API_KEY = os.environ.get("API_KEY")

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def _verify_api_key(key: str = Security(_api_key_header)):
    if _API_KEY and key != _API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# Only enforce the dependency when an API_KEY is configured
_global_deps = [Depends(_verify_api_key)] if _API_KEY else []

tags_metadata = [
    {"name": "/basis", "description": "Basis Technologies API mock endpoints"},
    {"name": "/dv360", "description": "Google Display & Video 360 API mock endpoints"},
    {"name": "/triton", "description": "Triton Digital Metrics API mock endpoints"},
    {"name": "/triton-booking", "description": "Triton Digital Booking (TAP) API mock endpoints"},
    {"name": "/hivestack", "description": "Hivestack OpenRTB 2.5 DOOH API mock endpoints"},
]

app = FastAPI(
    title="AdBridge - Local Ad Platform API Mock",
    description="Mock API layer for local integration testing against Basis, DV360, Triton Metrics, Triton Booking, and Hivestack DOOH platform APIs.",
    version="0.5.0",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"docExpansion": "none", "persistAuthorization": True},
    dependencies=_global_deps,
)

_enabled = get_enabled_apis()

if _enabled.get("basis"):
    from app.routes.basis import router as basis_router
    app.include_router(basis_router, tags=["/basis"])

if _enabled.get("dv360"):
    from app.routes.dv360 import router as dv360_router
    app.include_router(dv360_router, tags=["/dv360"])

if _enabled.get("triton"):
    from app.routes.triton import router as triton_router
    from app.routes.triton_booking import router as triton_booking_router
    app.include_router(triton_router, tags=["/triton"])
    app.include_router(triton_booking_router, tags=["/triton-booking"])

if _enabled.get("hivestack"):
    from app.routes.hivestack import router as hivestack_router
    app.include_router(hivestack_router, tags=["/hivestack"])


@app.on_event("startup")
def startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok"}
