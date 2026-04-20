import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app.database import init_db
from app.config import get_enabled_apis

_API_KEY = os.environ.get("API_KEY")

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
    swagger_ui_parameters={"docExpansion": "none"},
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


@app.middleware("http")
async def require_api_key(request: Request, call_next):
    if _API_KEY and request.headers.get("X-API-Key") != _API_KEY:
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


@app.on_event("startup")
def startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok"}
