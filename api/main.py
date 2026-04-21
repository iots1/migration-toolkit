"""
FastAPI application entry point.

Registers all routers, exception handlers, CORS middleware, and initializes
database schema on startup.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.base.auth import verify_api_key
from api.base.exceptions import (
    InvalidParameterException,
    ValidationException,
    BusinessRuleValidationException,
    invalid_parameter_handler,
    validation_handler,
    http_exception_handler,
    business_rule_validation_handler,
    unhandled_exception_handler,
)

from api.datasources.router import get_datasources_router
from api.configs.router import get_configs_router
from api.pipelines.router import get_pipelines_router
from api.pipeline_runs.router import get_pipeline_runs_router
from api.jobs.router import get_jobs_router
from api.data_explorers.router import get_data_explorers_router
from api.transformers.router import get_transformers_router
from api.validators.router import get_validators_router
from api.socket_manager import sio, socket_asgi, set_event_loop

from repositories.base import init_db


class UnicodeJSONResponse(JSONResponse):
    """JSONResponse that preserves non-ASCII characters (Thai, CJK, etc.).

    FastAPI's default uses json.dumps(ensure_ascii=True) which escapes
    Unicode characters to \\uXXXX sequences. On deploy servers this can
    cause Thai text to appear as ASCII codes in the response body.

    Setting ensure_ascii=False outputs raw UTF-8, which is both more
    compact and avoids any intermediate encoding/decoding issues.
    """

    def render(self, content) -> bytes:
        import json

        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        ).encode("utf-8")


# Create FastAPI app
app = FastAPI(
    title="HIS Analyzer API",
    description="REST API for hospital information system data migration toolkit",
    version="1.0.0",
    dependencies=[Depends(verify_api_key)],
    redirect_slashes=False,
    default_response_class=UnicodeJSONResponse,
)

# Register exception handlers
app.add_exception_handler(InvalidParameterException, invalid_parameter_handler)
app.add_exception_handler(ValidationException, validation_handler)
app.add_exception_handler(BusinessRuleValidationException, business_rule_validation_handler)
app.add_exception_handler(RequestValidationError, validation_handler)
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)

# CORS middleware for Svelte dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(get_datasources_router())
app.include_router(get_configs_router())
app.include_router(get_pipelines_router())
app.include_router(get_pipeline_runs_router())
app.include_router(get_jobs_router())
app.include_router(get_data_explorers_router())
app.include_router(get_transformers_router())
app.include_router(get_validators_router())

# Wrap FastAPI with Socket.IO so path /ws/socket.io/ is handled correctly.
# socket_asgi uses socketio_path="ws/socket.io", so it intercepts requests
# whose PATH_INFO starts with /ws/socket.io/ and forwards everything else
# to the FastAPI app.
socket_asgi.other_asgi_app = app


# Health check endpoint
@app.get("/health")
def health():
    return {"status": "ok"}


# Initialize database on startup and capture event loop for socket.io thread emits
@app.on_event("startup")
async def startup():
    import asyncio

    set_event_loop(asyncio.get_event_loop())
    try:
        init_db()
    except RuntimeError as e:
        # Database URL not configured, skip init
        print(f"Database initialization skipped: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(socket_asgi, host="0.0.0.0", port=8000)
