"""
FastAPI application entry point.

Registers all routers, exception handlers, CORS middleware, and initializes
database schema on startup.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError

from api.base.auth import verify_api_key
from api.base.exceptions import (
    InvalidParameterException,
    ValidationException,
    invalid_parameter_handler,
    validation_handler,
    http_exception_handler,
    unhandled_exception_handler,
)

from api.datasources.router import get_datasources_router
from api.configs.router import get_configs_router
from api.pipelines.router import get_pipelines_router
from api.pipeline_runs.router import get_pipeline_runs_router

from repositories.base import init_db

# Create FastAPI app
app = FastAPI(
    title="HIS Analyzer API",
    description="REST API for hospital information system data migration toolkit",
    version="1.0.0",
    dependencies=[Depends(verify_api_key)],
    redirect_slashes=False,
)

# Register exception handlers
app.add_exception_handler(InvalidParameterException, invalid_parameter_handler)
app.add_exception_handler(ValidationException, validation_handler)
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


# Health check endpoint
@app.get("/health")
def health():
    return {"status": "ok"}


# Initialize database on startup
@app.on_event("startup")
def startup():
    try:
        init_db()
    except RuntimeError as e:
        # Database URL not configured, skip init
        print(f"Database initialization skipped: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
