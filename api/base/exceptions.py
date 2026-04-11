"""
Custom exception classes and handlers for the FastAPI application.

Mirrors NestJS AllExceptionsFilter pattern with structured error responses.
"""
from __future__ import annotations

from datetime import datetime
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


class InvalidParameterException(HTTPException):
    """Query parameter format error (code 400002)."""

    def __init__(self, errors: list[dict]):
        super().__init__(status_code=400)
        self.validation_errors = errors  # [{"field": ..., "message": ...}]


class ValidationException(HTTPException):
    """Request body validation error (code 400001)."""

    def __init__(self, errors: list[dict]):
        super().__init__(status_code=422)
        self.validation_errors = errors  # [{"field": ..., "messages": [...]}]


# Exception handlers
async def invalid_parameter_handler(request: Request, exc: InvalidParameterException):
    """Handle InvalidParameterException (code 400002)."""
    return JSONResponse(
        status_code=400,
        content={
            "status": {"code": 400002, "message": "Invalid Parameters"},
            "errors": [
                {
                    "code": "INVALID_PARAMETER_FORMAT",
                    "title": "Invalid Parameter Format",
                    "detail": e["message"],
                    "source": {"parameter": e["field"]},
                }
                for e in exc.validation_errors
            ],
            "meta": {"timestamp": datetime.utcnow().isoformat()},
        },
    )


async def validation_handler(request: Request, exc: ValidationException):
    """Handle ValidationException (code 400001)."""
    errors = []
    for error in exc.validation_errors:
        for message in error.get("messages", []):
            errors.append(
                {
                    "code": "VALIDATION_ERROR",
                    "title": "Invalid Input",
                    "detail": message,
                    "source": {"pointer": f'/data/attributes/{error["field"]}'},
                }
            )

    return JSONResponse(
        status_code=422,
        content={
            "status": {"code": 400001, "message": "Validation Failed"},
            "errors": errors,
            "meta": {"timestamp": datetime.utcnow().isoformat()},
        },
    )


async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTPException (404, 409, etc.)."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": {"code": exc.status_code, "message": exc.detail},
            "errors": [
                {
                    "code": exc.status_code,
                    "title": "An error occurred",
                    "detail": exc.detail,
                }
            ],
            "meta": {"timestamp": datetime.utcnow().isoformat()},
        },
    )


async def unhandled_exception_handler(request: Request, exc: Exception):
    """Handle unhandled exceptions (500)."""
    return JSONResponse(
        status_code=500,
        content={
            "status": {"code": 500, "message": "Internal Server Error"},
            "errors": [
                {
                    "code": "INTERNAL_SERVER_ERROR",
                    "title": "An unexpected error occurred",
                    "detail": "Please try again later or contact support.",
                }
            ],
            "meta": {"timestamp": datetime.utcnow().isoformat()},
        },
    )
