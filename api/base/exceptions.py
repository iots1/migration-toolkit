"""
Custom exception classes and handlers for the FastAPI application.

All exception responses follow a consistent pattern:
{
    "status": {"code": <int>, "message": "<short summary>"},
    "errors": [{"code": "<CODE>", "title": "<title>", "detail": "<human-readable>", "source": {...}}],
    "meta": {"timestamp": "..."}
}
"""

from __future__ import annotations

from datetime import datetime
from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class InvalidParameterException(HTTPException):
    def __init__(self, errors: list[dict]):
        super().__init__(status_code=400)
        self.validation_errors = errors


class ValidationException(HTTPException):
    def __init__(self, errors: list[dict]):
        super().__init__(status_code=422)
        self.validation_errors = errors


class BusinessRuleValidationException(HTTPException):
    """Exception raised when a business rule validation fails."""

    def __init__(self, message: str):
        super().__init__(status_code=409, detail=message)
        self.message = message


_STATUS_TITLES = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    422: "Validation Failed",
    500: "Internal Server Error",
}


def _error_response(
    status_code: int,
    errors: list[dict],
    message: str | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "status": {
                "code": status_code,
                "message": message or _STATUS_TITLES.get(status_code, "Error"),
            },
            "errors": errors,
            "meta": {"timestamp": datetime.utcnow().isoformat()},
        },
    )


async def invalid_parameter_handler(request: Request, exc: InvalidParameterException):
    errors = [
        {
            "code": "INVALID_PARAMETER",
            "title": "Invalid query parameter",
            "detail": e["message"],
            "source": {"parameter": e["field"]},
        }
        for e in exc.validation_errors
    ]
    return _error_response(400, errors)


async def validation_handler(request: Request, exc: RequestValidationError):
    errors = []
    for error in exc.errors():
        loc = error.get("loc", [])
        field_parts = [str(p) for p in loc if isinstance(p, str) and p != "body"]
        pointer = "/".join(field_parts) if field_parts else "body"

        error_type = error.get("type", "")
        field = field_parts[-1] if field_parts else "field"
        msg = error.get("msg", "Invalid value")

        if "string_too_short" in error_type:
            min_len = (error.get("ctx") or {}).get("min_length", 1)
            msg = f"Must be at least {min_len} character(s) long"
        elif "string_too_long" in error_type:
            max_len = (error.get("ctx") or {}).get("max_length", "")
            msg = f"Must not exceed {max_len} character(s)"
        elif "missing" in error_type:
            msg = f"Field '{field}' is required"
        elif "json_invalid" in error_type:
            msg = "Invalid JSON format in request body"
        elif "enum" in error_type:
            allowed = (error.get("ctx") or {}).get("allowed_values", [])
            msg = f"Must be one of: {', '.join(str(v) for v in allowed)}"
        elif "uuid" in error_type:
            msg = f"Must be a valid UUID"
        elif "int_parsing" in error_type:
            msg = f"Must be a valid integer"
        elif "float_parsing" in error_type:
            msg = f"Must be a valid number"

        errors.append(
            {
                "code": "VALIDATION_ERROR",
                "title": "Invalid input",
                "detail": msg,
                "source": {"pointer": f"/{pointer}"},
            }
        )

    return _error_response(422, errors)


async def http_exception_handler(request: Request, exc: HTTPException):
    status_code = exc.status_code
    title = _STATUS_TITLES.get(status_code, "Error")
    return _error_response(
        status_code,
        [
            {
                "code": f"HTTP_{status_code}",
                "title": title,
                "detail": exc.detail,
            }
        ],
    )


async def business_rule_validation_handler(request: Request, exc: BusinessRuleValidationException):
    return _error_response(
        409,
        [
            {
                "code": "BUSINESS_RULE_VIOLATION",
                "title": "Business Rule Violation",
                "detail": exc.message,
            }
        ],
    )


async def unhandled_exception_handler(request: Request, exc: Exception):
    return _error_response(
        500,
        [
            {
                "code": "INTERNAL_ERROR",
                "title": "Something went wrong",
                "detail": "An unexpected error occurred while processing your request.",
            }
        ],
    )
