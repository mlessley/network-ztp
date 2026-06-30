from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


def _problem(
    status: int, title: str, detail: str, instance: str, trace_id: str = ""
) -> dict[str, object]:
    body: dict[str, object] = {
        "type": f"https://network-ztp/errors/{title.lower().replace(' ', '-')}",
        "title": title,
        "status": status,
        "detail": detail,
        "instance": instance,
    }
    if trace_id:
        body["trace_id"] = trace_id
    return body


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        trace_id = getattr(request.state, "trace_id", "")
        titles = {
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            409: "Conflict",
            422: "Unprocessable Entity",
            503: "Service Unavailable",
        }
        title = titles.get(exc.status_code, "Error")
        return JSONResponse(
            status_code=exc.status_code,
            content=_problem(
                exc.status_code,
                title,
                str(exc.detail),
                str(request.url.path),
                trace_id,
            ),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        trace_id = getattr(request.state, "trace_id", "")
        return JSONResponse(
            status_code=500,
            content=_problem(
                500, "Internal Server Error", str(exc), str(request.url.path), trace_id
            ),
        )
