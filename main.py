
import asyncio
import copy
import logging
import os
import secrets
import sys
import time
import traceback

from anyio import to_thread
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exception_handlers import http_exception_handler as default_http_exception_handler
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.status import HTTP_302_FOUND
from uvicorn import run

from logging.handlers import RotatingFileHandler

from core import (
    ASSETS_DIR,
    CANONICAL_HOST,
    CANONICAL_ORIGIN,
    CANONICAL_SCHEME,
    DOCUMENTS_DIR,
    LOGS_DIR,
    SESSION_DOMAIN,
    SESSION_SECRET,
    load_metrics,
    render,
    save_metrics,
    telethon_login_cli,
)
from routes.admin import router as admin_router
from routes.auth import router as auth_router
from routes.contracts import router as contracts_router
from routes.forms import router as forms_router
from routes.payments import router as payments_router
from routes.public import router as public_router

app = FastAPI(docs_url=None, redoc_url=None)
SAMESITE = "none" if CANONICAL_SCHEME == "https" else "lax"
INLINE_500_DEBUG = os.getenv("INLINE_500_DEBUG", "1").strip().lower() not in {"0", "false", "no", "off"}
app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    max_age=60 * 60 * 24 * 14,
    same_site=SAMESITE,
    https_only=bool(CANONICAL_SCHEME == "https"),
    domain=SESSION_DOMAIN,
)

log_path = LOGS_DIR / "app.log"
file_handler = RotatingFileHandler(
    log_path,
    maxBytes=5_000_000,
    backupCount=3,
    encoding="utf-8",
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(file_handler)
logging.getLogger("uvicorn").addHandler(file_handler)
logging.getLogger("uvicorn.access").addHandler(file_handler)
logging.getLogger("uvicorn.error").addHandler(file_handler)
access_logger = logging.getLogger("app.access")
access_logger.setLevel(logging.INFO)
access_logger.addHandler(file_handler)
access_logger.propagate = False
error_logger = logging.getLogger("app.errors")
error_logger.setLevel(logging.INFO)
error_logger.addHandler(file_handler)
error_logger.propagate = False
logging.getLogger(__name__).info("Logging initialized. Writing to %s", log_path)

app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")
app.mount("/documents", StaticFiles(directory=DOCUMENTS_DIR), name="documents")

_METRICS_CACHE = None
_METRICS_DIRTY = False
_METRICS_LAST_FLUSH = 0.0
_METRICS_FLUSH_INTERVAL = 15.0
_METRICS_LOCK: asyncio.Lock | None = None
_METRICS_FLUSH_TASK: asyncio.Task | None = None


def _forwarded_scheme(request: Request) -> str:
    cf_visitor = request.headers.get("cf-visitor", "")
    if '"scheme":"https"' in cf_visitor:
        return "https"
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip()
    if forwarded_proto:
        return forwarded_proto
    return request.url.scheme


def _forwarded_host(request: Request) -> str:
    forwarded_host = (request.headers.get("x-forwarded-host") or "").split(",", 1)[0].strip()
    host = forwarded_host or request.headers.get("host") or (request.url.hostname or "")
    return host.split(":", 1)[0].strip()


def _static_cache_control(path: str) -> str:
    lowered = path.lower()
    if lowered.startswith("/assets/"):
        if lowered.endswith((".css", ".js")):
            return "public, max-age=86400, stale-while-revalidate=604800"
        if lowered.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico")):
            return "public, max-age=2592000, stale-while-revalidate=604800"
        if lowered.endswith((".woff", ".woff2", ".ttf", ".otf")):
            return "public, max-age=2592000, stale-while-revalidate=604800"
        return "public, max-age=3600, stale-while-revalidate=86400"
    if lowered.startswith("/documents/"):
        return "public, max-age=3600, stale-while-revalidate=86400"
    return ""


async def _ensure_metrics_state() -> None:
    global _METRICS_CACHE, _METRICS_LOCK
    if _METRICS_CACHE is None:
        _METRICS_CACHE = load_metrics()
    if _METRICS_LOCK is None:
        _METRICS_LOCK = asyncio.Lock()


async def _flush_metrics(force: bool = False) -> None:
    global _METRICS_DIRTY, _METRICS_LAST_FLUSH, _METRICS_FLUSH_TASK
    await _ensure_metrics_state()
    if not _METRICS_DIRTY or _METRICS_CACHE is None:
        _METRICS_FLUSH_TASK = None
        return
    now = time.time()
    if not force and now - _METRICS_LAST_FLUSH < _METRICS_FLUSH_INTERVAL:
        _METRICS_FLUSH_TASK = None
        return
    snapshot = copy.deepcopy(_METRICS_CACHE)
    _METRICS_DIRTY = False
    _METRICS_LAST_FLUSH = now
    _METRICS_FLUSH_TASK = None
    await to_thread.run_sync(save_metrics, snapshot)


def _schedule_metrics_flush() -> None:
    global _METRICS_FLUSH_TASK
    if _METRICS_FLUSH_TASK and not _METRICS_FLUSH_TASK.done():
        return

    async def _runner():
        await asyncio.sleep(_METRICS_FLUSH_INTERVAL)
        if _METRICS_LOCK is None:
            return
        async with _METRICS_LOCK:
            await _flush_metrics(force=True)

    _METRICS_FLUSH_TASK = asyncio.create_task(_runner())


@app.middleware("http")
async def enforce_canonical_host(request: Request, call_next):
    if CANONICAL_ORIGIN and CANONICAL_HOST:
        current_scheme = _forwarded_scheme(request)
        current_host = _forwarded_host(request)
        scheme_mismatch = CANONICAL_SCHEME and current_scheme != CANONICAL_SCHEME
        host_mismatch = bool(current_host and current_host != CANONICAL_HOST)
        if scheme_mismatch or host_mismatch:
            target = f"{CANONICAL_ORIGIN}{request.url.path}"
            if request.url.query:
                target = f"{target}?{request.url.query}"
            return RedirectResponse(target, status_code=HTTP_302_FOUND)
    return await call_next(request)


@app.middleware("http")
async def track_metrics(request: Request, call_next):
    response = await call_next(request)
    try:
        if request.method != "GET":
            return response
        path = request.url.path
        if path.startswith(("/assets", "/documents")):
            return response
        if path in ("/healthz", "/favicon.ico", "/robots.txt", "/sitemap.xml"):
            return response
        accept = request.headers.get("accept", "")
        if "text/html" not in accept and path != "/":
            return response

        await _ensure_metrics_state()
        visit_id = request.cookies.get("visit_id")
        if not visit_id:
            visit_id = secrets.token_hex(8)
            response.set_cookie(
                "visit_id",
                visit_id,
                max_age=60 * 60 * 24 * 365,
                httponly=True,
                samesite="lax",
                secure=bool(CANONICAL_SCHEME == "https"),
                domain=SESSION_DOMAIN,
            )
        assert _METRICS_LOCK is not None
        async with _METRICS_LOCK:
            metrics = _METRICS_CACHE
            metrics["total_visits"] += 1
            unique_ids = metrics.get("unique_ids")
            if not isinstance(unique_ids, dict):
                unique_ids = {}
                metrics["unique_ids"] = unique_ids
            if visit_id not in unique_ids:
                metrics["unique_visits"] += 1
                unique_ids[visit_id] = int(time.time())
            if len(unique_ids) > 20000:
                cutoff = int(time.time() - 60 * 60 * 24 * 120)
                for key, ts in list(unique_ids.items()):
                    if ts < cutoff:
                        unique_ids.pop(key, None)
            metrics["path_counts"][path] = metrics["path_counts"].get(path, 0) + 1

            if path == "/":
                metrics["funnel"]["home"] = metrics["funnel"].get("home", 0) + 1
            if path == "/login":
                metrics["funnel"]["login"] = metrics["funnel"].get("login", 0) + 1

            global _METRICS_DIRTY
            _METRICS_DIRTY = True
            _schedule_metrics_flush()
    except Exception:
        error_logger.exception("Metrics tracking failed for %s %s", request.method, request.url.path)
    return response


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    try:
        response = await call_next(request)
    except Exception:
        error_logger.exception(
            "Unhandled error on %s %s", request.method, request.url.path
        )
        raise
    duration = (time.time() - start) * 1000
    access_logger.info(
        "%s %s -> %s (%.1f ms)",
        request.method,
        request.url.path,
        response.status_code,
        duration,
    )
    if response.status_code >= 500:
        error_logger.error(
            "HTTP %s on %s %s (%.1f ms)",
            response.status_code,
            request.method,
            request.url.path,
            duration,
        )
    cache_control = _static_cache_control(request.url.path)
    if cache_control:
        if "Cache-Control" not in response.headers:
            response.headers["Cache-Control"] = cache_control
        if "Vary" not in response.headers:
            response.headers["Vary"] = "Accept-Encoding"
    if request.url.path.lower().endswith((".css", ".js")):
        if "X-Content-Type-Options" not in response.headers:
            response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@app.on_event("startup")
async def log_startup():
    try:
        limiter = to_thread.current_default_thread_limiter()
        limiter.total_tokens = 8
        access_logger.info("Thread limiter set to %s tokens.", limiter.total_tokens)
    except Exception:
        logging.getLogger(__name__).warning("Failed to set thread limiter.")
    await _ensure_metrics_state()
    access_logger.info("App startup complete.")


@app.on_event("shutdown")
async def flush_metrics_on_shutdown():
    await _ensure_metrics_state()
    if _METRICS_LOCK is None:
        return
    async with _METRICS_LOCK:
        await _flush_metrics(force=True)


app.include_router(public_router)
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(forms_router)
app.include_router(contracts_router)
app.include_router(payments_router)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        response = render(request, "404.html")
        response.status_code = 404
        return response
    return await default_http_exception_handler(request, exc)


@app.exception_handler(Exception)
async def internal_error_handler(request: Request, exc: Exception):
    error_logger.exception(
        "Unhandled exception on %s %s",
        request.method,
        request.url.path,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    if not INLINE_500_DEBUG:
        return PlainTextResponse("Internal Server Error", status_code=500)
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    body = (
        f"500 Internal Server Error\n\n"
        f"{request.method} {request.url}\n\n"
        f"{tb}"
    )
    return PlainTextResponse(body, status_code=500)


if __name__ == "__main__":
    if "--telethon-login" in sys.argv:
        asyncio.run(telethon_login_cli())
    else:
        run(app)
