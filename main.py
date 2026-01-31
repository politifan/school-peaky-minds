import asyncio
import logging
import secrets
import sys
import time

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exception_handlers import http_exception_handler as default_http_exception_handler
from fastapi.staticfiles import StaticFiles
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
from routes.public import router as public_router

app = FastAPI(docs_url=None, redoc_url=None)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    max_age=60 * 60 * 24 * 14,
    same_site="lax",
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
logging.getLogger(__name__).info("Logging initialized. Writing to %s", log_path)

app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")
app.mount("/documents", StaticFiles(directory=DOCUMENTS_DIR), name="documents")


@app.middleware("http")
async def enforce_canonical_host(request: Request, call_next):
    if CANONICAL_ORIGIN and CANONICAL_HOST:
        if request.url.hostname and request.url.hostname != CANONICAL_HOST:
            target = f"{CANONICAL_ORIGIN}{request.url.path}"
            if request.url.query:
                target = f"{target}?{request.url.query}"
            return RedirectResponse(target, status_code=HTTP_302_FOUND)
    return await call_next(request)


@app.middleware("http")
async def track_metrics(request: Request, call_next):
    response = await call_next(request)
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

    metrics = load_metrics()
    metrics["total_visits"] += 1
    visit_id = request.session.get("visit_id") or request.cookies.get("visit_id")
    if not visit_id:
        visit_id = secrets.token_hex(8)
        request.session["visit_id"] = visit_id
        response.set_cookie(
            "visit_id",
            visit_id,
            max_age=60 * 60 * 24 * 365,
            httponly=True,
            samesite="lax",
            secure=bool(CANONICAL_SCHEME == "https"),
            domain=SESSION_DOMAIN,
        )
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

    save_metrics(metrics)
    return response


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = (time.time() - start) * 1000
    access_logger.info(
        "%s %s -> %s (%.1f ms)",
        request.method,
        request.url.path,
        response.status_code,
        duration,
    )
    return response


@app.on_event("startup")
async def log_startup():
    access_logger.info("App startup complete.")


app.include_router(public_router)
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(forms_router)
app.include_router(contracts_router)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        response = render(request, "404.html")
        response.status_code = 404
        return response
    return await default_http_exception_handler(request, exc)

 
if __name__ == "__main__":
    if "--telethon-login" in sys.argv:
        asyncio.run(telethon_login_cli())
    else:
        run(app)
