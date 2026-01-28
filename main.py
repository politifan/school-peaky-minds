import asyncio
import getpass
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import smtplib
import sys
import time
from datetime import date
from email.message import EmailMessage
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse
from uvicorn import run


from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.status import HTTP_302_FOUND

import httpx

try:
    from authlib.integrations.starlette_client import OAuth, OAuthError
except Exception:  # pragma: no cover - optional until deps are installed
    OAuth = None
    OAuthError = Exception

from telegram_bot import is_configured as telegram_is_configured
from telegram_bot import send_lead_message

try:
    from telethon import TelegramClient
    from telethon.errors import SessionPasswordNeededError
    from telethon.errors.rpcerrorlist import UsernameInvalidError, UsernameNotOccupiedError
except Exception:  # pragma: no cover - optional dependency
    TelegramClient = None
    SessionPasswordNeededError = Exception
    UsernameInvalidError = Exception
    UsernameNotOccupiedError = Exception

try:
    import qrcode
except Exception:  # pragma: no cover - optional dependency
    qrcode = None

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / "config" / ".env"


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


load_env(ENV_PATH)
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
ASSETS_DIR = STATIC_DIR / "assets"
DOCUMENTS_DIR = STATIC_DIR / "documents"
TEMPLATES_DIR = BASE_DIR / "templates"
DATA_DIR.mkdir(exist_ok=True)

USERS_FILE = DATA_DIR / "users.json"
CODES_FILE = DATA_DIR / "codes.json"
AGREEMENTS_DIR = DATA_DIR / "agreements"
AGREEMENTS_DIR.mkdir(exist_ok=True)

DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-secret-key")


def load_json(path: Path, default: Any) -> Any:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_current_user(request: Request) -> Optional[Dict[str, Any]]:
    return request.session.get("user")


def set_current_user(request: Request, user: Dict[str, Any]) -> None:
    request.session["user"] = user


def clear_user(request: Request) -> None:
    request.session.pop("user", None)


def send_email_code(recipient: str, code: str) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_from = os.getenv("SMTP_FROM", smtp_user or "no-reply@example.com")

    if not smtp_host or not smtp_user or not smtp_password:
        print(f"[dev] Email code for {recipient}: {code}")
        return

    msg = EmailMessage()
    msg["Subject"] = "Код для входа"
    msg["From"] = smtp_from
    msg["To"] = recipient
    msg.set_content(f"Ваш код для входа: {code}\nКод действует 10 минут.")

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


# OAuth setup
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
VK_CLIENT_ID = os.getenv("VK_CLIENT_ID")
VK_CLIENT_SECRET = os.getenv("VK_CLIENT_SECRET")
VK_SCOPE = os.getenv("VK_SCOPE", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME")
APP_BASE_URL = os.getenv("APP_BASE_URL")
CONTACT_PHONE = os.getenv("CONTACT_PHONE", "")
CONTACT_TELEGRAM = os.getenv("CONTACT_TELEGRAM", "")
CONTACT_VK = os.getenv("CONTACT_VK", "")
SEO_GOOGLE_VERIFICATION = os.getenv("SEO_GOOGLE_VERIFICATION", "")
SEO_YANDEX_VERIFICATION = os.getenv("SEO_YANDEX_VERIFICATION", "")
SEO_BING_VERIFICATION = os.getenv("SEO_BING_VERIFICATION", "")
TELETHON_API_ID = os.getenv("TG_API_ID") or os.getenv("TELETHON_API_ID")
TELETHON_API_HASH = os.getenv("TG_API_HASH") or os.getenv("TELETHON_API_HASH")
TELETHON_PASSWORD = os.getenv("TG_PASSWORD") or os.getenv("TELETHON_PASSWORD")
TELETHON_SESSION_PATH = (
    os.getenv("SESSION_PATH")
    or os.getenv("TELETHON_SESSION_PATH")
    or str((DATA_DIR / "telethon.session").resolve())
)
TELETHON_AUTO_LOGIN = os.getenv("TELETHON_AUTO_LOGIN", "").lower() in {"1", "true", "yes"}

providers = {
    "google": bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET),
    "vk": bool(VK_CLIENT_ID and VK_CLIENT_SECRET),
    "telegram": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_BOT_USERNAME),
}

TELETHON_ENABLED = bool(TELETHON_API_ID and TELETHON_API_HASH and TelegramClient)
_telethon_client: Optional["TelegramClient"] = None
_telethon_lock = None

CANONICAL_ORIGIN = None
CANONICAL_HOST = None
CANONICAL_SCHEME = None
if APP_BASE_URL:
    parsed_origin = urlparse(APP_BASE_URL.strip())
    if parsed_origin.scheme and parsed_origin.netloc:
        CANONICAL_ORIGIN = f"{parsed_origin.scheme}://{parsed_origin.netloc}"
        CANONICAL_HOST = parsed_origin.hostname
        CANONICAL_SCHEME = parsed_origin.scheme

session_domain = None
if CANONICAL_HOST and CANONICAL_HOST != "localhost" and not re.match(r"^\\d+\\.\\d+\\.\\d+\\.\\d+$", CANONICAL_HOST):
    session_domain = CANONICAL_HOST

app = FastAPI(docs_url=None, redoc_url=None)
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    max_age=60 * 60 * 24 * 14,
    same_site="lax",
    https_only=bool(CANONICAL_SCHEME == "https"),
    domain=session_domain,
)

log_path = LOGS_DIR / "app.log"
file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(file_handler)
logging.getLogger("uvicorn").addHandler(file_handler)
logging.getLogger("uvicorn.access").addHandler(file_handler)
logging.getLogger("uvicorn.error").addHandler(file_handler)

app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")
app.mount("/documents", StaticFiles(directory=DOCUMENTS_DIR), name="documents")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@app.middleware("http")
async def enforce_canonical_host(request: Request, call_next):
    if CANONICAL_ORIGIN and CANONICAL_HOST:
        if request.url.hostname and request.url.hostname != CANONICAL_HOST:
            target = f"{CANONICAL_ORIGIN}{request.url.path}"
            if request.url.query:
                target = f"{target}?{request.url.query}"
            return RedirectResponse(target, status_code=HTTP_302_FOUND)
    return await call_next(request)

def get_telethon_lock() -> asyncio.Lock:
    global _telethon_lock
    if _telethon_lock is None:
        # создаём lock только когда уже есть активный event loop
        asyncio.get_running_loop()
        _telethon_lock = asyncio.Lock()
    return _telethon_lock

def build_redirect_uri(request: Request, route_name: str) -> str:
    url = request.url_for(route_name)
    if APP_BASE_URL:
        return f"{APP_BASE_URL.rstrip('/')}{url.path}"
    return str(url)


oauth = OAuth() if OAuth else None
if oauth and providers["google"]:
    oauth.register(
        name="google",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )

if oauth and providers["vk"]:
    vk_kwargs = {"v": "5.131"}
    if VK_SCOPE:
        vk_kwargs["scope"] = VK_SCOPE
    oauth.register(
        name="vk",
        client_id=VK_CLIENT_ID,
        client_secret=VK_CLIENT_SECRET,
        authorize_url="https://oauth.vk.com/authorize",
        access_token_url="https://oauth.vk.com/access_token",
        api_base_url="https://api.vk.com/method/",
        client_kwargs=vk_kwargs,
    )


def verify_telegram_auth(data: Dict[str, str]) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False

    received_hash = data.get("hash")
    if not received_hash:
        return False

    data_check_items = [f"{k}={v}" for k, v in sorted(data.items()) if k != "hash"]
    data_check_string = "\n".join(data_check_items)
    secret_key = hashlib.sha256(TELEGRAM_BOT_TOKEN.encode("utf-8")).digest()
    calculated_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(calculated_hash, received_hash)


TELEGRAM_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")


def _telethon_api_id() -> Optional[int]:
    if not TELETHON_API_ID:
        return None
    try:
        return int(str(TELETHON_API_ID).strip())
    except (TypeError, ValueError):
        return None


def _print_qr_ascii(url: str) -> None:
    if qrcode is None:
        print(url)
        return
    qr = qrcode.QRCode(border=1)
    qr.add_data(url)
    qr.make(fit=True)
    qr.print_ascii(invert=True)


async def get_telethon_client() -> Optional["TelegramClient"]:
    if not TELETHON_ENABLED:
        return None
    api_id = _telethon_api_id()
    if not api_id:
        return None
    async with get_telethon_lock():
        global _telethon_client
        if _telethon_client is None:
            _telethon_client = TelegramClient(TELETHON_SESSION_PATH, api_id, TELETHON_API_HASH)
        if not _telethon_client.is_connected():
            await _telethon_client.connect()
        return _telethon_client


async def ensure_telethon_login(client: "TelegramClient", *, interactive: bool = False) -> bool:
    await client.connect()
    if await client.is_user_authorized():
        return True
    if not interactive:
        return False
    while True:
        qr_login = await client.qr_login()
        print("\nОтсканируйте QR‑код в Telegram:\n")
        _print_qr_ascii(qr_login.url)
        try:
            await qr_login.wait()
        except SessionPasswordNeededError:
            password = TELETHON_PASSWORD or getpass.getpass("Введите 2FA пароль Telegram: ")
            await client.sign_in(password=password)
        except asyncio.TimeoutError:
            print("QR‑код истёк. Генерирую новый…")
            continue
        if await client.is_user_authorized():
            break
        print("Авторизация не завершена. Генерирую новый QR‑код…")
    return await client.is_user_authorized()


async def telethon_login_cli() -> None:
    client = await get_telethon_client()
    if not client:
        print("Telethon не настроен. Нужны TG_API_ID и TG_API_HASH.")
        return
    ok = await ensure_telethon_login(client, interactive=True)
    if ok:
        print(f"Telethon авторизован. Сессия сохранена: {TELETHON_SESSION_PATH}")
    else:
        print("Не удалось авторизоваться через Telethon.")


def normalize_phone(phone: str) -> Optional[str]:
    value = (phone or "").strip()
    return value or None


def build_phone_link(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    cleaned = re.sub(r"[^\d+]", "", phone)
    if not cleaned:
        return None
    if not cleaned.startswith("+"):
        cleaned = f"+{cleaned}"
    return f"tel:{cleaned}"


def normalize_telegram(handle: str) -> Optional[str]:
    value = (handle or "").strip()
    if not value:
        return None
    if value.startswith("http://") or value.startswith("https://"):
        match = re.search(r"t\\.me/([A-Za-z0-9_]{5,32})", value)
        return match.group(1) if match else value
    if value.startswith("@"):
        value = value[1:]
    return value or None


def build_telegram_link(handle: Optional[str]) -> Optional[str]:
    if not handle:
        return None
    if handle.startswith("http://") or handle.startswith("https://"):
        return handle
    return f"https://t.me/{handle}"


def normalize_vk(handle: str) -> Optional[str]:
    value = (handle or "").strip()
    return value or None


def build_vk_link(handle: Optional[str]) -> Optional[str]:
    if not handle:
        return None
    if handle.startswith("http://") or handle.startswith("https://"):
        return handle
    if handle.startswith("vk.com/"):
        return f"https://{handle}"
    value = handle.lstrip("@")
    return f"https://vk.com/{value}"


def save_agreement(payload: Dict[str, Any]) -> Path:
    file_name = f"agreement_{int(time.time())}_{secrets.token_hex(4)}.json"
    path = AGREEMENTS_DIR / file_name
    save_json(path, payload)
    return path


def render(request: Request, template_name: str, context: Optional[Dict[str, Any]] = None) -> HTMLResponse:
    phone = normalize_phone(CONTACT_PHONE)
    phone_link = build_phone_link(phone)

    telegram_value = normalize_telegram(CONTACT_TELEGRAM)
    telegram_link = build_telegram_link(telegram_value)
    if telegram_value and not telegram_value.startswith("http"):
        telegram_display = f"@{telegram_value}"
    else:
        telegram_display = telegram_value

    vk_value = normalize_vk(CONTACT_VK)
    vk_link = build_vk_link(vk_value)
    if vk_value and not (vk_value.startswith("http") or vk_value.startswith("vk.com/")):
        vk_display = f"vk.com/{vk_value.lstrip('@')}"
    else:
        vk_display = vk_value

    ctx = {
        "request": request,
        "user": get_current_user(request),
        "contact_phone": phone,
        "contact_phone_link": phone_link,
        "contact_telegram": telegram_display,
        "contact_telegram_link": telegram_link,
        "contact_vk": vk_display,
        "contact_vk_link": vk_link,
        "seo_google_verification": SEO_GOOGLE_VERIFICATION,
        "seo_yandex_verification": SEO_YANDEX_VERIFICATION,
        "seo_bing_verification": SEO_BING_VERIFICATION,
    }
    if context:
        ctx.update(context)
    return templates.TemplateResponse(template_name, ctx)


def login_context(request: Request, next_url: Optional[str] = None, error: Optional[str] = None) -> Dict[str, Any]:
    ctx = {"providers": providers, "telegram_bot_username": TELEGRAM_BOT_USERNAME}
    if next_url is not None:
        ctx["next"] = next_url
    if error:
        ctx["error"] = error
    if providers["telegram"]:
        ctx["telegram_auth_url"] = build_redirect_uri(request, "login_telegram")
    return ctx


@app.get("/", include_in_schema=False)
def index(request: Request):
    return render(request, "index.html")


@app.get("/index.html", include_in_schema=False)
def index_alias(request: Request):
    return render(request, "index.html")


@app.get("/robots.txt", include_in_schema=False)
def robots(request: Request):
    base_url = str(request.base_url)
    content = f"User-agent: *\nAllow: /\nSitemap: {base_url}sitemap.xml\n"
    return PlainTextResponse(content, media_type="text/plain")


@app.get("/sitemap.xml", include_in_schema=False)
def sitemap(request: Request):
    base_url = str(request.base_url)
    lastmod = date.today().isoformat()
    urls = [
        ("", "1.0"),
        ("courses/fullstack", "0.85"),
        ("courses/data-science", "0.85"),
        ("courses/business", "0.85"),
        ("courses/python-beginners", "0.85"),
    ]
    entries = "\n".join(
        [
            "  <url>"
            f"<loc>{base_url}{path}</loc>"
            f"<lastmod>{lastmod}</lastmod>"
            f"<changefreq>weekly</changefreq>"
            f"<priority>{priority}</priority>"
            "</url>"
            for path, priority in urls
        ]
    )
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{entries}
</urlset>"""
    return Response(content=xml, media_type="application/xml")


@app.get("/course-fullstack.html", include_in_schema=False)
def course_fullstack_legacy(request: Request):
    return RedirectResponse("/courses/fullstack", status_code=HTTP_302_FOUND)


@app.get("/courses/fullstack", include_in_schema=False)
@app.get("/courses/fullstack/", include_in_schema=False)
def course_fullstack(request: Request):
    return render(request, "course-fullstack.html", {"course_slug": "Full-stack"})


@app.get("/course-datascience.html", include_in_schema=False)
def course_datascience_legacy(request: Request):
    return RedirectResponse("/courses/data-science", status_code=HTTP_302_FOUND)


@app.get("/courses/data-science", include_in_schema=False)
@app.get("/courses/data-science/", include_in_schema=False)
def course_datascience(request: Request):
    return render(request, "course-datascience.html", {"course_slug": "Data Science"})


@app.get("/course-business.html", include_in_schema=False)
def course_business_legacy(request: Request):
    return RedirectResponse("/courses/business", status_code=HTTP_302_FOUND)


@app.get("/course-python-beginners.html", include_in_schema=False)
def course_python_beginners_legacy(request: Request):
    return RedirectResponse("/courses/python-beginners", status_code=HTTP_302_FOUND)


@app.get("/courses/business", include_in_schema=False)
@app.get("/courses/business/", include_in_schema=False)
def course_business(request: Request):
    return render(request, "course-business.html", {"course_slug": "Business"})


@app.get("/courses/python-beginners", include_in_schema=False)
@app.get("/courses/python-beginners/", include_in_schema=False)
def course_python_beginners(request: Request):
    return render(request, "course-python-beginners.html", {"course_slug": "Python для новичков"})


@app.get("/login", include_in_schema=False)
def login(request: Request):
    next_url = request.query_params.get("next") or "/"
    return render(request, "login.html", login_context(request, next_url=next_url))


@app.post("/login/email", include_in_schema=False)
async def login_email(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    next_url = str(form.get("next", "/"))
    if not email:
        return render(request, "login.html", login_context(request, next_url=next_url, error="Введите email"))

    code = f"{secrets.randbelow(10 ** 6):06d}"
    codes = load_json(CODES_FILE, {})
    codes[email] = {"code": code, "expires": time.time() + 600}
    save_json(CODES_FILE, codes)
    send_email_code(email, code)

    return render(request, "verify.html", {"email": email, "next": next_url})


@app.post("/login/verify", include_in_schema=False)
async def login_verify(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    code = str(form.get("code", "")).strip()
    next_url = str(form.get("next", "/"))

    codes = load_json(CODES_FILE, {})
    entry = codes.get(email)
    if not entry or entry.get("code") != code or entry.get("expires", 0) < time.time():
        return render(request, "verify.html", {"email": email, "next": next_url, "error": "Неверный или просроченный код"})

    users = load_json(USERS_FILE, {})
    user_id = f"email:{email}"
    user = users.get(user_id) or {"id": user_id, "email": email, "name": email, "provider": "email"}
    users[user_id] = user
    save_json(USERS_FILE, users)

    set_current_user(request, user)
    codes.pop(email, None)
    save_json(CODES_FILE, codes)

    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@app.get("/login/google", include_in_schema=False)
async def login_google(request: Request):
    if not (oauth and providers["google"]):
        return render(request, "login.html", login_context(request, error="Google OAuth не настроен"))
    redirect_uri = build_redirect_uri(request, "auth_google")
    return await oauth.google.authorize_redirect(request, redirect_uri)


@app.get("/auth/google/callback", include_in_schema=False)
async def auth_google(request: Request):
    if not oauth:
        return render(request, "login.html", login_context(request, error="Google OAuth не настроен"))
    try:
        token = await oauth.google.authorize_access_token(request)
        userinfo = None
        if isinstance(token, dict) and token.get("id_token"):
            try:
                userinfo = await oauth.google.parse_id_token(request, token)
            except Exception:
                userinfo = None
        if not userinfo:
            userinfo_resp = await oauth.google.get("userinfo")
            userinfo = userinfo_resp.json()
    except OAuthError as exc:
        detail = getattr(exc, "error", None) or str(exc) or "OAuthError"
        description = getattr(exc, "description", None) or ""
        response_hint = ""
        extra = ""
        response = getattr(exc, "response", None)
        if response is not None:
            try:
                response_hint = f" (status {response.status_code})"
            except Exception:
                response_hint = ""
            try:
                data = response.json()
                if isinstance(data, dict):
                    err = data.get("error")
                    err_desc = data.get("error_description")
                    if err or err_desc:
                        extra = f" [{err}: {err_desc}]" if err_desc else f" [{err}]"
            except Exception:
                extra = ""
        message = f"{detail}{response_hint}{extra}"
        if description:
            message = f"{message}: {description}"
        safe_detail = re.sub(r"[\\r\\n]+", " ", message)[:300]
        return render(
            request,
            "login.html",
            login_context(request, error=f"Ошибка авторизации Google: {safe_detail}"),
        )
    except Exception as exc:
        safe_detail = re.sub(r"[\\r\\n]+", " ", str(exc) or "Unknown error")[:300]
        return render(
            request,
            "login.html",
            login_context(request, error=f"Ошибка авторизации Google: {safe_detail}"),
        )

    users = load_json(USERS_FILE, {})
    user_id = f"google:{userinfo.get('sub')}"
    user = {
        "id": user_id,
        "email": userinfo.get("email"),
        "name": userinfo.get("name") or userinfo.get("email"),
        "provider": "google",
    }
    users[user_id] = user
    save_json(USERS_FILE, users)

    set_current_user(request, user)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@app.get("/login/vk", include_in_schema=False)
async def login_vk(request: Request):
    if not (oauth and providers["vk"]):
        return render(request, "login.html", login_context(request, error="VK OAuth не настроен"))
    redirect_uri = build_redirect_uri(request, "auth_vk")
    return await oauth.vk.authorize_redirect(request, redirect_uri)


@app.get("/auth/vk/callback", include_in_schema=False)
async def auth_vk(request: Request):
    if not oauth:
        return render(request, "login.html", login_context(request, error="VK OAuth не настроен"))
    try:
        token = await oauth.vk.authorize_access_token(request)
        user_resp = await oauth.vk.get("users.get", params={"v": "5.131", "fields": "photo_200"})
        profile = user_resp.json().get("response", [{}])[0]
    except OAuthError:
        return render(request, "login.html", login_context(request, error="Ошибка авторизации VK"))

    users = load_json(USERS_FILE, {})
    user_id = f"vk:{profile.get('id')}"
    user = {
        "id": user_id,
        "email": token.get("email"),
        "name": f"{profile.get('first_name', '')} {profile.get('last_name', '')}".strip(),
        "provider": "vk",
    }
    users[user_id] = user
    save_json(USERS_FILE, users)

    set_current_user(request, user)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@app.get("/login/telegram", include_in_schema=False)
async def login_telegram(request: Request):
    data = dict(request.query_params)
    if not data or "hash" not in data:
        return render(request, "login.html", login_context(request, error="Нажмите и подтвердите вход через Telegram"))
    if not verify_telegram_auth(data):
        return render(request, "login.html", login_context(request, error="Ошибка авторизации Telegram"))

    users = load_json(USERS_FILE, {})
    user_id = f"telegram:{data.get('id')}"
    user = {
        "id": user_id,
        "email": None,
        "name": data.get("first_name") or data.get("username") or "Telegram",
        "provider": "telegram",
    }
    users[user_id] = user
    save_json(USERS_FILE, users)
    set_current_user(request, user)

    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@app.get("/validate/telegram", include_in_schema=False)
async def validate_telegram(username: str) -> dict:
    handle = username.strip().lstrip("@")
    if not handle or not TELEGRAM_USERNAME_RE.match(handle):
        return {"ok": False, "reason": "invalid"}

    if TELETHON_ENABLED:
        client = await get_telethon_client()
        if not client:
            return {"ok": False, "reason": "not_configured"}
        if not await client.is_user_authorized():
            if TELETHON_AUTO_LOGIN:
                await ensure_telethon_login(client, interactive=True)
            if not await client.is_user_authorized():
                return {"ok": False, "reason": "telethon_login_required"}
        try:
            entity = await client.get_entity(handle)
            return {"ok": True, "description": getattr(entity, "first_name", "") or getattr(entity, "title", "")}
        except (UsernameInvalidError, ValueError):
            return {"ok": False, "reason": "invalid"}
        except UsernameNotOccupiedError:
            return {"ok": False, "reason": "not_found"}
        except Exception:
            return {"ok": False, "reason": "error"}

    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "reason": "not_configured"}

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChat"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url, params={"chat_id": f"@{handle}"})
        data = response.json()
    except Exception:
        return {"ok": False, "reason": "error"}

    return {"ok": bool(data.get("ok")), "description": data.get("description")}


@app.get("/logout", include_in_schema=False)
def logout(request: Request):
    clear_user(request)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@app.get("/account", include_in_schema=False)
def account(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=HTTP_302_FOUND)
    return render(request, "account.html")


@app.post("/apply", include_in_schema=False)
async def apply(request: Request):
    form = await request.form()
    name = str(form.get("name", "")).strip()
    contact = str(form.get("phone", "")).strip()
    course = str(form.get("course", "")).strip()
    page = request.headers.get("referer", "")

    if telegram_is_configured():
        text = (
            "<b>Новая заявка</b>\n"
            f"Имя: {name or '—'}\n"
            f"Контакт: {contact or '—'}\n"
            f"Курс: {course or '—'}\n"
            f"Страница: {page or '—'}"
        )
        try:
            await send_lead_message(text)
        except Exception:
            pass

    if "text/html" in request.headers.get("accept", ""):
        return render(request, "success.html")

    return HTMLResponse("OK")


@app.post("/enroll", include_in_schema=False)
async def enroll(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=HTTP_302_FOUND)

    form = await request.form()
    payload = {
        "timestamp": int(time.time()),
        "user": user,
        "course": form.get("course"),
        "full_name": form.get("full_name"),
        "phone": form.get("phone"),
        "email": form.get("email"),
        "telegram": form.get("telegram"),
        "agreement": form.get("agreement"),
        "consent": form.get("consent"),
    }

    save_agreement(payload)

    if telegram_is_configured():
        text = (
            "<b>Заявка на покупку курса</b>\n"
            f"Курс: {payload.get('course')}\n"
            f"ФИО: {payload.get('full_name')}\n"
            f"Телефон: {payload.get('phone')}\n"
            f"Email: {payload.get('email')}\n"
            f"Telegram: {payload.get('telegram')}"
        )
        try:
            await send_lead_message(text)
        except Exception:
            pass

    return render(request, "enroll_success.html", {"course": payload.get("course")})


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok"}

if __name__ == "__main__":
    if "--telethon-login" in sys.argv:
        asyncio.run(telethon_login_cli())
    else:
        run(app)
