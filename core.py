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
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_302_FOUND

try:
    from authlib.integrations.starlette_client import OAuth, OAuthError
except Exception:  # pragma: no cover - optional until deps are installed
    OAuth = None
    OAuthError = Exception

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
LEADS_DIR = DATA_DIR / "leads"
LEADS_DIR.mkdir(exist_ok=True)
METRICS_FILE = DATA_DIR / "metrics.json"
WHITELIST_FILE = DATA_DIR / "telegram_whitelist.json"

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


def load_whitelist() -> List[int]:
    default_ids = [980343575, 1065558838, 1547353132]
    if not WHITELIST_FILE.exists():
        try:
            save_json(WHITELIST_FILE, default_ids)
        except Exception:
            logging.getLogger("core").warning("Failed to create whitelist file", exc_info=True)
    data = load_json(WHITELIST_FILE, default_ids)
    if not isinstance(data, list):
        data = default_ids
    cleaned = []
    for item in data:
        try:
            cleaned.append(int(item))
        except Exception:
            continue
    if not cleaned:
        cleaned = default_ids
    target_admin_id = 1547353132
    if target_admin_id not in cleaned:
        if len(cleaned) >= 2:
            last = cleaned[-1]
            cleaned = cleaned[:-1] + [target_admin_id, last]
        else:
            cleaned.append(target_admin_id)
        try:
            save_whitelist(cleaned)
        except Exception:
            logging.getLogger("core").warning("Failed to persist whitelist update", exc_info=True)
    elif len(cleaned) >= 2 and cleaned[-1] == target_admin_id:
        last = cleaned[-2]
        cleaned = cleaned[:-2] + [target_admin_id, last]
        try:
            save_whitelist(cleaned)
        except Exception:
            logging.getLogger("core").warning("Failed to persist whitelist reorder", exc_info=True)
    return cleaned


WHITELIST_IDS = load_whitelist()


def save_whitelist(ids: List[int]) -> None:
    save_json(WHITELIST_FILE, ids)


def get_admin_ids() -> Set[int]:
    if len(WHITELIST_IDS) <= 1:
        return set(WHITELIST_IDS)
    return set(WHITELIST_IDS[:-1])


def is_admin_user(user: Optional[Dict[str, Any]]) -> bool:
    if not user or user.get("provider") != "telegram":
        return False
    user_id = str(user.get("id", ""))
    if not user_id.startswith("telegram:"):
        return False
    try:
        tg_id = int(user_id.split("telegram:", 1)[1])
    except Exception:
        return False
    return tg_id in get_admin_ids()


def next_lead_path() -> Path:
    return LEADS_DIR / f"lead_{int(time.time())}_{secrets.token_hex(4)}.json"


def save_lead(payload: Dict[str, Any]) -> Path:
    path = next_lead_path()
    save_json(path, payload)
    return path


def load_leads() -> List[Dict[str, Any]]:
    items = []
    for path in sorted(LEADS_DIR.glob("lead_*.json")):
        data = load_json(path, {})
        if isinstance(data, dict):
            data["_file"] = path.name
            items.append(data)
    return sorted(items, key=lambda item: item.get("timestamp", 0), reverse=True)


def update_lead_status(file_name: str, status: str) -> bool:
    if not file_name:
        return False
    path = LEADS_DIR / file_name
    if not path.exists():
        return False
    data = load_json(path, {})
    if not isinstance(data, dict):
        return False
    if status:
        data["status"] = status
        data["status_updated_at"] = int(time.time())
    else:
        data.pop("status", None)
        data.pop("status_updated_at", None)
    save_json(path, data)
    return True


def update_agreement_status(file_name: str, status: str) -> bool:
    if not file_name:
        return False
    path = AGREEMENTS_DIR / file_name
    if not path.exists():
        return False
    data = load_json(path, {})
    if not isinstance(data, dict):
        return False
    if status:
        data["status"] = status
    else:
        data.pop("status", None)
    save_json(path, data)
    return True


def load_agreements() -> List[Dict[str, Any]]:
    items = []
    for path in sorted(AGREEMENTS_DIR.glob("agreement_*.json")):
        data = load_json(path, {})
        if isinstance(data, dict):
            data["_file"] = path.name
            items.append(data)
    return sorted(items, key=lambda item: item.get("timestamp", 0), reverse=True)


def load_metrics() -> dict:
    default = {
        "total_visits": 0,
        "unique_visits": 0,
        "path_counts": {},
        "funnel": {"home": 0, "login": 0, "apply": 0, "enroll": 0},
    }
    data = load_json(METRICS_FILE, default)
    if not isinstance(data, dict):
        return default
    for key in ("total_visits", "unique_visits"):
        if not isinstance(data.get(key), int):
            data[key] = 0
    if not isinstance(data.get("path_counts"), dict):
        data["path_counts"] = {}
    if not isinstance(data.get("funnel"), dict):
        data["funnel"] = {"home": 0, "login": 0, "apply": 0, "enroll": 0}
    return data


def save_metrics(metrics: dict) -> None:
    save_json(METRICS_FILE, metrics)


def parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def within_dates(timestamp: Optional[int], date_from: Optional[date], date_to: Optional[date]) -> bool:
    if not timestamp:
        return False
    try:
        ts_date = datetime.fromtimestamp(int(timestamp)).date()
    except Exception:
        return False
    if date_from and ts_date < date_from:
        return False
    if date_to and ts_date > date_to:
        return False
    return True


def filter_items(items: List[Dict[str, Any]], course: str, date_from: Optional[date], date_to: Optional[date]) -> List[Dict[str, Any]]:
    filtered = items
    if course:
        filtered = [item for item in filtered if (item.get("course") or "") == course]
    if date_from or date_to:
        filtered = [item for item in filtered if within_dates(item.get("timestamp"), date_from, date_to)]
    return filtered


def admin_required(request: Request) -> Optional[Response]:
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/admin", status_code=HTTP_302_FOUND)
    if not is_admin_user(user):
        return HTMLResponse("Доступ запрещён", status_code=403)
    return None


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

SESSION_DOMAIN = None
if CANONICAL_HOST and CANONICAL_HOST != "localhost" and not re.match(r"^\\d+\\.\\d+\\.\\d+\\.\\d+$", CANONICAL_HOST):
    SESSION_DOMAIN = CANONICAL_HOST


def get_telethon_lock() -> asyncio.Lock:
    global _telethon_lock
    if _telethon_lock is None:
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
    phone = CONTACT_PHONE.strip()
    if not phone:
        phone = input("Введите телефон (+7999...): ").strip()
    if not phone:
        return False
    sent = await client.send_code_request(phone)
    code = input("Код из Telegram: ").strip()
    password = None
    if TELETHON_PASSWORD:
        password = TELETHON_PASSWORD
    if not password and "PASSWORD" in os.environ:
        password = os.environ.get("PASSWORD")
    try:
        await client.sign_in(phone=phone, code=code, password=password, phone_code_hash=sent.phone_code_hash)
    except SessionPasswordNeededError:
        if TELETHON_PASSWORD:
            await client.sign_in(password=TELETHON_PASSWORD)
        else:
            raise
    return await client.is_user_authorized()


async def telethon_login_cli() -> None:
    client = await get_telethon_client()
    if not client:
        print("Telethon не настроен")
        return
    ok = await ensure_telethon_login(client, interactive=True)
    if ok:
        me = await client.get_me()
        print(f"Telethon login ok: {me.id} {me.username}")
    else:
        print("Telethon login failed")


def normalize_phone(value: str) -> str:
    value = re.sub(r"\D", "", value or "")
    if value.startswith("8"):
        value = "7" + value[1:]
    if value.startswith("7") and len(value) == 11:
        return value
    return value


def build_phone_link(value: str) -> Optional[str]:
    if not value:
        return None
    if value.startswith("+"):
        return f"tel:{value}"
    if value.startswith("7"):
        return f"tel:+{value}"
    return f"tel:{value}"


def normalize_telegram(handle: str) -> Optional[str]:
    value = (handle or "").strip()
    if not value:
        return None
    if value.startswith("https://") or value.startswith("http://"):
        return value
    return value.lstrip("@")


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


templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


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


if __name__ == "__main__":
    if "--telethon-login" in sys.argv:
        asyncio.run(telethon_login_cli())
    else:
        logging.getLogger(__name__).warning("core.py is not intended to be run directly")
