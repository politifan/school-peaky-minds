import asyncio
import getpass
import hashlib
import hmac
import html as html_lib
import json
import logging
import os
import re
import secrets
import smtplib
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import httpx

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
CONTRACTS_DIR = DOCUMENTS_DIR / "contracts"
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
REFERRALS_FILE = DATA_DIR / "referrals.json"
PAYMENTS_FILE = DATA_DIR / "payments.json"

DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
CONTRACTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

SESSION_SECRET = os.getenv("SESSION_SECRET", "dev-secret-key")

CONTRACT_KEY_POINTS = [
    "Предоставление образовательных услуг по выбранному курсу.",
    "Фиксированная стоимость и порядок оплаты занятий.",
    "Права и обязанности сторон (ученик и исполнитель).",
    "Политика возвратов и переносов занятий.",
    "Обработка персональных данных и согласие.",
    "Формат и сроки обучения по программе курса.",
]

CONTRACT_DOCUMENTS = [
    ("Договор", "/documents/dogovor.pdf"),
]

CONTRACT_STATUS_META = {
    "draft": ("Черновик", "status-muted"),
    "sent": ("Отправлен", "status-warm"),
    "signed": ("Подписан", "status-good"),
}
MOSCOW_TZ = timezone(timedelta(hours=3))
MONTHS_RU = (
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


def generate_contract_token() -> str:
    return secrets.token_urlsafe(24)


def ensure_contract_fields(data: Dict[str, Any], path: Optional[Path] = None) -> Dict[str, Any]:
    changed = False
    token = (data.get("contract_token") or "").strip()
    if not token:
        data["contract_token"] = generate_contract_token()
        changed = True
    status = (data.get("contract_status") or "").strip()
    if status not in CONTRACT_STATUS_META:
        data["contract_status"] = "draft"
        changed = True
    if changed and path:
        save_json(path, data)
    return data


def contract_status_from_item(item: Dict[str, Any]) -> Tuple[str, str, str]:
    status = (item.get("contract_status") or "").strip()
    if status not in CONTRACT_STATUS_META:
        status = "draft"
    label, cls = CONTRACT_STATUS_META[status]
    return status, label, cls


def build_contract_url(token: str, request: Optional[Request] = None) -> str:
    if not token:
        return ""
    if APP_BASE_URL:
        return f"{APP_BASE_URL.rstrip('/')}/contract/{token}"
    if request:
        base = str(request.base_url).rstrip("/")
        return f"{base}/contract/{token}"
    return f"/contract/{token}"


def moscow_now() -> datetime:
    return datetime.now(MOSCOW_TZ)


def format_moscow_date(dt: Optional[datetime] = None) -> str:
    value = dt or moscow_now()
    month_name = MONTHS_RU[value.month - 1]
    return f"«{value.day:02d}» {month_name} {value.year} г."


def course_rate(course: Optional[str]) -> Optional[int]:
    value = (course or "").strip().lower()
    if not value:
        return None
    if "full" in value:
        return 1500
    if "data" in value or "science" in value or "аналитик" in value:
        return 2000
    if "business" in value or "бизнес" in value or "автоматизац" in value:
        return 2000
    if "python" in value:
        return 1000
    return None


def resolve_contract_fields(agreement: Dict[str, Any]) -> Dict[str, str]:
    fields = agreement.get("contract_fields") or {}
    user = agreement.get("user") or {}
    return {
        "city": str(fields.get("city") or "Москва").strip(),
        "customer_name": str(
            fields.get("customer_name")
            or agreement.get("full_name")
            or user.get("name")
            or ""
        ).strip(),
        "customer_passport": str(fields.get("customer_passport") or "").strip(),
        "customer_address": str(fields.get("customer_address") or "").strip(),
        "customer_phone": str(
            fields.get("customer_phone") or agreement.get("phone") or ""
        ).strip(),
        "customer_email": str(
            fields.get("customer_email") or agreement.get("email") or user.get("email") or ""
        ).strip(),
    }


def contract_missing_fields(agreement: Dict[str, Any]) -> List[str]:
    fields = resolve_contract_fields(agreement)
    required = {
        "city": "Город договора",
        "customer_name": "ФИО заказчика",
        "customer_passport": "Паспорт заказчика",
        "customer_address": "Адрес заказчика",
        "customer_phone": "Телефон заказчика",
        "customer_email": "Email заказчика",
    }
    missing = []
    for key, label in required.items():
        if not fields.get(key):
            missing.append(label)
    return missing


def _contract_pdf_template_path() -> Path:
    raw = os.getenv("CONTRACT_PDF_TEMPLATE", "").strip()
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = BASE_DIR / raw
        return path
    return DOCUMENTS_DIR / "dogovor.pdf"


def _normalize_pdf_field_name(name: str) -> str:
    return re.sub(r"[^a-z0-9а-я]+", "", (name or "").lower())


def _contract_pdf_values(agreement: Dict[str, Any]) -> Dict[str, str]:
    fields = resolve_contract_fields(agreement)
    contract_number = str(agreement.get("contract_number") or "").strip() or "—"
    contract_date = str(agreement.get("contract_date") or "").strip() or format_moscow_date()
    course = str(agreement.get("course") or "—").strip()
    rate = course_rate(course)
    rate_text = f"{rate} руб./час" if rate else "—"
    return {
        "contract_number": contract_number,
        "contract_date": contract_date,
        "city": fields["city"] or "—",
        "course": course,
        "rate": rate_text,
        "customer_name": fields["customer_name"] or "—",
        "customer_passport": fields["customer_passport"] or "—",
        "customer_address": fields["customer_address"] or "—",
        "customer_phone": fields["customer_phone"] or "—",
        "customer_email": fields["customer_email"] or "—",
        "executor_name": EXECUTOR_FULL_NAME,
        "executor_inn": EXECUTOR_INN,
        "executor_passport": EXECUTOR_PASSPORT,
        "executor_address": EXECUTOR_ADDRESS or "—",
        "executor_phone": EXECUTOR_PHONE,
        "executor_email": EXECUTOR_EMAIL,
        "executor_recipient": EXECUTOR_RECIPIENT,
        "executor_bank": EXECUTOR_BANK,
        "executor_account": EXECUTOR_ACCOUNT,
        "executor_sbp_phone": EXECUTOR_SBP_PHONE,
    }


def _contract_pdf_field_map(values: Dict[str, str]) -> Dict[str, str]:
    raw = os.getenv("CONTRACT_PDF_FIELD_MAP", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        logging.getLogger("app.contract").warning("Invalid CONTRACT_PDF_FIELD_MAP JSON.")
        return {}
    if not isinstance(data, dict):
        return {}
    known_keys = set(values)
    if set(data.keys()) & known_keys:
        return {str(field_name): values[key] for key, field_name in data.items() if key in values}
    return {str(field_name): values[key] for field_name, key in data.items() if key in values}


def _match_contract_pdf_key(field_name: str, values: Dict[str, str]) -> Optional[str]:
    name = _normalize_pdf_field_name(field_name)
    if not name:
        return None

    if "contractnumber" in name or "номердоговора" in name or "номердог" in name or name == "номер":
        return "contract_number"
    if "contractdate" in name or "датадоговора" in name or name == "дата":
        return "contract_date"
    if "city" in name or "город" in name:
        return "city"
    if "program" in name or "course" in name or "программа" in name:
        return "course"
    if "rate" in name or "ставка" in name:
        return "rate"

    if "executor" in name or "исполнитель" in name:
        if "inn" in name or "инн" in name:
            return "executor_inn"
        if "passport" in name or "паспорт" in name:
            return "executor_passport"
        if "address" in name or "адрес" in name:
            return "executor_address"
        if "phone" in name or "тел" in name:
            return "executor_phone"
        if "email" in name or "почт" in name:
            return "executor_email"
        if "bank" in name or "банк" in name:
            return "executor_bank"
        if "account" in name or "счет" in name or "счёт" in name or "карта" in name:
            return "executor_account"
        if "recipient" in name or "получатель" in name:
            return "executor_recipient"
        if "sbp" in name or "сбп" in name:
            return "executor_sbp_phone"
        if "fio" in name or "фио" in name or "name" in name:
            return "executor_name"

    if "customer" in name or "заказчик" in name or "client" in name:
        if "passport" in name or "паспорт" in name:
            return "customer_passport"
        if "address" in name or "адрес" in name:
            return "customer_address"
        if "phone" in name or "тел" in name:
            return "customer_phone"
        if "email" in name or "почт" in name:
            return "customer_email"
        if "fio" in name or "фио" in name or "name" in name:
            return "customer_name"

    if "passport" in name or "паспорт" in name:
        return "customer_passport"
    if "address" in name or "адрес" in name:
        return "customer_address"
    if "phone" in name or "тел" in name:
        return "customer_phone"
    if "email" in name or "почт" in name:
        return "customer_email"
    if "fio" in name or "фио" in name or "name" in name:
        return "customer_name"
    return None


def _fill_contract_pdf_template(agreement: Dict[str, Any]) -> Optional[str]:
    template_path = _contract_pdf_template_path()
    if not template_path.exists():
        return None

    try:
        try:
            from pypdf import PdfReader, PdfWriter
        except Exception:  # pragma: no cover - fallback for older installs
            from PyPDF2 import PdfReader, PdfWriter
    except Exception:
        return None

    try:
        reader = PdfReader(str(template_path))
    except Exception:
        logging.getLogger("app.contract").warning("Failed to read contract PDF template.", exc_info=True)
        return None

    fields = reader.get_fields() or {}
    if not fields:
        return None

    values = _contract_pdf_values(agreement)
    field_values: Dict[str, str] = {}
    field_values.update(_contract_pdf_field_map(values))
    for field_name in fields.keys():
        if field_name in field_values:
            continue
        key = _match_contract_pdf_key(field_name, values)
        if key and key in values:
            field_values[field_name] = values[key]

    writer = PdfWriter()
    writer.append_pages_from_reader(reader)
    for page in writer.pages:
        try:
            writer.update_page_form_field_values(page, field_values)
        except Exception:
            pass
    try:
        writer.set_need_appearances_writer()
    except Exception:
        try:
            from PyPDF2.generic import BooleanObject, NameObject

            writer._root_object.update({NameObject("/NeedAppearances"): BooleanObject(True)})
        except Exception:
            pass

    contract_number = str(agreement.get("contract_number") or "draft")
    token = str(agreement.get("contract_token") or secrets.token_hex(8))
    file_name = f"contract_{contract_number}_{token}.pdf"
    file_path = CONTRACTS_DIR / file_name
    try:
        with open(file_path, "wb") as handle:
            writer.write(handle)
    except Exception:
        logging.getLogger("app.contract").warning("Failed to write filled contract PDF.", exc_info=True)
        return None
    return f"/documents/contracts/{file_name}"


def count_signed_contracts() -> int:
    total = 0
    for item in load_agreements():
        if (item.get("contract_status") or "").strip() == "signed":
            total += 1
    return total


def build_contract_document_text(agreement: Dict[str, Any]) -> str:
    template_path = DOCUMENTS_DIR / "dogovor.html"
    html = template_path.read_text(encoding="utf-8") if template_path.exists() else ""
    html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<br\\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)

    fields = resolve_contract_fields(agreement)
    contract_number = str(agreement.get("contract_number") or "").strip() or "—"
    contract_date = str(agreement.get("contract_date") or "").strip() or format_moscow_date()
    course = str(agreement.get("course") or "—").strip()
    rate = course_rate(course)
    rate_text = f"{rate} руб./час" if rate else "—"

    text = text.replace("ДОГОВОР № ___", f"ДОГОВОР № {contract_number}")
    text = re.sub(
        r"г\\.\\s*____________\\s+«__»\\s+____________\\s+\\d{4} г\\.",
        f"г. {fields['city']}    {contract_date}",
        text,
    )
    text = text.replace(
        "Исполнитель: _____________________________ (ФИО), ИНН ____________, применяющий(ая) специальный налоговый режим «Налог на профессиональный доход» (самозанятый), далее — «Исполнитель», с одной стороны, и",
        "Исполнитель: "
        f"{EXECUTOR_FULL_NAME} (ФИО), ИНН {EXECUTOR_INN}, "
        "применяющий(ая) специальный налоговый режим «Налог на профессиональный доход» (самозанятый), далее — «Исполнитель», с одной стороны, и",
    )
    text = text.replace(
        "Заказчик: _____________________________ (ФИО), паспорт: _____________________________, далее — «Заказчик», с другой стороны, вместе именуемые «Стороны», заключили настоящий Договор о нижеследующем.",
        "Заказчик: "
        f"{fields['customer_name']} (ФИО), паспорт: {fields['customer_passport']}, "
        "далее — «Заказчик», с другой стороны, вместе именуемые «Стороны», заключили настоящий Договор о нижеследующем.",
    )
    text = text.replace("1) Выбранная Программа: ___________________________", f"1) Выбранная Программа: {course}")
    text = text.replace("2) Ставка: ______ руб./час", f"2) Ставка: {rate_text}")

    def replace_once(source: str, old: str, new: str) -> str:
        return source.replace(old, new, 1)

    text = replace_once(text, "Получатель: ____________________________", f"Получатель: {EXECUTOR_RECIPIENT}")
    text = replace_once(text, "ИНН: ____________________________", f"ИНН: {EXECUTOR_INN}")
    text = replace_once(text, "Банк: ____________________________", f"Банк: {EXECUTOR_BANK}")
    text = replace_once(text, "Счёт / карта: ______________________", f"Счёт / карта: {EXECUTOR_ACCOUNT}")
    text = text.replace(
        f"Счёт / карта: {EXECUTOR_ACCOUNT}",
        f"Счёт / карта: {EXECUTOR_ACCOUNT}\nСБП: {EXECUTOR_SBP_PHONE}",
    )
    text = replace_once(
        text,
        "Назначение платежа: Оплата услуг по договору № ____",
        f"Назначение платежа: Оплата услуг по договору № {contract_number}",
    )

    text = replace_once(text, "ФИО: _______________________", f"ФИО: {EXECUTOR_FULL_NAME}")
    text = replace_once(text, "ИНН: _______________________", f"ИНН: {EXECUTOR_INN}")
    text = replace_once(text, "Паспорт: ___________________", f"Паспорт: {EXECUTOR_PASSPORT}")
    text = replace_once(text, "Адрес: _____________________", f"Адрес: {EXECUTOR_ADDRESS or '—'}")
    text = replace_once(text, "Тел.: _______________________", f"Тел.: {EXECUTOR_PHONE}")
    text = replace_once(text, "E-mail: _____________________", f"E-mail: {EXECUTOR_EMAIL}")

    text = replace_once(text, "Заказчик: ФИО: _______________________", f"Заказчик: ФИО: {fields['customer_name']}")
    text = replace_once(text, "Паспорт: ___________________", f"Паспорт: {fields['customer_passport']}")
    text = replace_once(text, "Адрес: _____________________", f"Адрес: {fields['customer_address'] or '—'}")
    text = replace_once(text, "Тел.: _______________________", f"Тел.: {fields['customer_phone'] or '—'}")
    text = replace_once(text, "E-mail: _____________________", f"E-mail: {fields['customer_email'] or '—'}")
    text = replace_once(
        text,
        "Подпись: _____________ /__________/",
        f"Подпись: _____________ /{EXECUTOR_FULL_NAME}/",
    )
    text = replace_once(
        text,
        "Подпись: _____________ /__________/",
        f"Подпись: _____________ /{fields['customer_name']}/",
    )

    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def _find_font_path() -> Optional[Path]:
    candidates = [
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/freefont/FreeSans.ttf"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def generate_contract_pdf(agreement: Dict[str, Any]) -> Optional[str]:
    pdf_url = _fill_contract_pdf_template(agreement)
    if pdf_url:
        return pdf_url
    try:
        from fpdf import FPDF
    except Exception:
        return None

    contract_number = str(agreement.get("contract_number") or "draft")
    token = str(agreement.get("contract_token") or secrets.token_hex(8))
    file_name = f"contract_{contract_number}_{token}.pdf"
    file_path = CONTRACTS_DIR / file_name

    text = build_contract_document_text(agreement)
    pdf = FPDF(format="A4")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    font_path = _find_font_path()
    if font_path:
        pdf.add_font("DejaVu", "", str(font_path), uni=True)
        pdf.set_font("DejaVu", size=11)
    else:
        pdf.set_font("Helvetica", size=11)
    max_width = pdf.w - pdf.l_margin - pdf.r_margin

    def _wrap_line(value: str) -> List[str]:
        if not value:
            return [""]
        if pdf.get_string_width(value) <= max_width:
            return [value]
        lines: List[str] = []
        current = ""
        for ch in value:
            candidate = f"{current}{ch}"
            if pdf.get_string_width(candidate) <= max_width or not current:
                current = candidate
            else:
                lines.append(current)
                current = ch
        if current:
            lines.append(current)
        return lines

    for line in text.split("\n"):
        if not line.strip():
            pdf.ln(4)
        else:
            for wrapped in _wrap_line(line):
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(0, 6, wrapped)
    pdf.output(str(file_path))
    return f"/documents/contracts/{file_name}"


def normalize_materials(value: Any) -> List[Dict[str, str]]:
    materials: List[Dict[str, str]] = []
    if not value:
        return materials
    if isinstance(value, str):
        raw_lines = value.splitlines()
    elif isinstance(value, list):
        raw_lines = value
    else:
        raw_lines = [value]
    for item in raw_lines:
        if isinstance(item, dict):
            url = str(item.get("url") or item.get("link") or item.get("href") or "").strip()
            title = str(item.get("title") or item.get("label") or "").strip()
        else:
            text = str(item).strip()
            if not text:
                continue
            if "|" in text:
                title, url = text.split("|", 1)
                title = title.strip()
                url = url.strip()
            else:
                title = ""
                url = text
        if not url:
            continue
        materials.append({"title": title, "url": url})
    return materials


def materials_to_text(materials: Any) -> str:
    normalized = normalize_materials(materials)
    lines = []
    for item in normalized:
        title = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()
        if not url:
            continue
        if title:
            lines.append(f"{title} | {url}")
        else:
            lines.append(url)
    return "\n".join(lines)


def load_json(path: Path, default: Any) -> Any:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_referral_code(value: str) -> str:
    raw = re.sub(r"\s+", "", value or "")
    return raw.upper()


def load_referrals() -> Dict[str, Any]:
    default = {"students": {}, "audit": []}
    data = load_json(REFERRALS_FILE, default)
    if not isinstance(data, dict):
        return default
    students = data.get("students")
    if isinstance(students, list):
        converted = {}
        for item in students:
            if not isinstance(item, dict):
                continue
            sid = item.get("id")
            if sid is None:
                continue
            converted[str(sid)] = item
        students = converted
    if not isinstance(students, dict):
        students = {}
    cleaned_students = {}
    for key, item in students.items():
        if not isinstance(item, dict):
            continue
        sid = item.get("id", key)
        if sid is None:
            continue
        cleaned_students[str(sid)] = item
    audit = data.get("audit")
    if not isinstance(audit, list):
        audit = []
    return {"students": cleaned_students, "audit": audit}


def save_referrals(data: Dict[str, Any]) -> None:
    payload = {"students": {}, "audit": []}
    if isinstance(data.get("students"), dict):
        payload["students"] = data["students"]
    if isinstance(data.get("audit"), list):
        payload["audit"] = data["audit"]
    save_json(REFERRALS_FILE, payload)


def load_payments() -> Dict[str, Any]:
    default = {"payments": {}, "events": []}
    data = load_json(PAYMENTS_FILE, default)
    if not isinstance(data, dict):
        return default
    payments = data.get("payments")
    if not isinstance(payments, dict):
        payments = {}
    events = data.get("events")
    if not isinstance(events, list):
        events = []
    return {"payments": payments, "events": events}


def save_payments(data: Dict[str, Any]) -> None:
    payload = {"payments": {}, "events": []}
    if isinstance(data.get("payments"), dict):
        payload["payments"] = data["payments"]
    if isinstance(data.get("events"), list):
        payload["events"] = data["events"]
    save_json(PAYMENTS_FILE, payload)


def referral_confirmed_months(student: Dict[str, Any]) -> int:
    months = student.get("months") or {}
    if not isinstance(months, dict):
        return 0
    total = 0
    for entry in months.values():
        if isinstance(entry, dict) and entry.get("paid") and entry.get("attended"):
            total += 1
    return total


def month_key(dt: Optional[datetime] = None) -> str:
    value = dt or moscow_now()
    return value.strftime("%Y-%m")


def _parse_month_key(value: str) -> Optional[Tuple[int, int]]:
    if not value or not re.match(r"^\d{4}-\d{2}$", value):
        return None
    try:
        year = int(value[:4])
        month = int(value[5:7])
    except Exception:
        return None
    if month < 1 or month > 12:
        return None
    return year, month


def referral_monthly_discounts(student: Dict[str, Any]) -> Dict[str, Any]:
    data = student.get("monthly_discounts") or {}
    if not isinstance(data, dict):
        return {}
    return data


def cleanup_monthly_discounts(student: Dict[str, Any], current_key: Optional[str] = None) -> bool:
    current_key = current_key or month_key()
    current_tuple = _parse_month_key(current_key)
    if not current_tuple:
        return False
    data = referral_monthly_discounts(student)
    if not data:
        return False
    changed = False
    for key in list(data.keys()):
        key_tuple = _parse_month_key(key)
        if not key_tuple:
            data.pop(key, None)
            changed = True
            continue
        if key_tuple < current_tuple:
            data.pop(key, None)
            changed = True
    if changed:
        student["monthly_discounts"] = data
        student["updated_at"] = int(time.time())
    return changed


def referral_monthly_percent(student: Dict[str, Any], key: str) -> int:
    data = referral_monthly_discounts(student)
    entry = data.get(key) or {}
    if isinstance(entry, dict):
        raw = entry.get("percent")
    else:
        raw = entry
    try:
        value = int(raw or 0)
    except Exception:
        return 0
    return max(value, 0)


def referral_effective_percent(student: Dict[str, Any], course: str, key: Optional[str] = None) -> int:
    base = referral_monthly_percent(student, key or month_key())
    if base <= 0:
        return 0
    primary = str(student.get("primary_course") or "").strip()
    if not primary:
        return base
    if str(course or "").strip() == primary:
        return base
    return max(int(round(base * 0.5)), 0)


def referral_reserved_total(student: Dict[str, Any], current_key: Optional[str] = None) -> int:
    data = referral_monthly_discounts(student)
    if not data:
        return 0
    current_key = current_key or month_key()
    current_tuple = _parse_month_key(current_key)
    total = 0
    for key, entry in data.items():
        key_tuple = _parse_month_key(key)
        if not key_tuple or not current_tuple:
            continue
        if key_tuple < current_tuple:
            continue
        if isinstance(entry, dict):
            raw = entry.get("percent")
        else:
            raw = entry
        try:
            value = int(raw or 0)
        except Exception:
            continue
        if value > 0:
            total += value
    return total


def referral_applied_total(student: Dict[str, Any]) -> int:
    applied = student.get("discount_applied") or []
    if not isinstance(applied, list):
        return 0
    total = 0
    for item in applied:
        if not isinstance(item, dict):
            continue
        try:
            total += int(item.get("percent") or 0)
        except Exception:
            continue
    return total


def referral_stats_for_referrer(referrer: Dict[str, Any], students: Dict[str, Any]) -> Dict[str, Any]:
    referrer_id = referrer.get("id")
    referrals = []
    confirmed = 0
    for student in students.values():
        if str(student.get("referrer_id")) == str(referrer_id):
            referrals.append(student)
            confirmed += referral_confirmed_months(student)
    earned = confirmed * 10
    applied = referral_applied_total(referrer)
    reserved = referral_reserved_total(referrer)
    raw_balance = earned - applied - reserved
    balance = max(raw_balance, 0)
    overflow = max(balance - 100, 0)
    balance = min(balance, 100)
    return {
        "referrals": referrals,
        "referrals_count": len(referrals),
        "confirmed_months": confirmed,
        "earned": earned,
        "applied": applied,
        "reserved": reserved,
        "balance": balance,
        "overflow": overflow,
    }


def next_student_id(students: Dict[str, Any]) -> int:
    max_id = 0
    for key, item in students.items():
        try:
            value = int(item.get("id", key))
        except Exception:
            continue
        if value > max_id:
            max_id = value
    return max_id + 1


def find_student_by_phone(students: Dict[str, Any], phone: str) -> Optional[Dict[str, Any]]:
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    for item in students.values():
        if normalize_phone(item.get("phone", "")) == normalized:
            return item
    return None


def find_student_by_code(students: Dict[str, Any], code: str) -> Optional[Dict[str, Any]]:
    normalized = normalize_referral_code(code)
    if not normalized:
        return None
    for item in students.values():
        if normalize_referral_code(item.get("referral_code", "")) == normalized:
            return item
    return None


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


def update_agreement_contract_status(file_name: str, status: str) -> bool:
    if not file_name:
        return False
    path = AGREEMENTS_DIR / file_name
    if not path.exists():
        return False
    data = load_json(path, {})
    if not isinstance(data, dict):
        return False
    if status and status in CONTRACT_STATUS_META:
        data["contract_status"] = status
    else:
        data["contract_status"] = "draft"
    save_json(path, data)
    return True


def load_agreements() -> List[Dict[str, Any]]:
    items = []
    for path in sorted(AGREEMENTS_DIR.glob("agreement_*.json")):
        data = load_json(path, {})
        if isinstance(data, dict):
            ensure_contract_fields(data, path)
            data["_file"] = path.name
            items.append(data)
    return sorted(items, key=lambda item: item.get("timestamp", 0), reverse=True)


def find_agreement_by_token(token: str) -> Tuple[Optional[Dict[str, Any]], Optional[Path]]:
    value = (token or "").strip()
    if not value:
        return None, None
    for path in AGREEMENTS_DIR.glob("agreement_*.json"):
        data = load_json(path, {})
        if not isinstance(data, dict):
            continue
        if data.get("contract_token") == value:
            ensure_contract_fields(data, path)
            data["_file"] = path.name
            return data, path
    return None, None


def load_metrics() -> dict:
    default = {
        "total_visits": 0,
        "unique_visits": 0,
        "unique_ids": {},
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
    if not isinstance(data.get("unique_ids"), dict):
        data["unique_ids"] = {}
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


def send_email_message(recipient: str, subject: str, body: str) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    smtp_from = os.getenv("SMTP_FROM", smtp_user or "no-reply@example.com")

    if not smtp_host or not smtp_user or not smtp_password:
        raise RuntimeError("SMTP не настроен для отправки письма")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = recipient
    msg.set_content(body)

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
VK_MESSAGE_TOKEN = os.getenv("VK_MESSAGE_TOKEN")
VK_API_VERSION = os.getenv("VK_API_VERSION", "5.131")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME")
APP_BASE_URL = os.getenv("APP_BASE_URL")
CONTACT_PHONE = os.getenv("CONTACT_PHONE", "")
CONTACT_TELEGRAM = os.getenv("CONTACT_TELEGRAM", "")
CONTACT_VK = os.getenv("CONTACT_VK", "")
EXECUTOR_FULL_NAME = os.getenv("EXECUTOR_FULL_NAME", "Павлов Михаил Александрович")
EXECUTOR_INN = os.getenv("EXECUTOR_INN", "344597209940")
EXECUTOR_PASSPORT = os.getenv("EXECUTOR_PASSPORT", "1820699537")
EXECUTOR_PHONE = os.getenv("EXECUTOR_PHONE", "7 995 028 29 40")
EXECUTOR_EMAIL = os.getenv("EXECUTOR_EMAIL", "mihailpavlov042006@gmail.com")
EXECUTOR_RECIPIENT = os.getenv("EXECUTOR_RECIPIENT", "Исполнитель")
EXECUTOR_BANK = os.getenv("EXECUTOR_BANK", "Ozon")
EXECUTOR_ACCOUNT = os.getenv("EXECUTOR_ACCOUNT", "2204320674292448")
EXECUTOR_SBP_PHONE = os.getenv("EXECUTOR_SBP_PHONE", "7 968 287 29 40")
EXECUTOR_ADDRESS = os.getenv("EXECUTOR_ADDRESS", "Волоград, Полоненко 10")
SEO_GOOGLE_VERIFICATION = os.getenv("SEO_GOOGLE_VERIFICATION", "")
SEO_YANDEX_VERIFICATION = os.getenv("SEO_YANDEX_VERIFICATION", "")
SEO_BING_VERIFICATION = os.getenv("SEO_BING_VERIFICATION", "")
GA_MEASUREMENT_ID = os.getenv("GA_MEASUREMENT_ID", "")
YANDEX_METRIKA_ID = os.getenv("YANDEX_METRIKA_ID", "")
TELETHON_API_ID = os.getenv("TG_API_ID") or os.getenv("TELETHON_API_ID")
TELETHON_API_HASH = os.getenv("TG_API_HASH") or os.getenv("TELETHON_API_HASH")
TELETHON_PASSWORD = os.getenv("TG_PASSWORD") or os.getenv("TELETHON_PASSWORD")
TELETHON_SESSION_PATH = (
    os.getenv("SESSION_PATH")
    or os.getenv("TELETHON_SESSION_PATH")
    or str((DATA_DIR / "telethon.session").resolve())
)
TELETHON_AUTO_LOGIN = os.getenv("TELETHON_AUTO_LOGIN", "").lower() in {"1", "true", "yes"}
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
YOOKASSA_ENABLED = bool(YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY)

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


def contract_channel_label(channel: Optional[str]) -> str:
    mapping = {"email": "Email", "telegram": "Telegram", "vk": "VK"}
    return mapping.get(channel or "", "—")


def default_contract_channel(user: Optional[Dict[str, Any]]) -> str:
    provider = (user or {}).get("provider") or ""
    if provider in {"email", "google"}:
        return "email"
    if provider == "telegram":
        return "telegram"
    if provider == "vk":
        return "vk"
    return "email"


def resolve_contact_email(user: Optional[Dict[str, Any]], agreement: Optional[Dict[str, Any]], override: str = "") -> Optional[str]:
    if override:
        return override.strip()
    if agreement:
        email = (agreement.get("email") or "").strip()
        if email:
            return email
    if user:
        email = (user.get("email") or "").strip()
        if email:
            return email
    return None


def extract_telegram_chat_id(user: Optional[Dict[str, Any]], agreement: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if user and user.get("provider") == "telegram":
        raw = str(user.get("id") or "")
        if raw.startswith("telegram:"):
            return raw.split("telegram:", 1)[1]
    if agreement:
        handle = str(agreement.get("telegram") or "").strip()
        if handle:
            handle = handle.replace("https://t.me/", "").replace("http://t.me/", "")
            handle = handle.lstrip("@")
            if handle:
                return f"@{handle}"
    return None


def extract_vk_user_id(user: Optional[Dict[str, Any]]) -> Optional[str]:
    if not user or user.get("provider") != "vk":
        return None
    raw = str(user.get("id") or "")
    if raw.startswith("vk:"):
        return raw.split("vk:", 1)[1]
    return None


async def send_telegram_message(chat_id: str, text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("Telegram бот не настроен")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(url, data=payload)
    data = {}
    try:
        data = resp.json()
    except Exception:
        data = {}
    if not resp.is_success or not data.get("ok"):
        error = data.get("description") or resp.text
        raise RuntimeError(f"Telegram send failed: {error}")
    return True


async def send_vk_message(user_id: str, text: str) -> bool:
    if not VK_MESSAGE_TOKEN:
        raise RuntimeError("VK отправка не настроена")
    params = {
        "access_token": VK_MESSAGE_TOKEN,
        "v": VK_API_VERSION,
        "user_id": user_id,
        "message": text,
        "random_id": secrets.randbelow(1_000_000_000),
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post("https://api.vk.com/method/messages.send", params=params)
    data = {}
    try:
        data = resp.json()
    except Exception:
        data = {}
    if "error" in data:
        raise RuntimeError(data["error"].get("error_msg") or "VK send failed")
    return True


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
    # Try QR login first (if supported by Telethon)
    try:
        qr_login = await client.qr_login()
        if qr_login:
            print("Отсканируйте QR-код в Telegram (Настройки -> Устройства -> Подключить устройство).")
            _print_qr_ascii(qr_login.url)
            await qr_login.wait()
            if await client.is_user_authorized():
                return True
    except Exception:
        pass
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
    data = dict(payload)
    if not data.get("contract_token"):
        data["contract_token"] = generate_contract_token()
    if (data.get("contract_status") or "").strip() not in CONTRACT_STATUS_META:
        data["contract_status"] = "draft"
    save_json(path, data)
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

    user = get_current_user(request)
    avatar_url = ""
    avatar_initial = ""
    avatar_variant = "default"
    if user:
        raw_url = (user.get("avatar_url") or user.get("photo_url") or "").strip()
        avatar_url = raw_url
        provider = user.get("provider") or ""
        if provider == "email":
            avatar_variant = "email"
            email = (user.get("email") or "").strip()
            if email:
                avatar_initial = email[0].upper()
        if not avatar_initial:
            name = (user.get("name") or "").strip()
            if name:
                avatar_initial = name[0].upper()
        if not avatar_initial and user.get("email"):
            avatar_initial = str(user.get("email"))[0].upper()
    if not avatar_initial:
        avatar_initial = "?"

    ctx = {
        "request": request,
        "user": user,
        "user_avatar": {
            "url": avatar_url,
            "initial": avatar_initial,
            "variant": avatar_variant,
        },
        "contact_phone": phone,
        "contact_phone_link": phone_link,
        "contact_telegram": telegram_display,
        "contact_telegram_link": telegram_link,
        "contact_vk": vk_display,
        "contact_vk_link": vk_link,
        "seo_google_verification": SEO_GOOGLE_VERIFICATION,
        "seo_yandex_verification": SEO_YANDEX_VERIFICATION,
        "seo_bing_verification": SEO_BING_VERIFICATION,
        "ga_measurement_id": GA_MEASUREMENT_ID,
        "yandex_metrika_id": YANDEX_METRIKA_ID,
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
