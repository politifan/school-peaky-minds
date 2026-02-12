import logging
import time
from typing import Optional, Dict, Tuple

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.status import HTTP_302_FOUND

from core import (
    EXECUTOR_EMAIL,
    TELEGRAM_USERNAME_RE,
    find_student_by_code,
    find_student_by_phone,
    is_valid_phone,
    load_metrics,
    load_referrals,
    normalize_telegram,
    normalize_phone,
    normalize_referral_code,
    render,
    save_agreement,
    save_lead,
    save_metrics,
    save_referrals,
    next_student_id,
    get_current_user,
    send_email_message,
)
from telegram_bot import is_configured as telegram_is_configured
from telegram_bot import send_lead_message

router = APIRouter()

# Simple in-memory rate limits (per-process).
_RATE_LIMIT = {
    "apply": {"daily": 5, "window": 10},
    "enroll": {"daily": 3, "window": 10},
}
_rate_daily: Dict[Tuple[str, str], Dict[str, object]] = {}
_rate_burst: Dict[Tuple[str, str], float] = {}


def _get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("cf-connecting-ip") or request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _check_rate_limit(request: Request, key: str) -> Optional[str]:
    limits = _RATE_LIMIT.get(key)
    if not limits:
        return None
    now = int(time.time())
    ip = _get_client_ip(request)
    day_key = time.strftime("%Y-%m-%d", time.gmtime(now))

    daily_key = (key, ip)
    entry = _rate_daily.get(daily_key)
    if not entry or entry.get("day") != day_key:
        entry = {"day": day_key, "count": 0}
        _rate_daily[daily_key] = entry
    if int(entry.get("count", 0)) >= int(limits["daily"]):
        return "daily"

    burst_key = (key, ip)
    last_ts = _rate_burst.get(burst_key)
    if last_ts and (now - int(last_ts)) < int(limits["window"]):
        return "burst"

    entry["count"] = int(entry.get("count", 0)) + 1
    _rate_burst[burst_key] = now
    return None


def _render_rate_limit_page(request: Request, flow: str, reason: str) -> HTMLResponse:
    title = "\u0417\u0430\u043f\u0440\u043e\u0441\u044b \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d\u044b"
    message = "\u041c\u044b \u0437\u0430\u043c\u0435\u0442\u0438\u043b\u0438 \u043f\u043e\u0434\u043e\u0437\u0440\u0438\u0442\u0435\u043b\u044c\u043d\u044b\u0439 \u0442\u0440\u0430\u0444\u0438\u043a \u0438\u0437 \u0432\u0430\u0448\u0435\u0439 \u0441\u0435\u0442\u0438. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u043e\u0441\u0442\u0430\u0432\u0438\u0442\u044c \u0437\u0430\u044f\u0432\u043a\u0443 \u043d\u0435\u043c\u043d\u043e\u0433\u043e \u043f\u043e\u0437\u0436\u0435."
    hint = "\u041e\u0431\u044b\u0447\u043d\u043e \u0434\u043e\u0441\u0442\u0443\u043f \u0432\u043e\u0441\u0441\u0442\u0430\u043d\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u0442\u0441\u044f \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438."
    if reason == "burst":
        hint = "\u041f\u043e\u0434\u043e\u0436\u0434\u0438\u0442\u0435 10 \u0441\u0435\u043a\u0443\u043d\u0434 \u0438 \u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u043f\u043e\u043f\u044b\u0442\u043a\u0443."
    response = render(
        request,
        "enroll_limit.html",
        {"limit_title": title, "limit_message": message, "limit_hint": hint, "limit_flow": flow},
    )
    response.status_code = 429
    return response


def clamp_text(value: object, max_len: int) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return text[:max_len]


def is_valid_email(value: object) -> bool:
    if value is None:
        return False
    email = str(value).strip().lower()
    if not email or "@" not in email:
        return False
    parts = email.split("@")
    if len(parts) != 2:
        return False
    local, domain = parts
    if not local or not domain:
        return False
    if len(domain) < 4:
        return False
    if domain.count(".") != 1:
        return False
    if domain.startswith(".") or domain.endswith("."):
        return False
    return True


def _upsert_student_from_enroll(payload: dict, referral_code: str, referrer_id: Optional[int]) -> int:
    data = load_referrals()
    students = data.get("students") or {}
    phone = str(payload.get("phone") or "").strip()
    existing = find_student_by_phone(students, phone)
    now_ts = int(time.time())

    if existing:
        student = existing
    else:
        student_id = next_student_id(students)
        student = {
            "id": student_id,
            "created_at": now_ts,
            "months": {},
            "discount_applied": [],
        }
        students[str(student_id)] = student

    student["name"] = payload.get("full_name") or student.get("name") or ""
    student["phone"] = phone
    student["phone_norm"] = normalize_phone(phone)
    student["email"] = payload.get("email") or student.get("email") or ""
    student["telegram"] = payload.get("telegram") or student.get("telegram") or ""
    student["course"] = payload.get("course") or student.get("course") or ""
    student["updated_at"] = now_ts

    if referral_code and referrer_id:
        if not student.get("referrer_id"):
            student["referrer_id"] = referrer_id
            student["referrer_code"] = referral_code
            student["referrer_assigned_at"] = now_ts

    data["students"] = students
    save_referrals(data)
    return int(student.get("id") or 0)


@router.post("/apply", include_in_schema=False)
async def apply(request: Request):
    rate_error = _check_rate_limit(request, "apply")
    if rate_error:
        return _render_rate_limit_page(request, "apply", rate_error)
    form = await request.form()
    name = clamp_text(form.get("name", ""), 60)
    contact = str(form.get("phone", "")).strip()
    telegram_raw = str(form.get("telegram", "")).strip()
    course = str(form.get("course", "")).strip()
    if telegram_raw and course and telegram_raw.strip().lower() == course.lower():
        telegram_raw = ""
    if telegram_raw.strip().lower() == "main":
        telegram_raw = ""
    telegram_norm = normalize_telegram(telegram_raw) or ""
    telegram_display = ""
    if telegram_norm:
        if telegram_norm.startswith("http://") or telegram_norm.startswith("https://"):
            telegram_display = telegram_norm
        else:
            if not TELEGRAM_USERNAME_RE.match(telegram_norm):
                return HTMLResponse("Некорректный Telegram", status_code=400)
            telegram_display = f"@{telegram_norm}"
    if not contact and not telegram_display:
        return HTMLResponse("Укажите телефон или Telegram", status_code=400)
    if contact and not is_valid_phone(contact):
        return HTMLResponse("Некорректный телефон", status_code=400)
    page = request.headers.get("referer", "")

    lead_payload = {
        "timestamp": int(time.time()),
        "name": name,
        "contact": contact,
        "telegram": telegram_display,
        "course": course,
        "page": page,
        "user": get_current_user(request),
    }
    lead_path = save_lead(lead_payload)
    metrics = load_metrics()
    metrics["funnel"]["apply"] = metrics["funnel"].get("apply", 0) + 1
    save_metrics(metrics)

    if telegram_is_configured():
        text = (
            "🆕 <b>Новая заявка</b>\n"
            f"👤 <b>Имя:</b> {name or '—'}\n"
            f"📱 <b>Контакт:</b> {contact or '—'}\n"
            f"💬 <b>Telegram:</b> {telegram_display or '—'}\n"
            f"🎯 <b>Курс:</b> {course or '—'}\n"
            f"🔗 <b>Страница:</b> {page or '—'}"
        )
        try:
            sent = await send_lead_message(text, lead_file=lead_path.name)
            if not sent:
                logging.getLogger("app.telegram").warning("Telegram lead message not delivered.")
        except Exception as exc:
            logging.getLogger("app.telegram").error("Telegram lead send failed: %s", exc)

    return render(request, "success.html", {"course": course})


@router.post("/enroll", include_in_schema=False)
async def enroll(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=HTTP_302_FOUND)
    rate_error = _check_rate_limit(request, "enroll")
    if rate_error:
        return _render_rate_limit_page(request, "enroll", rate_error)

    form = await request.form()
    email = str(form.get("email") or "").strip().lower()
    if not is_valid_email(email):
        return HTMLResponse("Некорректный email", status_code=400)
    referral_code_raw = str(form.get("referral_code") or "").strip()
    referral_code = normalize_referral_code(referral_code_raw)
    if referral_code and len(referral_code) > 16:
        return HTMLResponse("Реферальный код слишком длинный", status_code=400)
    payload = {
        "timestamp": int(time.time()),
        "user": user,
        "course": form.get("course"),
        "full_name": clamp_text(form.get("full_name"), 60),
        "phone": form.get("phone"),
        "email": email,
        "telegram": form.get("telegram"),
        "agreement": form.get("agreement"),
        "consent": form.get("consent"),
    }
    phone_raw = str(payload.get("phone") or "").strip()
    telegram_raw = str(payload.get("telegram") or "").strip()
    course = str(payload.get("course") or "").strip()
    if telegram_raw and course and telegram_raw.strip().lower() == course.lower():
        telegram_raw = ""
    if telegram_raw.strip().lower() == "main":
        telegram_raw = ""
    telegram_norm = normalize_telegram(telegram_raw) or ""
    if telegram_norm:
        if telegram_norm.startswith("http://") or telegram_norm.startswith("https://"):
            payload["telegram"] = telegram_norm
        else:
            if not TELEGRAM_USERNAME_RE.match(telegram_norm):
                return HTMLResponse("Некорректный Telegram", status_code=400)
            payload["telegram"] = f"@{telegram_norm}"
    else:
        payload["telegram"] = ""
    if not phone_raw and not payload.get("telegram"):
        return HTMLResponse("Укажите телефон или Telegram", status_code=400)
    if phone_raw and not is_valid_phone(phone_raw):
        return HTMLResponse("Некорректный телефон", status_code=400)

    referrer = None
    referrer_id = None
    if referral_code:
        referral_data = load_referrals()
        students = referral_data.get("students") or {}
        referrer = find_student_by_code(students, referral_code)
        if not referrer:
            return HTMLResponse("Неверный реферальный код", status_code=400)
        referrer_id = referrer.get("id")
        existing = find_student_by_phone(students, payload.get("phone"))
        if existing and existing.get("referrer_id") and existing.get("referrer_id") != referrer_id:
            return HTMLResponse("Реферальный код уже применён", status_code=400)
        payload["referral_code"] = referral_code
        payload["referrer_id"] = referrer_id

    student_id = _upsert_student_from_enroll(payload, referral_code, referrer_id)
    if student_id:
        payload["student_id"] = student_id

    save_agreement(payload)
    metrics = load_metrics()
    metrics["funnel"]["enroll"] = metrics["funnel"].get("enroll", 0) + 1
    save_metrics(metrics)

    if telegram_is_configured():
        text = (
            "✅ <b>Заявка на покупку курса</b>\n"
            f"🎯 <b>Курс:</b> {payload.get('course')}\n"
            f"👤 <b>ФИО:</b> {payload.get('full_name')}\n"
            f"📞 <b>Телефон:</b> {payload.get('phone')}\n"
            f"✉️ <b>Email:</b> {payload.get('email')}\n"
            f"💬 <b>Telegram:</b> {payload.get('telegram')}"
        )
        try:
            sent = await send_lead_message(text)
            if not sent:
                logging.getLogger("app.telegram").warning("Telegram enroll message not delivered.")
        except Exception as exc:
            logging.getLogger("app.telegram").error("Telegram enroll send failed: %s", exc)

    if referral_code and referrer:
        notify_text = (
            "🤝 <b>Новый реферал</b>\n"
            f"👤 <b>Реферал:</b> {payload.get('full_name') or '—'}\n"
            f"📞 <b>Телефон:</b> {payload.get('phone') or '—'}\n"
            f"🎯 <b>Курс:</b> {payload.get('course') or '—'}\n"
            f"🔗 <b>Код:</b> {referral_code}\n"
            f"🏷 <b>Участник:</b> {referrer.get('name') or '—'}"
        )
        try:
            sent = await send_lead_message(notify_text)
            if not sent and EXECUTOR_EMAIL:
                try:
                    send_email_message(EXECUTOR_EMAIL, "Новый реферал", notify_text.replace("<b>", "").replace("</b>", ""))
                except Exception as exc:
                    logging.getLogger("app.telegram").warning("Referral email notify failed: %s", exc)
        except Exception as exc:
            logging.getLogger("app.telegram").warning("Referral telegram notify failed: %s", exc)

    return render(request, "enroll_success.html", {"course": payload.get("course")})
