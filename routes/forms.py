import logging
import time

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from starlette.status import HTTP_302_FOUND

from core import get_current_user, load_metrics, render, save_agreement, save_lead, save_metrics
from telegram_bot import is_configured as telegram_is_configured
from telegram_bot import send_lead_message

router = APIRouter()


@router.post("/apply", include_in_schema=False)
async def apply(request: Request):
    form = await request.form()
    name = str(form.get("name", "")).strip()
    contact = str(form.get("phone", "")).strip()
    course = str(form.get("course", "")).strip()
    page = request.headers.get("referer", "")

    lead_payload = {
        "timestamp": int(time.time()),
        "name": name,
        "contact": contact,
        "course": course,
        "page": page,
        "user": get_current_user(request),
    }
    save_lead(lead_payload)
    metrics = load_metrics()
    metrics["funnel"]["apply"] = metrics["funnel"].get("apply", 0) + 1
    save_metrics(metrics)

    if telegram_is_configured():
        text = (
            "🆕 <b>Новая заявка</b>\n"
            f"👤 <b>Имя:</b> {name or '—'}\n"
            f"📱 <b>Контакт:</b> {contact or '—'}\n"
            f"🎯 <b>Курс:</b> {course or '—'}\n"
            f"🔗 <b>Страница:</b> {page or '—'}"
        )
        try:
            sent = await send_lead_message(text)
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

    return render(request, "enroll_success.html", {"course": payload.get("course")})
