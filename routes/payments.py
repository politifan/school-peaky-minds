import hashlib
import logging
import secrets
import time
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

import core
from core import (
    AGREEMENTS_DIR,
    APP_BASE_URL,
    admin_required,
    TINKOFF_ENABLED,
    TINKOFF_PASSWORD,
    TINKOFF_TERMINAL_KEY,
    course_rate,
    find_student_by_phone,
    get_current_user,
    load_agreements,
    load_json,
    load_payments,
    load_referrals,
    month_key,
    normalize_phone,
    referral_effective_percent,
    referral_stats_for_referrer,
    render,
    save_json,
    save_payments,
    save_referrals,
)

router = APIRouter()

TINKOFF_API_URL = "https://securepay.tinkoff.ru/v2"


def _amount_value(amount: Decimal) -> str:
    return str(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _format_ts(value: Optional[int]) -> str:
    if not value:
        return "-"
    try:
        return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "-"


def _append_event(data: Dict[str, Any], entry: Dict[str, Any]) -> None:
    events = data.get("events") if isinstance(data.get("events"), list) else []
    events.append(entry)
    if len(events) > 50:
        events = events[-50:]
    data["events"] = events


def _build_return_url(request: Request) -> str:
    base = (APP_BASE_URL or str(request.base_url)).rstrip("/")
    return f"{base}/account?payment=return"


def _build_test_return_url(request: Request) -> str:
    base = (APP_BASE_URL or str(request.base_url)).rstrip("/")
    return f"{base}/test_payment?payment=return"


def _agreement_matches_user(agreement: Dict[str, Any], user: Dict[str, Any]) -> bool:
    item_user = agreement.get("user") or {}
    user_id = user.get("id")
    if user_id and item_user.get("id") == user_id:
        return True
    user_email = (user.get("email") or "").strip().lower()
    agreement_email = (agreement.get("email") or "").strip().lower()
    return bool(user_email and agreement_email and user_email == agreement_email)


def _tinkoff_token(payload: Dict[str, Any]) -> str:
    values: Dict[str, str] = {}
    for key, value in payload.items():
        if key in {"Token", "Password"}:
            continue
        if isinstance(value, (dict, list)):
            continue
        values[str(key)] = str(value)
    values["Password"] = TINKOFF_PASSWORD or ""
    token_str = "".join(values[key] for key in sorted(values.keys()))
    return hashlib.sha256(token_str.encode("utf-8")).hexdigest()


async def _tinkoff_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    payload["TerminalKey"] = TINKOFF_TERMINAL_KEY
    payload["Token"] = _tinkoff_token(payload)
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.post(f"{TINKOFF_API_URL}/{method}", json=payload)
    try:
        data = response.json()
    except Exception:
        data = {}
    if response.status_code >= 300:
        detail = data.get("Message") or data.get("Details") or f"http_{response.status_code}"
        raise RuntimeError(f"Tinkoff error: {detail}")
    if not isinstance(data, dict):
        raise RuntimeError("Tinkoff error: invalid response")
    return data


async def _tinkoff_init(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await _tinkoff_post("Init", payload)


async def _tinkoff_get_state(payment_id: str) -> Optional[Dict[str, Any]]:
    if not payment_id:
        return None
    try:
        data = await _tinkoff_post("GetState", {"PaymentId": payment_id})
    except Exception:
        return None
    return data if isinstance(data, dict) else None


async def _tinkoff_get_qr(payment_id: str) -> Optional[Dict[str, Any]]:
    if not payment_id:
        return None
    try:
        data = await _tinkoff_post("GetQr", {"PaymentId": payment_id, "DataType": "PAYLOAD"})
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _tinkoff_verify_token(payload: Dict[str, Any]) -> bool:
    token = str(payload.get("Token") or "")
    if not token:
        return False
    expected = _tinkoff_token(payload)
    return token == expected


@router.post("/payments/create", include_in_schema=False)
async def create_payment(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/account", status_code=HTTP_302_FOUND)
    if not TINKOFF_ENABLED:
        return HTMLResponse("Оплата временно недоступна", status_code=503)

    form = await request.form()
    agreement_file = str(form.get("agreement_file") or "").strip()
    lessons_raw = str(form.get("lessons") or "").strip()
    try:
        lessons = int(lessons_raw)
    except Exception:
        lessons = 0
    if not agreement_file or lessons <= 0:
        return RedirectResponse("/account?payment_error=Некорректные+данные", status_code=HTTP_302_FOUND)

    path = AGREEMENTS_DIR / agreement_file
    if not path.exists():
        return RedirectResponse("/account?payment_error=Договор+не+найден", status_code=HTTP_302_FOUND)
    agreement = load_json(path, {})
    if not isinstance(agreement, dict):
        return RedirectResponse("/account?payment_error=Договор+не+найден", status_code=HTTP_302_FOUND)
    if not _agreement_matches_user(agreement, user):
        return RedirectResponse("/account?payment_error=Доступ+запрещен", status_code=HTTP_302_FOUND)

    course = str(agreement.get("course") or "").strip()
    rate = course_rate(course)
    if not rate:
        return RedirectResponse("/account?payment_error=Цена+курса+не+найдена", status_code=HTTP_302_FOUND)

    discount_percent = 0
    referrals = load_referrals()
    students = referrals.get("students") or {}
    student = find_student_by_phone(students, agreement.get("phone") or "")
    if student and student.get("referral_code"):
        discount_percent = referral_effective_percent(student, course, month_key())
        if discount_percent > 100:
            discount_percent = 100

    amount = Decimal(rate) * Decimal(lessons)
    if discount_percent:
        amount = amount * (Decimal(100 - discount_percent) / Decimal(100))
    if amount < Decimal("1.00"):
        return RedirectResponse("/account?payment_error=Сумма+слишком+мала", status_code=HTTP_302_FOUND)

    payments_data = load_payments()
    payments = payments_data.get("payments") or {}
    now_ts = int(time.time())
    active_statuses = {
        "NEW",
        "FORM_SHOWED",
        "AUTHORIZING",
        "AUTHORIZED",
        "pending",
        "waiting_for_capture",
        "waiting_for_confirmation",
    }
    for record in payments.values():
        if not isinstance(record, dict):
            continue
        if record.get("agreement_file") != agreement_file:
            continue
        if record.get("applied"):
            continue
        status = record.get("status") or ""
        created_at = int(record.get("created_at") or 0)
        if status in active_statuses and (now_ts - created_at) < 2 * 60 * 60:
            confirmation_url = record.get("confirmation_url") or ""
            if confirmation_url:
                return RedirectResponse(confirmation_url, status_code=HTTP_302_FOUND)
            return RedirectResponse("/account?payment_error=Есть+незавершенный+платеж", status_code=HTTP_302_FOUND)

    metadata = {
        "agreement_file": agreement_file,
        "lessons": str(lessons),
        "discount_percent": str(discount_percent),
        "discount_source": "monthly" if discount_percent else "",
        "price_per_lesson": str(rate),
        "user_id": str(user.get("id") or ""),
        "course": course,
        "phone": normalize_phone(agreement.get("phone") or ""),
    }
    amount_rub = _amount_value(amount)
    amount_kopeks = int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    order_id = f"{agreement_file}-{now_ts}-{secrets.token_hex(4)}"
    init_payload = {
        "Amount": amount_kopeks,
        "OrderId": order_id,
        "Description": f"Оплата занятий: {course}",
        "NotificationURL": f"{(APP_BASE_URL or str(request.base_url)).rstrip('/')}/payments/tinkoff",
        "SuccessURL": _build_return_url(request),
        "FailURL": _build_return_url(request),
        "DATA": metadata,
    }

    try:
        payment = await _tinkoff_init(init_payload)
    except Exception as exc:
        logging.getLogger("app.payments").error("Tinkoff init failed: %s", exc)
        return RedirectResponse("/account?payment_error=Ошибка+создания+платежа", status_code=HTTP_302_FOUND)

    if not payment.get("Success"):
        return RedirectResponse("/account?payment_error=Ошибка+платежа", status_code=HTTP_302_FOUND)

    payment_id = str(payment.get("PaymentId") or "")
    if not payment_id:
        return RedirectResponse("/account?payment_error=Ошибка+платежа", status_code=HTTP_302_FOUND)

    qr = await _tinkoff_get_qr(payment_id)
    confirmation_url = str((qr or {}).get("Data") or "")
    if not confirmation_url:
        return RedirectResponse("/account?payment_error=QR+не+получен", status_code=HTTP_302_FOUND)

    payments[payment_id] = {
        "id": payment_id,
        "provider": "tinkoff",
        "status": payment.get("Status") or "NEW",
        "amount": {"value": amount_rub, "currency": "RUB"},
        "course": course,
        "lessons": lessons,
        "discount_percent": discount_percent,
        "agreement_file": agreement_file,
        "order_id": order_id,
        "user_id": str(user.get("id") or ""),
        "phone": normalize_phone(agreement.get("phone") or ""),
        "created_at": now_ts,
        "updated_at": now_ts,
        "applied": False,
        "confirmation_url": confirmation_url,
    }
    payments_data["payments"] = payments
    _append_event(
        payments_data,
        {
            "ts": now_ts,
            "event": "payment_created",
            "payment_id": payment_id,
            "agreement_file": agreement_file,
            "status": payment.get("Status"),
            "amount": amount_rub,
        },
    )
    save_payments(payments_data)

    return RedirectResponse(confirmation_url, status_code=HTTP_302_FOUND)


@router.post("/payments/tinkoff", include_in_schema=False)
async def tinkoff_webhook(request: Request):
    if not TINKOFF_ENABLED:
        return Response(status_code=503)
    try:
        payload = await request.json()
    except Exception:
        return Response(status_code=400)

    if not _tinkoff_verify_token(payload):
        return Response(status_code=400)

    payment_id = str(payload.get("PaymentId") or "")
    if not payment_id:
        return Response(status_code=400)

    status = str(payload.get("Status") or "")
    metadata = payload.get("DATA") if isinstance(payload.get("DATA"), dict) else {}
    test_mode = str(metadata.get("test_mode") or "").lower() in {"1", "true", "yes"}
    discount_source = str(metadata.get("discount_source") or "")
    lessons_raw = metadata.get("lessons") or ""
    try:
        lessons = int(lessons_raw)
    except Exception:
        lessons = 0
    discount_raw = metadata.get("discount_percent") or ""
    try:
        discount_percent = int(discount_raw)
    except Exception:
        discount_percent = 0

    payments_data = load_payments()
    payments = payments_data.get("payments") or {}
    record = payments.get(payment_id)
    if not record:
        amount_raw = payload.get("Amount")
        amount_value = "-"
        try:
            amount_value = str(Decimal(amount_raw) / Decimal(100))
        except Exception:
            pass
        record = {
            "id": payment_id,
            "provider": "tinkoff",
            "agreement_file": metadata.get("agreement_file"),
            "course": metadata.get("course"),
            "lessons": lessons,
            "discount_percent": discount_percent,
            "status": status,
            "amount": {"value": amount_value, "currency": "RUB"},
            "order_id": payload.get("OrderId") or "",
            "created_at": int(time.time()),
            "updated_at": int(time.time()),
            "applied": False,
            "phone": normalize_phone(metadata.get("phone") or ""),
            "test_mode": test_mode,
        }

    record["status"] = status
    record["updated_at"] = int(time.time())

    if status == "CONFIRMED" and not record.get("applied"):
        agreement_file = record.get("agreement_file") or metadata.get("agreement_file")
        if test_mode:
            debug = {"agreement_file": agreement_file, "lessons": lessons, "discount_percent": discount_percent}
            if agreement_file:
                path = AGREEMENTS_DIR / str(agreement_file)
                if path.exists():
                    agreement = load_json(path, {})
                    if isinstance(agreement, dict):
                        try:
                            current_paid = int(agreement.get("paid_lessons") or 0)
                        except Exception:
                            current_paid = 0
                        debug["current_paid_lessons"] = current_paid
                        debug["would_paid_lessons"] = current_paid + max(lessons, 0)
            record["debug"] = debug
        else:
            if agreement_file:
                path = AGREEMENTS_DIR / str(agreement_file)
                if path.exists():
                    agreement = load_json(path, {})
                    if isinstance(agreement, dict):
                        try:
                            current_paid = int(agreement.get("paid_lessons") or 0)
                        except Exception:
                            current_paid = 0
                        if lessons > 0:
                            agreement["paid_lessons"] = current_paid + lessons
                            save_json(path, agreement)

            if discount_percent > 0 and discount_source != "monthly":
                referrals = load_referrals()
                students = referrals.get("students") or {}
                phone = record.get("phone") or normalize_phone(metadata.get("phone") or "")
                student = find_student_by_phone(students, phone)
                if student and student.get("referral_code"):
                    stats = referral_stats_for_referrer(student, students)
                    available = int(stats.get("balance") or 0)
                    apply_percent = min(available, discount_percent)
                    if apply_percent > 0:
                        applied = student.get("discount_applied") if isinstance(student.get("discount_applied"), list) else []
                        applied.append(
                            {
                                "ts": int(time.time()),
                                "percent": apply_percent,
                                "note": f"СБП {payment_id}",
                            }
                        )
                        student["discount_applied"] = applied
                        student["updated_at"] = int(time.time())
                        referrals["students"] = students
                        save_referrals(referrals)

        record["applied"] = True

    payments[payment_id] = record
    payments_data["payments"] = payments
    _append_event(
        payments_data,
        {
            "ts": int(time.time()),
            "event": "payment_update",
            "payment_id": payment_id,
            "status": status,
            "test_mode": test_mode,
            "applied": record.get("applied"),
        },
    )
    save_payments(payments_data)
    return Response(status_code=200)


@router.get("/test_payment", include_in_schema=False)
async def test_payment_page(request: Request):
    guard = admin_required(request)
    if guard:
        return guard

    agreements = load_agreements()
    agreements_view = []
    for item in agreements:
        agreements_view.append(
            {
                "file": item.get("_file"),
                "course": item.get("course") or "-",
                "name": item.get("full_name") or (item.get("user") or {}).get("name") or "-",
                "phone": item.get("phone") or "-",
            }
        )

    payments_data = load_payments()
    payments = payments_data.get("payments") or {}
    payments_view = []
    last_debug = None
    last_debug_ts = 0
    for item in payments.values():
        if not isinstance(item, dict):
            continue
        debug = item.get("debug")
        created_ts = int(item.get("created_at") or 0)
        if debug and created_ts >= last_debug_ts:
            last_debug = debug
            last_debug_ts = created_ts
        payments_view.append(
            {
                "id": item.get("id"),
                "status": item.get("status"),
                "amount": (item.get("amount") or {}).get("value") or "-",
                "lessons": item.get("lessons"),
                "agreement_file": item.get("agreement_file"),
                "test_mode": bool(item.get("test_mode")),
                "created_at": _format_ts(item.get("created_at")),
                "created_at_ts": int(item.get("created_at") or 0),
                "confirmation_url": item.get("confirmation_url"),
            }
        )
    payments_view.sort(key=lambda item: item.get("created_at_ts") or 0, reverse=True)
    payments_view = payments_view[:15]

    events = payments_data.get("events") if isinstance(payments_data.get("events"), list) else []
    events_view = []
    for entry in events[-15:]:
        if not isinstance(entry, dict):
            continue
        events_view.append(
            {
                **entry,
                "ts_label": _format_ts(entry.get("ts")),
            }
        )

    payment_id = request.query_params.get("payment_id") or ""
    payment_check = None
    if payment_id and TINKOFF_ENABLED:
        payment_check = await _tinkoff_get_state(payment_id)

    return render(
        request,
        "test_payment.html",
        {
            "agreements": agreements_view,
            "payments": payments_view,
            "events": events_view,
            "payment_check": payment_check,
            "payment_id": payment_id,
            "payments_enabled": TINKOFF_ENABLED,
            "payment_error": request.query_params.get("error"),
            "payment_status": request.query_params.get("payment"),
            "last_debug": last_debug,
        },
    )


@router.post("/test_payment/create", include_in_schema=False)
async def test_payment_create(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    if not TINKOFF_ENABLED:
        return RedirectResponse("/test_payment?error=Оплата+не+настроена", status_code=HTTP_302_FOUND)

    form = await request.form()
    agreement_file = str(form.get("agreement_file") or "").strip()
    lessons_raw = str(form.get("lessons") or "").strip()
    try:
        lessons = int(lessons_raw)
    except Exception:
        lessons = 0
    if not agreement_file or lessons <= 0:
        return RedirectResponse("/test_payment?error=Некорректные+данные", status_code=HTTP_302_FOUND)

    path = AGREEMENTS_DIR / agreement_file
    if not path.exists():
        return RedirectResponse("/test_payment?error=Договор+не+найден", status_code=HTTP_302_FOUND)
    agreement = load_json(path, {})
    if not isinstance(agreement, dict):
        return RedirectResponse("/test_payment?error=Договор+не+найден", status_code=HTTP_302_FOUND)

    course = str(agreement.get("course") or "").strip()
    rate = course_rate(course) or 0

    discount_percent = 0
    referrals = load_referrals()
    students = referrals.get("students") or {}
    student = find_student_by_phone(students, agreement.get("phone") or "")
    if student and student.get("referral_code"):
        discount_percent = referral_effective_percent(student, course, month_key())
        if discount_percent > 100:
            discount_percent = 100

    amount = Decimal("1.00")
    metadata = {
        "agreement_file": agreement_file,
        "lessons": str(lessons),
        "discount_percent": str(discount_percent),
        "discount_source": "monthly" if discount_percent else "",
        "price_per_lesson": str(rate),
        "course": course,
        "phone": normalize_phone(agreement.get("phone") or ""),
        "test_mode": "1",
    }
    amount_kopeks = int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    order_id = f"{agreement_file}-test-{int(time.time())}-{secrets.token_hex(4)}"
    init_payload = {
        "Amount": amount_kopeks,
        "OrderId": order_id,
        "Description": f"Тестовый платеж 1 ₽ ({course})",
        "NotificationURL": f"{(APP_BASE_URL or str(request.base_url)).rstrip('/')}/payments/tinkoff",
        "SuccessURL": _build_test_return_url(request),
        "FailURL": _build_test_return_url(request),
        "DATA": metadata,
    }

    try:
        payment = await _tinkoff_init(init_payload)
    except Exception as exc:
        logging.getLogger("app.payments").error("Test payment create failed: %s", exc)
        return RedirectResponse("/test_payment?error=Ошибка+создания+платежа", status_code=HTTP_302_FOUND)

    if not payment.get("Success"):
        return RedirectResponse("/test_payment?error=Ошибка+платежа", status_code=HTTP_302_FOUND)

    payment_id = str(payment.get("PaymentId") or "")
    if not payment_id:
        return RedirectResponse("/test_payment?error=Ошибка+платежа", status_code=HTTP_302_FOUND)

    qr = await _tinkoff_get_qr(payment_id)
    confirmation_url = str((qr or {}).get("Data") or "")
    if not confirmation_url:
        return RedirectResponse("/test_payment?error=QR+не+получен", status_code=HTTP_302_FOUND)

    payments_data = load_payments()
    payments = payments_data.get("payments") or {}
    now_ts = int(time.time())
    payments[payment_id] = {
        "id": payment_id,
        "provider": "tinkoff",
        "status": payment.get("Status") or "NEW",
        "amount": {"value": _amount_value(amount), "currency": "RUB"},
        "course": course,
        "lessons": lessons,
        "discount_percent": discount_percent,
        "agreement_file": agreement_file,
        "order_id": order_id,
        "user_id": "",
        "phone": normalize_phone(agreement.get("phone") or ""),
        "created_at": now_ts,
        "updated_at": now_ts,
        "applied": False,
        "confirmation_url": confirmation_url,
        "test_mode": True,
    }
    payments_data["payments"] = payments
    _append_event(
        payments_data,
        {
            "ts": now_ts,
            "event": "test_payment_created",
            "payment_id": payment_id,
            "agreement_file": agreement_file,
            "status": payment.get("Status"),
            "amount": _amount_value(amount),
        },
    )
    save_payments(payments_data)

    return RedirectResponse(confirmation_url, status_code=HTTP_302_FOUND)
