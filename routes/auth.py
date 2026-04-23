import logging
import re
import secrets
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse
from starlette.status import HTTP_302_FOUND
from routes.account_content import (
    ACCOUNT_WORKSPACE_ROUTE_MAP,
    build_account_content,
    build_account_page_payload,
    build_account_homework_payload,
    build_account_lectures_payload,
    build_account_schedule_payload,
    build_account_teachers_payload,
    get_lecture_record,
)

from core import (
    GITHUB_CLIENT_ID,
    GITHUB_CLIENT_SECRET,
    OAuthError,
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    CONTRACT_KEY_POINTS,
    CONTRACT_DOCUMENTS,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_USERNAME_RE,
    TELETHON_AUTO_LOGIN,
    TELETHON_ENABLED,
    PROFILE_AVATARS_DIR,
    USERS_FILE,
    CODES_FILE,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    build_redirect_uri,
    build_contract_url,
    build_month_calendar,
    course_rate,
    clear_user,
    contract_channel_label,
    contract_status_from_item,
    ensure_telethon_login,
    get_current_user,
    get_telethon_client,
    load_agreements,
    load_payments,
    load_referrals,
    load_json,
    find_student_by_phone,
    month_key,
    normalize_phone,
    normalize_materials,
    login_context,
    oauth,
    providers,
    referral_effective_percent,
    render,
    save_json,
    send_email_code,
    set_current_user,
    validate_antibot_submission,
    verify_telegram_auth,
    TINKOFF_ENABLED,
)

router = APIRouter()

ACCOUNT_SHELL_PAGES = [
    {
        "key": "courses",
        "label": "Курсы",
        "href": "/account",
        "workspace": "courses",
        "count_key": "total_courses",
        "note": "Все траектории, прогресс по модулям и точки входа в материалы.",
    },
    {
        "key": "calendar",
        "label": "Календарь",
        "href": "/account/calendar",
        "workspace": "calendar",
        "count_key": "upcoming_events",
        "note": "Ближайшие занятия и помесячный ритм обучения без лишнего шума.",
    },
    {
        "key": "homework",
        "label": "ДЗ",
        "href": "/account/homework",
        "workspace": "homework",
        "count_key": "homework_open",
        "note": "Активные домашние задания, дедлайны и нужные материалы в одном месте.",
    },
    {
        "key": "lectures",
        "label": "Лекции",
        "href": "/account/lectures",
        "workspace": "lectures",
        "count_key": "lecture_records",
        "note": "Записи занятий, фильтры и быстрый возврат к нужной теме.",
    },
    {
        "key": "teachers",
        "label": "Преподаватели",
        "href": "/account/teachers",
        "workspace": "teachers",
        "count_key": "teachers_count",
        "note": "Кто ведёт занятия, за что отвечает и как быстро связаться.",
    },
    {
        "key": "documents",
        "label": "Документы",
        "href": "/account/documents",
        "workspace": "documents",
        "count_key": "signed_contracts",
        "note": "Договоры, PDF и сервисные детали без поиска по длинной странице.",
    },
    {
        "key": "profile",
        "label": "Профиль",
        "href": "/account/profile",
        "workspace": "settings",
        "note": "Способ входа, данные аккаунта и системные настройки кабинета.",
    },
]

ACCOUNT_SHELL_PAGE_MAP = {item["key"]: item for item in ACCOUNT_SHELL_PAGES}

AVATAR_MAX_BYTES = 4 * 1024 * 1024


def _local_avatar_url(value: Any) -> str:
    url = str(value or "").strip()
    return url if url.startswith("/profile_avatars/") else ""


def _build_login_session_user(
    users: Dict[str, Any],
    user_id: str,
    user: Dict[str, Any],
    *,
    photo_url: Optional[str] = None,
) -> Dict[str, Any]:
    existing = users.get(user_id) if isinstance(users, dict) else None
    if not isinstance(existing, dict):
        existing = {}

    stored_user = {key: value for key, value in user.items() if key != "photo_url"}
    saved_avatar_url = _local_avatar_url(existing.get("avatar_url"))
    if saved_avatar_url:
        stored_user["avatar_url"] = saved_avatar_url
    else:
        stored_user.pop("avatar_url", None)
    stored_user.pop("photo_url", None)
    users[user_id] = stored_user

    session_user = dict(stored_user)
    if photo_url and not session_user.get("avatar_url"):
        session_user["photo_url"] = photo_url
    return session_user


def _detect_avatar_extension(data: bytes) -> str:
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if data[:6] in {b"GIF87a", b"GIF89a"}:
        return ".gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return ""


def _safe_avatar_file_stem(user_id: str) -> str:
    stem = re.sub(r"[^a-zA-Z0-9_-]+", "-", user_id).strip("-")
    return stem[:80] or secrets.token_hex(8)


def _delete_uploaded_avatar(url: Any) -> None:
    avatar_url = _local_avatar_url(url)
    if not avatar_url:
        return
    file_name = avatar_url.rsplit("/", 1)[-1].split("?", 1)[0]
    if not file_name or "/" in file_name or "\\" in file_name:
        return
    try:
        root = PROFILE_AVATARS_DIR.resolve()
        target = (PROFILE_AVATARS_DIR / file_name).resolve()
        target.relative_to(root)
        if target.exists():
            target.unlink()
    except Exception:
        logging.getLogger("app.auth").warning("Failed to delete old profile avatar: %s", avatar_url, exc_info=True)


def _account_avatar_redirect(next_url: str, **params: str) -> RedirectResponse:
    safe_next = next_url if next_url.startswith("/account") else "/account/profile"
    separator = "&" if "?" in safe_next else "?"
    location = f"{safe_next}{separator}{urlencode(params)}" if params else safe_next
    return RedirectResponse(location, status_code=HTTP_302_FOUND)


def _load_current_user_agreements(user: Dict[str, Any]) -> List[Dict[str, Any]]:
    user_id = user.get("id")
    user_email = (user.get("email") or "").strip().lower()
    agreements = []
    for item in load_agreements():
        item_user = item.get("user") or {}
        matches = False
        if user_id and item_user.get("id") == user_id:
            matches = True
        elif user_email and (item.get("email") or "").strip().lower() == user_email:
            matches = True
        if matches:
            agreements.append(item)
    return agreements


def _antibot_error_message(reason: str) -> str:
    if reason in {"missing_turnstile", "turnstile_failed", "turnstile_unavailable"}:
        return "Подтвердите, что вы не робот, и отправьте форму снова."
    return "Запрос отклонен защитой от ботов. Обновите страницу и повторите попытку."


def _legacy_account_workspace_redirect(request: Request) -> Optional[RedirectResponse]:
    workspace = str(request.query_params.get("workspace") or "").strip()
    if not workspace:
        return None
    target = ACCOUNT_WORKSPACE_ROUTE_MAP.get(workspace)
    if not target:
        return None
    query_items = [(key, value) for key, value in request.query_params.multi_items() if key != "workspace"]
    location = target
    if query_items:
        location = f"{location}?{urlencode(query_items, doseq=True)}"
    return RedirectResponse(location, status_code=HTTP_302_FOUND)


def _build_account_shell(*, active_key: str, stats: Dict[str, Any]) -> Dict[str, Any]:
    active_page = ACCOUNT_SHELL_PAGE_MAP.get(active_key) or ACCOUNT_SHELL_PAGES[0]
    items: List[Dict[str, Any]] = []
    for page in ACCOUNT_SHELL_PAGES:
        count_value = stats.get(page.get("count_key", "")) if page.get("count_key") else None
        count = str(count_value) if isinstance(count_value, int) and count_value > 0 else ""
        items.append(
            {
                "key": page["key"],
                "label": page["label"],
                "href": page["href"],
                "workspace": page.get("workspace", ""),
                "scroll_target": page.get("scroll_target", ""),
                "note": page.get("note", ""),
                "count": count,
                "active": page["key"] == active_key,
            }
        )
    return {
        "eyebrow": "Student shell",
        "title": "Личный кабинет",
        "subtitle": active_page.get("note", ""),
        "active": active_key,
        "active_label": active_page.get("label", ""),
        "items": items,
        "keys": ",".join(page["key"] for page in ACCOUNT_SHELL_PAGES),
        "scroll_target": active_page.get("scroll_target"),
    }


def _build_account_page_context(
    request: Request,
    *,
    user: Dict[str, Any],
    workspace_active: str,
    shell_active: str,
) -> Dict[str, Any]:
    agreements_all = load_agreements()
    user_id = user.get("id")
    user_email = (user.get("email") or "").strip().lower()

    def safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    def format_ts(value: Any) -> str:
        if not value:
            return "-"
        try:
            return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
        except Exception:
            return "-"

    agreements_view = []
    calendar_month = str(request.query_params.get("month") or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", calendar_month or ""):
        calendar_month = month_key()
    lecture_q = str(request.query_params.get("lecture_q") or "").strip()
    lecture_source = str(request.query_params.get("lecture_source") or "").strip()
    lecture_course = str(request.query_params.get("lecture_course") or "").strip()
    try:
        lecture_page = max(int(request.query_params.get("lecture_page") or 1), 1)
    except Exception:
        lecture_page = 1
    referrals_data = load_referrals()
    referrals_students = referrals_data.get("students") if isinstance(referrals_data, dict) else {}
    if not isinstance(referrals_students, dict):
        referrals_students = {}
    payments_data = load_payments()
    payments = payments_data.get("payments") if isinstance(payments_data, dict) else {}
    if not isinstance(payments, dict):
        payments = {}
    for item in agreements_all:
        item_user = item.get("user") or {}
        matches = False
        if user_id and item_user.get("id") == user_id:
            matches = True
        elif user_email and (item.get("email") or "").strip().lower() == user_email:
            matches = True
        if not matches:
            continue

        status_key, status_label, status_class = contract_status_from_item(item)
        total_lessons = safe_int(item.get("total_lessons"))
        paid_lessons = safe_int(item.get("paid_lessons"))
        attended_lessons = safe_int(item.get("attended_lessons"))
        current_paid = None
        if paid_lessons is not None and attended_lessons is not None:
            current_paid = max(paid_lessons - attended_lessons, 0)
        remaining = None
        if total_lessons is not None and paid_lessons is not None:
            remaining = max(total_lessons - paid_lessons, 0)
        materials = normalize_materials(item.get("materials"))
        saved_rate = safe_int(item.get("price_per_lesson"))
        price_per_lesson = saved_rate if saved_rate else course_rate(item.get("course"), item.get("duration"))
        discount_percent = 0
        discount_value = None
        discounted_price = None
        student = None
        primary_course = ""
        if referrals_students:
            student = find_student_by_phone(referrals_students, item.get("phone") or "")
        if student:
            primary_course = student.get("primary_course") or ""
            if price_per_lesson and student.get("referral_code"):
                discount_percent = referral_effective_percent(student, item.get("course") or "", month_key())
        if price_per_lesson and discount_percent:
            discount_value = int(round(price_per_lesson * (discount_percent / 100)))
            discounted_price = max(price_per_lesson - discount_value, 0)
        payment_list = []
        active_payment = None
        item_phone = normalize_phone(item.get("phone") or "")
        for payment in payments.values():
            if not isinstance(payment, dict):
                continue
            if payment.get("agreement_file") != item.get("_file"):
                continue
            if user_id and str(payment.get("user_id") or "") != str(user_id):
                if item_phone and normalize_phone(payment.get("phone") or "") != item_phone:
                    continue
            status = str(payment.get("status") or "")
            status_labels = {
                "NEW": "Создан",
                "FORM_SHOWED": "Ожидает оплаты",
                "AUTHORIZING": "Ожидает подтверждения",
                "AUTHORIZED": "Ожидает подтверждения",
                "CONFIRMED": "Оплачен",
                "CANCELED": "Отменён",
                "REJECTED": "Отказ",
                "DEADLINE_EXPIRED": "Истёк срок",
                "REFUNDED": "Возврат",
            }
            payment_item = {
                "id": payment.get("id"),
                "status": status,
                "status_label": status_labels.get(status, status),
                "amount": (payment.get("amount") or {}).get("value") or "-",
                "lessons": payment.get("lessons"),
                "discount_percent": payment.get("discount_percent") or 0,
                "created_at": format_ts(payment.get("created_at")),
                "created_at_ts": int(payment.get("created_at") or 0),
                "confirmation_url": payment.get("confirmation_url"),
                "test_mode": bool(payment.get("test_mode")),
            }
            payment_list.append(payment_item)
            if not active_payment and status in {"pending", "waiting_for_capture", "waiting_for_confirmation"}:
                active_payment = payment_item

        payment_list.sort(key=lambda entry: entry.get("created_at_ts") or 0, reverse=True)

        lesson_calendar = item.get("lesson_calendar") if isinstance(item.get("lesson_calendar"), dict) else {}
        calendar_weeks = build_month_calendar(calendar_month, lesson_calendar)
        agreements_view.append(
            {
                **item,
                "file": item.get("_file"),
                "agreement_file": item.get("_file"),
                "contract_url": build_contract_url(item.get("contract_token"), request),
                "contract_pdf_url": item.get("contract_pdf_url"),
                "contract_status_key": status_key,
                "contract_status_label": status_label,
                "contract_status_class": status_class,
                "contract_channel_label": contract_channel_label(item.get("contract_channel")),
                "total_lessons": total_lessons,
                "paid_lessons": paid_lessons,
                "attended_lessons": attended_lessons,
                "current_paid_lessons": current_paid,
                "remaining_lessons": remaining,
                "current_module": item.get("current_module") or "-",
                "materials": materials,
                "price_per_lesson": price_per_lesson,
                "discount_percent": discount_percent,
                "discount_value": discount_value,
                "discounted_price": discounted_price,
                "primary_course": primary_course,
                "lesson_month": calendar_month,
                "lesson_calendar_map": lesson_calendar,
                "lesson_calendar": calendar_weeks,
                "payments": payment_list,
                "active_payment": active_payment,
            }
        )

    user_display = user.get("name") or user.get("email") or "Пользователь"
    account_content = build_account_content(
        agreements_view,
        user_display=user_display,
        payments_enabled=TINKOFF_ENABLED,
        user=user,
        lectures_query=lecture_q,
        lectures_source=lecture_source,
        lectures_course=lecture_course,
        lectures_page=lecture_page,
    )
    allowed_workspace_keys = {item["key"] for item in account_content["workspace_tabs"]}
    if workspace_active not in allowed_workspace_keys:
        workspace_active = "courses"
    page_payload = build_account_page_payload(
        page_key=shell_active,
        account_content=account_content,
        agreements=agreements_view,
    )
    schedule_items = account_content["schedule"]
    homework_items = account_content["homework"]
    lecture_items = account_content["lectures"]
    teacher_items = account_content["teachers"]
    return {
        "agreements": agreements_view,
        "account_schedule": schedule_items,
        "account_homework": homework_items,
        "account_lectures": lecture_items,
        "account_teachers": teacher_items,
        "account_api": account_content["api"],
        "account_hero": account_content["hero"],
        "account_focus": account_content["focus"],
        "account_overview": account_content["overview"],
        "account_sections": account_content["sections"],
        "account_stats": account_content["stats"],
        "account_schedule_payload": account_content["schedule_payload"],
        "account_homework_payload": account_content["homework_payload"],
        "account_lectures_payload": account_content["lectures_payload"],
        "account_teachers_payload": account_content["teachers_payload"],
        "account_settings": account_content["settings_payload"],
        "account_workspace_tabs": account_content["workspace_tabs"],
        "account_workspace_active": workspace_active,
        "account_page": page_payload["account_page"],
        "account_shell": page_payload["account_shell"],
        "account_template": page_payload["template"],
        "payments_enabled": TINKOFF_ENABLED,
        "payment_status": request.query_params.get("payment"),
        "payment_error": request.query_params.get("payment_error"),
    }


def _render_account_page(
    request: Request,
    *,
    workspace_active: str,
    shell_active: str,
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=HTTP_302_FOUND)
    context = _build_account_page_context(
        request,
        user=user,
        workspace_active=workspace_active,
        shell_active=shell_active,
    )
    return render(request, "account.html", context)


@router.get("/login", include_in_schema=False)
async def login(request: Request):
    next_url = request.query_params.get("next") or "/"
    return render(request, "login.html", login_context(request, next_url=next_url))


def _login_email_context(
    request: Request,
    *,
    next_url: str = "/",
    error: Optional[str] = None,
    email: str = "",
) -> Dict[str, Any]:
    context = login_context(request, next_url=next_url, error=error)
    context["email_value"] = email
    return context


@router.get("/login/email", include_in_schema=False)
async def login_email_page(request: Request):
    next_url = request.query_params.get("next") or "/"
    email = str(request.query_params.get("email") or "").strip().lower()
    return render(request, "login_email.html", _login_email_context(request, next_url=next_url, email=email))


@router.post("/login/vk-bridge", include_in_schema=False)
async def login_vk_bridge(request: Request):
    return RedirectResponse("/login?error=Вход+через+VK+отключен", status_code=HTTP_302_FOUND)
    return RedirectResponse("/login?error=Вход+через+VK+отключен", status_code=HTTP_302_FOUND)
    try:
        payload = await request.json()
    except Exception:
        return RedirectResponse("/login?error=Некорректные+данные", status_code=HTTP_302_FOUND)

    vk_id = payload.get("id")
    if not vk_id:
        return RedirectResponse("/login?error=Нет+данных+VK", status_code=HTTP_302_FOUND)

    users = load_json(USERS_FILE, {})
    user_id = f"vk:{vk_id}"
    existing = users.get(user_id) if isinstance(users, dict) else None
    if not isinstance(existing, dict):
        existing = {}
    name = f"{payload.get('first_name', '')} {payload.get('last_name', '')}".strip() or "VK User"
    phone = existing.get("phone") or payload.get("phone") or payload.get("mobile_phone")
    email = payload.get("email") or existing.get("email")
    user = {
        "id": user_id,
        "email": email,
        "name": name,
        "phone": phone,
        "provider": "vk",
    }
    session_user = _build_login_session_user(
        users,
        user_id,
        user,
        photo_url=payload.get("photo_200") or payload.get("photo_100"),
    )
    save_json(USERS_FILE, users)

    set_current_user(request, session_user)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.post("/login/email", include_in_schema=False)
async def login_email(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    next_url = str(form.get("next", "/"))
    antibot_ok, antibot_reason = await validate_antibot_submission(request, form, "login_email")
    if not antibot_ok:
        return render(
            request,
            "login_email.html",
            _login_email_context(
                request,
                next_url=next_url,
                error=_antibot_error_message(antibot_reason),
                email=email,
            ),
        )
    if not email:
        return render(
            request,
            "login_email.html",
            _login_email_context(request, next_url=next_url, error="Введите email", email=email),
        )

    code = f"{secrets.randbelow(10 ** 6):06d}"
    codes = load_json(CODES_FILE, {})
    codes[email] = {"code": code, "expires": time.time() + 600}
    save_json(CODES_FILE, codes)
    send_email_code(email, code)

    return render(request, "verify.html", {"email": email, "next": next_url})


@router.post("/login/verify", include_in_schema=False)
async def login_verify(request: Request):
    form = await request.form()
    email = str(form.get("email", "")).strip().lower()
    code = str(form.get("code", "")).strip()
    next_url = str(form.get("next", "/"))
    antibot_ok, antibot_reason = await validate_antibot_submission(request, form, "login_verify")
    if not antibot_ok:
        return render(
            request,
            "verify.html",
            {"email": email, "next": next_url, "error": _antibot_error_message(antibot_reason)},
        )

    codes = load_json(CODES_FILE, {})
    entry = codes.get(email)
    if not entry or entry.get("code") != code or entry.get("expires", 0) < time.time():
        return render(
            request,
            "verify.html",
            {"email": email, "next": next_url, "error": "Неверный или просроченный код"},
        )

    users = load_json(USERS_FILE, {})
    user_id = f"email:{email}"
    user = users.get(user_id) or {
        "id": user_id,
        "email": email,
        "name": email,
        "provider": "email",
    }
    session_user = _build_login_session_user(users, user_id, user)
    save_json(USERS_FILE, users)

    set_current_user(request, session_user)
    codes.pop(email, None)
    save_json(CODES_FILE, codes)

    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.get("/login/google", include_in_schema=False)
async def login_google(request: Request):
    if not (oauth and providers["google"]):
        return render(request, "login.html", login_context(request, error="Google OAuth не настроен"))
    logging.getLogger("app.auth").info(
        "Google login start: host=%s scheme=%s cookies=%s session_keys=%s",
        request.url.hostname,
        request.url.scheme,
        list(request.cookies.keys()),
        list(request.session.keys()),
    )
    redirect_uri = build_redirect_uri(request, "auth_google")
    return await oauth.google.authorize_redirect(request, redirect_uri)


@router.get("/auth/google/callback", include_in_schema=False, name="auth_google")
async def auth_google(request: Request):
    if not oauth:
        return render(request, "login.html", login_context(request, error="Google OAuth не настроен"))
    logging.getLogger("app.auth").info(
        "Google callback: host=%s scheme=%s query_state=%s cookies=%s session_keys=%s",
        request.url.hostname,
        request.url.scheme,
        request.query_params.get("state"),
        list(request.cookies.keys()),
        list(request.session.keys()),
    )

    async def exchange_google_token() -> Tuple[Optional[dict], Optional[str]]:
        code = request.query_params.get("code")
        if not code:
            return None, "missing_code"
        redirect_uri = build_redirect_uri(request, "auth_google")
        payload = {
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post("https://oauth2.googleapis.com/token", data=payload)
        try:
            data = resp.json()
        except Exception:
            data = {}
        if not isinstance(data, dict) or "access_token" not in data:
            err = data.get("error") or f"http_{resp.status_code}"
            err_desc = data.get("error_description")
            message = f"{err}: {err_desc}" if err_desc else str(err)
            return None, message
        return data, None

    try:
        token = await oauth.google.authorize_access_token(request)
        if not isinstance(token, dict) or not token.get("access_token"):
            token, manual_error = await exchange_google_token()
            if not token:
                detail = "missing_token"
                extra = f" [manual_exchange: {manual_error}]" if manual_error else " [manual_exchange_failed]"
                safe_detail = re.sub(r"[\\r\\n]+", " ", f"{detail}{extra}")[:300]
                return render(
                    request,
                    "login.html",
                    login_context(request, error=f"Ошибка авторизации Google: {safe_detail}"),
                )
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

        if "missing_token" in str(detail):
            token, manual_error = await exchange_google_token()
            if token:
                detail = ""
                description = ""
            else:
                extra = f" [manual_exchange: {manual_error}]" if manual_error else " [manual_exchange_failed]"
        if detail:
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

    userinfo = None
    if isinstance(token, dict) and token.get("id_token"):
        try:
            userinfo = await oauth.google.parse_id_token(request, token)
        except Exception:
            userinfo = None
    if not userinfo:
        try:
            userinfo_resp = await oauth.google.get("userinfo")
            userinfo = userinfo_resp.json()
        except Exception:
            userinfo = None
    if not userinfo or not isinstance(userinfo, dict):
        try:
            access_token = token.get("access_token") if isinstance(token, dict) else None
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    "https://www.googleapis.com/oauth2/v3/userinfo",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
            userinfo = resp.json()
        except Exception as exc:
            safe_detail = re.sub(r"[\\r\\n]+", " ", str(exc) or "userinfo_error")[:300]
            return render(
                request,
                "login.html",
                login_context(request, error=f"Ошибка авторизации Google: userinfo_failed {safe_detail}"),
            )

    if not userinfo or not isinstance(userinfo, dict):
        return render(request, "login.html", login_context(request, error="Ошибка авторизации Google"))

    users = load_json(USERS_FILE, {})
    user_sub = userinfo.get("sub") or userinfo.get("id") or userinfo.get("email")
    user_id = f"google:{user_sub}"
    existing = users.get(user_id) if isinstance(users, dict) else None
    if not isinstance(existing, dict):
        existing = {}
    phone = (
        existing.get("phone")
        or userinfo.get("phone_number")
        or userinfo.get("phone")
        or userinfo.get("phoneNumber")
    )
    user = {
        "id": user_id,
        "email": userinfo.get("email") or existing.get("email"),
        "name": userinfo.get("name") or userinfo.get("given_name") or existing.get("name") or userinfo.get("email"),
        "phone": phone,
        "provider": "google",
    }
    session_user = _build_login_session_user(users, user_id, user, photo_url=userinfo.get("picture"))
    save_json(USERS_FILE, users)

    set_current_user(request, session_user)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/login/github", include_in_schema=False)
async def login_github(request: Request):
    if not (oauth and providers["github"]):
        return render(request, "login.html", login_context(request, error="GitHub OAuth не настроен"))
    redirect_uri = build_redirect_uri(request, "auth_github")
    return await oauth.github.authorize_redirect(request, redirect_uri)


@router.get("/auth/github/callback", include_in_schema=False, name="auth_github")
async def auth_github(request: Request):
    if not (oauth and GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET):
        return render(request, "login.html", login_context(request, error="GitHub OAuth не настроен"))
    try:
        token = await oauth.github.authorize_access_token(request)
        profile_resp = await oauth.github.get("user", token=token)
        profile = profile_resp.json()
    except OAuthError as exc:
        safe_detail = re.sub(r"[\r\n]+", " ", getattr(exc, "description", None) or str(exc) or "oauth_error")[:300]
        return render(request, "login.html", login_context(request, error=f"Ошибка авторизации GitHub: {safe_detail}"))
    except Exception as exc:
        safe_detail = re.sub(r"[\r\n]+", " ", str(exc) or "unknown_error")[:300]
        return render(request, "login.html", login_context(request, error=f"Ошибка авторизации GitHub: {safe_detail}"))

    if not isinstance(profile, dict) or not profile.get("id"):
        return render(request, "login.html", login_context(request, error="Ошибка авторизации GitHub: профиль не получен"))

    email = profile.get("email")
    if not email:
        try:
            emails_resp = await oauth.github.get("user/emails", token=token)
            emails_payload = emails_resp.json()
        except Exception:
            emails_payload = []
        if isinstance(emails_payload, list):
            email = next(
                (
                    item.get("email")
                    for item in emails_payload
                    if isinstance(item, dict) and item.get("primary") and item.get("verified") and item.get("email")
                ),
                None,
            ) or next(
                (
                    item.get("email")
                    for item in emails_payload
                    if isinstance(item, dict) and item.get("email")
                ),
                None,
            )

    users = load_json(USERS_FILE, {})
    user_id = f"github:{profile.get('id')}"
    existing = users.get(user_id) if isinstance(users, dict) else None
    if not isinstance(existing, dict):
        existing = {}
    user = {
        "id": user_id,
        "email": email or existing.get("email"),
        "name": profile.get("name") or profile.get("login") or existing.get("name") or email or "GitHub User",
        "phone": existing.get("phone"),
        "provider": "github",
    }
    session_user = _build_login_session_user(users, user_id, user, photo_url=profile.get("avatar_url"))
    save_json(USERS_FILE, users)

    set_current_user(request, session_user)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/login/vk", include_in_schema=False)
async def login_vk(request: Request):
    return render(request, "login.html", login_context(request, error="Вход через VK отключен"))
    if not (oauth and providers["vk"]):
        return render(request, "login.html", login_context(request, error="VK OAuth не настроен"))
    redirect_uri = build_redirect_uri(request, "auth_vk")
    return await oauth.vk.authorize_redirect(request, redirect_uri)


@router.get("/auth/vk/callback", include_in_schema=False, name="auth_vk")
async def auth_vk(request: Request):
    return render(request, "login.html", login_context(request, error="Вход через VK отключен"))
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
    existing = users.get(user_id) if isinstance(users, dict) else None
    if not isinstance(existing, dict):
        existing = {}
    phone = (
        existing.get("phone")
        or profile.get("mobile_phone")
        or profile.get("phone")
        or token.get("phone")
    )
    user = {
        "id": user_id,
        "email": token.get("email") or existing.get("email"),
        "name": f"{profile.get('first_name', '')} {profile.get('last_name', '')}".strip() or existing.get("name"),
        "phone": phone,
        "provider": "vk",
    }
    session_user = _build_login_session_user(users, user_id, user, photo_url=profile.get("photo_200"))
    save_json(USERS_FILE, users)

    set_current_user(request, session_user)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/login/telegram", include_in_schema=False)
async def login_telegram(request: Request):
    logging.getLogger("app.auth").info("Telegram login start: cookies=%s session_keys=%s", list(request.cookies.keys()), list(request.session.keys()))
    data = dict(request.query_params)
    if not data or "hash" not in data:
        return render(request, "login.html", login_context(request, error="Нажмите и подтвердите вход через Telegram"))
    if not verify_telegram_auth(data):
        return render(request, "login.html", login_context(request, error="Ошибка авторизации Telegram"))

    users = load_json(USERS_FILE, {})
    user_id = f"telegram:{data.get('id')}"
    existing = users.get(user_id) if isinstance(users, dict) else None
    if not isinstance(existing, dict):
        existing = {}
    user = {
        "id": user_id,
        "email": existing.get("email"),
        "name": data.get("first_name") or data.get("username") or "Telegram",
        "phone": existing.get("phone"),
        "provider": "telegram",
    }
    session_user = _build_login_session_user(users, user_id, user, photo_url=data.get("photo_url"))
    save_json(USERS_FILE, users)
    set_current_user(request, session_user)

    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/validate/telegram", include_in_schema=False)
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
        except UsernameInvalidError:
            return {"ok": False, "reason": "invalid"}
        except UsernameNotOccupiedError:
            return {"ok": False, "reason": "not_found"}
        except ValueError:
            return {"ok": False, "reason": "invalid"}
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


@router.get("/logout", include_in_schema=False)
async def logout(request: Request):
    clear_user(request)
    return RedirectResponse("/", status_code=HTTP_302_FOUND)


@router.get("/account", include_in_schema=False)
async def account(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=HTTP_302_FOUND)
    legacy_redirect = _legacy_account_workspace_redirect(request)
    if legacy_redirect:
        return legacy_redirect
    return _render_account_page(request, workspace_active="courses", shell_active="courses")
    agreements_all = load_agreements()
    user_id = user.get("id")
    user_email = (user.get("email") or "").strip().lower()
    def safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    def format_ts(value: Any) -> str:
        if not value:
            return "-"
        try:
            return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
        except Exception:
            return "-"

    agreements_view = []
    calendar_month = str(request.query_params.get("month") or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", calendar_month or ""):
        calendar_month = month_key()
    lecture_q = str(request.query_params.get("lecture_q") or "").strip()
    lecture_source = str(request.query_params.get("lecture_source") or "").strip()
    lecture_course = str(request.query_params.get("lecture_course") or "").strip()
    try:
        lecture_page = max(int(request.query_params.get("lecture_page") or 1), 1)
    except Exception:
        lecture_page = 1
    referrals_data = load_referrals()
    referrals_students = referrals_data.get("students") if isinstance(referrals_data, dict) else {}
    if not isinstance(referrals_students, dict):
        referrals_students = {}
    payments_data = load_payments()
    payments = payments_data.get("payments") if isinstance(payments_data, dict) else {}
    if not isinstance(payments, dict):
        payments = {}
    for item in agreements_all:
        item_user = item.get("user") or {}
        matches = False
        if user_id and item_user.get("id") == user_id:
            matches = True
        elif user_email and (item.get("email") or "").strip().lower() == user_email:
            matches = True
        # Fallbacks are intentionally strict to avoid leaking agreements.
        if not matches:
            continue

        status_key, status_label, status_class = contract_status_from_item(item)
        total_lessons = safe_int(item.get("total_lessons"))
        paid_lessons = safe_int(item.get("paid_lessons"))
        attended_lessons = safe_int(item.get("attended_lessons"))
        current_paid = None
        if paid_lessons is not None and attended_lessons is not None:
            current_paid = max(paid_lessons - attended_lessons, 0)
        remaining = None
        if total_lessons is not None and paid_lessons is not None:
            remaining = max(total_lessons - paid_lessons, 0)
        materials = normalize_materials(item.get("materials"))
        saved_rate = safe_int(item.get("price_per_lesson"))
        price_per_lesson = saved_rate if saved_rate else course_rate(item.get("course"), item.get("duration"))
        discount_percent = 0
        discount_value = None
        discounted_price = None
        student = None
        primary_course = ""
        if referrals_students:
            student = find_student_by_phone(referrals_students, item.get("phone") or "")
        if student:
            primary_course = student.get("primary_course") or ""
            if price_per_lesson and student.get("referral_code"):
                discount_percent = referral_effective_percent(student, item.get("course") or "", month_key())
        if price_per_lesson and discount_percent:
            discount_value = int(round(price_per_lesson * (discount_percent / 100)))
            discounted_price = max(price_per_lesson - discount_value, 0)
        payment_list = []
        active_payment = None
        item_phone = normalize_phone(item.get("phone") or "")
        for payment in payments.values():
            if not isinstance(payment, dict):
                continue
            if payment.get("agreement_file") != item.get("_file"):
                continue
            if user_id and str(payment.get("user_id") or "") != str(user_id):
                if item_phone and normalize_phone(payment.get("phone") or "") != item_phone:
                    continue
            status = str(payment.get("status") or "")
            status_labels = {
                "NEW": "Создан",
                "FORM_SHOWED": "Ожидает оплаты",
                "AUTHORIZING": "Ожидает подтверждения",
                "AUTHORIZED": "Ожидает подтверждения",
                "CONFIRMED": "Оплачен",
                "CANCELED": "Отменён",
                "REJECTED": "Отказ",
                "DEADLINE_EXPIRED": "Истёк срок",
                "REFUNDED": "Возврат",
            }
            payment_item = {
                "id": payment.get("id"),
                "status": status,
                "status_label": status_labels.get(status, status),
                "amount": (payment.get("amount") or {}).get("value") or "-",
                "lessons": payment.get("lessons"),
                "discount_percent": payment.get("discount_percent") or 0,
                "created_at": format_ts(payment.get("created_at")),
                "created_at_ts": int(payment.get("created_at") or 0),
                "confirmation_url": payment.get("confirmation_url"),
                "test_mode": bool(payment.get("test_mode")),
            }
            payment_list.append(payment_item)
            if not active_payment and status in {"pending", "waiting_for_capture", "waiting_for_confirmation"}:
                active_payment = payment_item

        payment_list.sort(key=lambda entry: entry.get("created_at_ts") or 0, reverse=True)

        lesson_calendar = item.get("lesson_calendar") if isinstance(item.get("lesson_calendar"), dict) else {}
        calendar_weeks = build_month_calendar(calendar_month, lesson_calendar)
        agreements_view.append(
            {
                **item,
                "file": item.get("_file"),
                "agreement_file": item.get("_file"),
                "contract_url": build_contract_url(item.get("contract_token"), request),
                "contract_pdf_url": item.get("contract_pdf_url"),
                "contract_status_key": status_key,
                "contract_status_label": status_label,
                "contract_status_class": status_class,
                "contract_channel_label": contract_channel_label(item.get("contract_channel")),
                "total_lessons": total_lessons,
                "paid_lessons": paid_lessons,
                "attended_lessons": attended_lessons,
                "current_paid_lessons": current_paid,
                "remaining_lessons": remaining,
                "current_module": item.get("current_module") or "-",
                "materials": materials,
                "price_per_lesson": price_per_lesson,
                "discount_percent": discount_percent,
                "discount_value": discount_value,
                "discounted_price": discounted_price,
                "primary_course": primary_course,
                "lesson_month": calendar_month,
                "lesson_calendar_map": lesson_calendar,
                "lesson_calendar": calendar_weeks,
                "payments": payment_list,
                "active_payment": active_payment,
            }
        )

    user_display = user.get("name") or user.get("email") or "Пользователь"
    account_content = build_account_content(
        agreements_view,
        user_display=user_display,
        payments_enabled=TINKOFF_ENABLED,
        user=user,
        lectures_query=lecture_q,
        lectures_source=lecture_source,
        lectures_course=lecture_course,
        lectures_page=lecture_page,
    )
    workspace_active = str(request.query_params.get("workspace") or "courses").strip() or "courses"
    allowed_workspace_keys = {item["key"] for item in account_content["workspace_tabs"]}
    if workspace_active not in allowed_workspace_keys:
        workspace_active = "courses"
    schedule_items = account_content["schedule"]
    homework_items = account_content["homework"]
    lecture_items = account_content["lectures"]
    teacher_items = account_content["teachers"]
    return render(
        request,
        "account.html",
        {
            "agreements": agreements_view,
            "account_schedule": schedule_items,
            "account_homework": homework_items,
            "account_lectures": lecture_items,
            "account_teachers": teacher_items,
            "account_api": account_content["api"],
            "account_hero": account_content["hero"],
            "account_focus": account_content["focus"],
            "account_overview": account_content["overview"],
            "account_sections": account_content["sections"],
            "account_stats": account_content["stats"],
            "account_schedule_payload": account_content["schedule_payload"],
            "account_homework_payload": account_content["homework_payload"],
            "account_lectures_payload": account_content["lectures_payload"],
            "account_teachers_payload": account_content["teachers_payload"],
            "account_settings": account_content["settings_payload"],
            "account_workspace_tabs": account_content["workspace_tabs"],
            "account_workspace_active": workspace_active,
            "payments_enabled": TINKOFF_ENABLED,
            "payment_status": request.query_params.get("payment"),
            "payment_error": request.query_params.get("payment_error"),
            "avatar_status": request.query_params.get("avatar"),
            "avatar_error": request.query_params.get("avatar_error"),
        },
    )


@router.get("/account/courses", include_in_schema=False)
async def account_courses(request: Request):
    return _render_account_page(request, workspace_active="courses", shell_active="courses")


@router.get("/account/calendar", include_in_schema=False)
async def account_calendar(request: Request):
    return _render_account_page(request, workspace_active="calendar", shell_active="calendar")


@router.get("/account/homework", include_in_schema=False)
async def account_homework_page(request: Request):
    return _render_account_page(request, workspace_active="homework", shell_active="homework")


@router.get("/account/lectures", include_in_schema=False)
async def account_lectures_page(request: Request):
    return _render_account_page(request, workspace_active="lectures", shell_active="lectures")


@router.get("/account/teachers", include_in_schema=False)
async def account_teachers_page(request: Request):
    return _render_account_page(request, workspace_active="teachers", shell_active="teachers")


@router.get("/account/documents", include_in_schema=False)
async def account_documents(request: Request):
    return _render_account_page(request, workspace_active="documents", shell_active="documents")


@router.get("/account/profile", include_in_schema=False)
async def account_profile(request: Request):
    return _render_account_page(request, workspace_active="settings", shell_active="profile")


@router.post("/account/profile/avatar", include_in_schema=False)
async def account_profile_avatar(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login?next=/account/profile", status_code=HTTP_302_FOUND)

    form = await request.form()
    next_url = str(form.get("next") or "/account/profile")
    upload = form.get("avatar")
    if not upload or not getattr(upload, "filename", ""):
        return _account_avatar_redirect(next_url, avatar_error="Выберите изображение профиля.")

    try:
        data = await upload.read(AVATAR_MAX_BYTES + 1)
    finally:
        close = getattr(upload, "close", None)
        if close:
            await close()

    if not data:
        return _account_avatar_redirect(next_url, avatar_error="Файл пустой. Выберите другое изображение.")
    if len(data) > AVATAR_MAX_BYTES:
        return _account_avatar_redirect(next_url, avatar_error="Фото слишком большое. Максимум 4 МБ.")

    extension = _detect_avatar_extension(data)
    if not extension:
        return _account_avatar_redirect(next_url, avatar_error="Поддерживаются только JPG, PNG, WebP и GIF.")

    PROFILE_AVATARS_DIR.mkdir(parents=True, exist_ok=True)
    user_id = str(user.get("id") or "")
    file_name = f"{_safe_avatar_file_stem(user_id)}-{int(time.time())}-{secrets.token_hex(4)}{extension}"
    file_path = PROFILE_AVATARS_DIR / file_name
    file_path.write_bytes(data)
    avatar_url = f"/profile_avatars/{file_name}"

    users = load_json(USERS_FILE, {})
    if not isinstance(users, dict):
        users = {}
    existing = users.get(user_id) if user_id else None
    if not isinstance(existing, dict):
        existing = {}
    previous_avatar_url = existing.get("avatar_url") or user.get("avatar_url")

    stored_user = {key: value for key, value in {**existing, **user}.items() if key != "photo_url"}
    stored_user["avatar_url"] = avatar_url
    if user_id:
        users[user_id] = stored_user
        save_json(USERS_FILE, users)

    session_user = dict(stored_user)
    set_current_user(request, session_user)
    if previous_avatar_url != avatar_url:
        _delete_uploaded_avatar(previous_avatar_url)

    return _account_avatar_redirect(next_url, avatar="updated")


@router.get("/api/account/schedule", include_in_schema=False)
async def account_schedule_api(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    agreements = _load_current_user_agreements(user)
    month = request.query_params.get("month") or ""
    payload = build_account_schedule_payload(agreements, month=month)
    return JSONResponse({"ok": True, **payload})


@router.get("/api/account/lectures", include_in_schema=False)
async def account_lectures_api(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    agreements = _load_current_user_agreements(user)
    try:
        page = max(int(request.query_params.get("page") or 1), 1)
    except Exception:
        page = 1
    try:
        per_page = max(int(request.query_params.get("per_page") or 6), 1)
    except Exception:
        per_page = 6
    payload = build_account_lectures_payload(
        agreements,
        q=str(request.query_params.get("q") or "").strip(),
        source_type=str(request.query_params.get("source") or "").strip(),
        course=str(request.query_params.get("course") or "").strip(),
        page=page,
        per_page=per_page,
    )
    return JSONResponse({"ok": True, **payload})


@router.get("/api/account/homework", include_in_schema=False)
async def account_homework_api(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    agreements = _load_current_user_agreements(user)
    payload = build_account_homework_payload(agreements)
    return JSONResponse({"ok": True, **payload})


@router.get("/api/account/teachers", include_in_schema=False)
async def account_teachers_api(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    agreements = _load_current_user_agreements(user)
    payload = build_account_teachers_payload(agreements)
    return JSONResponse({"ok": True, **payload})


@router.get("/api/account/lectures/{lecture_id}", include_in_schema=False)
async def account_lecture_detail_api(request: Request, lecture_id: str):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

    agreements = _load_current_user_agreements(user)
    allowed_files = {
        str(item.get("_file") or item.get("agreement_file") or "").strip()
        for item in agreements
        if str(item.get("_file") or item.get("agreement_file") or "").strip()
    }
    lecture = get_lecture_record(lecture_id)
    if not lecture or lecture.get("agreement_file") not in allowed_files:
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    return JSONResponse({"ok": True, "item": lecture})
