import csv
import html
import io
import logging
import math
import re
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

from telegram_bot import is_configured as telegram_is_configured
from telegram_bot import send_lead_message

import core
from core import (
    EXECUTOR_EMAIL,
    USERS_FILE,
    admin_required,
    build_contract_url,
    contract_channel_label,
    contract_status_from_item,
    find_student_by_code,
    find_student_by_phone,
    filter_items,
    get_admin_ids,
    load_agreements,
    load_json,
    load_leads,
    load_metrics,
    load_referrals,
    materials_to_text,
    moscow_now,
    normalize_materials,
    normalize_phone,
    normalize_referral_code,
    parse_date,
    render,
    save_referrals,
    send_email_message,
    save_whitelist,
    next_student_id,
    update_agreement_status,
    update_agreement_contract_status,
    update_lead_status,
)

router = APIRouter()

STATUS_META = {
    "new": ("Новая", "status-new"),
    "contacted": ("Связались", "status-warm"),
    "qualified": ("Квалифицирован", "status-warm"),
    "call_scheduled": ("Созвон", "status-new"),
    "paid": ("Оплачен", "status-good"),
    "lost": ("Потерян", "status-muted"),
    "in_progress": ("В работе", "status-warm"),
    "closed": ("Закрыта", "status-muted"),
    "archived": ("Архив", "status-muted"),
}

STATUS_OPTIONS = [
    ("auto", "Авто"),
    ("new", "Новая"),
    ("contacted", "Связались"),
    ("qualified", "Квалифицирован"),
    ("call_scheduled", "Созвон"),
    ("paid", "Оплачен"),
    ("lost", "Потерян"),
    ("in_progress", "В работе"),
    ("closed", "Закрыта"),
    ("archived", "Архив"),
]

AGREEMENT_STATUS_META = {
    "signed": ("Подписан", "status-good"),
    "paid": ("Оплачен", "status-new"),
    "review": ("На проверке", "status-warm"),
    "canceled": ("Отменён", "status-muted"),
}

AGREEMENT_STATUS_OPTIONS = [
    ("auto", "Авто"),
    ("signed", "Подписан"),
    ("paid", "Оплачен"),
    ("review", "На проверке"),
    ("canceled", "Отменён"),
]

CONTRACT_STATUS_OPTIONS = [
    ("draft", "Черновик"),
    ("sent", "Отправлен"),
    ("signed", "Подписан"),
]
CONTRACT_STATUS_LABELS = {key: label for key, label in CONTRACT_STATUS_OPTIONS}


def format_ts(value: Optional[int]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromtimestamp(int(value)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "—"


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def format_amount(value: Any) -> str:
    amount = parse_amount(value)
    if amount is None:
        return "—"
    if amount.is_integer():
        return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")


def normalize_tags(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        raw = ",".join([str(item) for item in value])
    else:
        raw = str(value)
    parts = [part.strip() for part in re.split(r"[,\n]+", raw) if part.strip()]
    return ", ".join(parts)


def format_date_input(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        return text
    try:
        return datetime.fromtimestamp(int(text)).strftime("%Y-%m-%d")
    except Exception:
        return ""


def referral_month_key(value: Optional[datetime] = None) -> str:
    dt = value or moscow_now()
    return dt.strftime("%Y-%m")


def referral_month_status(student: Dict[str, Any], month_key: str) -> Tuple[bool, bool]:
    months = student.get("months") or {}
    if not isinstance(months, dict):
        return False, False
    entry = months.get(month_key) or {}
    if not isinstance(entry, dict):
        return False, False
    return bool(entry.get("paid")), bool(entry.get("attended"))


def referral_confirmed_months(student: Dict[str, Any]) -> int:
    months = student.get("months") or {}
    if not isinstance(months, dict):
        return 0
    total = 0
    for entry in months.values():
        if isinstance(entry, dict) and entry.get("paid") and entry.get("attended"):
            total += 1
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
    balance = max(earned - applied, 0)
    overflow = max(balance - 100, 0)
    balance = min(balance, 100)
    return {
        "referrals": referrals,
        "referrals_count": len(referrals),
        "confirmed_months": confirmed,
        "earned": earned,
        "applied": applied,
        "balance": balance,
        "overflow": overflow,
    }


async def notify_admins(text: str, subject: str = "Реферальная программа") -> None:
    if not text:
        return
    if telegram_is_configured():
        try:
            await send_lead_message(text)
            return
        except Exception as exc:
            logging.getLogger("app.telegram").warning("Referral telegram notify failed: %s", exc)
    if EXECUTOR_EMAIL:
        try:
            send_email_message(EXECUTOR_EMAIL, subject, re.sub(r"<[^>]+>", "", text))
        except Exception as exc:
            logging.getLogger("app.email").warning("Referral email notify failed: %s", exc)


def referral_redirect(message: str = "", error: str = "") -> RedirectResponse:
    params = {"view": "referrals"}
    if message:
        params["ref_message"] = message
    if error:
        params["ref_error"] = error
    return RedirectResponse(f"/admin?{urlencode(params)}", status_code=HTTP_302_FOUND)


def status_from_item(item: Dict[str, Any]) -> Tuple[str, str, str]:
    manual = (item.get("status") or "").strip()
    if manual in STATUS_META:
        label, cls = STATUS_META[manual]
        return manual, label, cls

    ts = item.get("timestamp")
    if not ts:
        return "archived", STATUS_META["archived"][0], STATUS_META["archived"][1]
    try:
        delta = datetime.now() - datetime.fromtimestamp(safe_int(ts))
    except Exception:
        return "archived", STATUS_META["archived"][0], STATUS_META["archived"][1]

    if delta <= timedelta(days=1):
        return "new", STATUS_META["new"][0], STATUS_META["new"][1]
    if delta <= timedelta(days=7):
        return "in_progress", STATUS_META["in_progress"][0], STATUS_META["in_progress"][1]
    return "archived", STATUS_META["archived"][0], STATUS_META["archived"][1]


def agreement_status_from_item(item: Dict[str, Any]) -> Tuple[str, str, str]:
    manual = (item.get("status") or "").strip()
    if manual in AGREEMENT_STATUS_META:
        label, cls = AGREEMENT_STATUS_META[manual]
        return manual, label, cls
    return "signed", AGREEMENT_STATUS_META["signed"][0], AGREEMENT_STATUS_META["signed"][1]


def matches_query(item: Dict[str, Any], fields: List[str], query: str) -> bool:
    if not query:
        return True
    value = query.lower()
    for field in fields:
        raw = item.get(field)
        if raw is None:
            continue
        if value in str(raw).lower():
            return True
    return False


def apply_search(items: List[Dict[str, Any]], query: str, fields: List[str]) -> List[Dict[str, Any]]:
    if not query:
        return items
    return [item for item in items if matches_query(item, fields, query)]


def sort_items(items: List[Dict[str, Any]], sort_key: str, order: str, key_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    reverse = order != "asc"
    key_fn = key_map.get(sort_key) or key_map.get("date")
    return sorted(items, key=key_fn, reverse=reverse)


def extract_source(page: str) -> str:
    if not page:
        return "Прямой"
    try:
        parsed = urlparse(page)
        params = parse_qs(parsed.query)
        for key in ("utm_source", "source", "utm"):
            if key in params and params[key]:
                return params[key][0][:48]
        host = (parsed.netloc or "").lower()
    except Exception:
        host = ""

    page_lower = page.lower()
    for token, label in (
        ("google", "Google"),
        ("yandex", "Yandex"),
        ("vk.com", "VK"),
        ("vk", "VK"),
        ("t.me", "Telegram"),
        ("telegram", "Telegram"),
        ("youtube", "YouTube"),
        ("instagram", "Instagram"),
    ):
        if token in page_lower or token in host:
            return label

    if host:
        return host
    return "Прямой"


def extract_utm(page: str) -> Dict[str, str]:
    if not page:
        return {}
    try:
        parsed = urlparse(page)
        params = parse_qs(parsed.query)
    except Exception:
        return {}
    result = {}
    for key in ("utm_source", "utm_medium", "utm_campaign"):
        if key in params and params[key]:
            result[key] = params[key][0][:64]
    return result


def parse_amount(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(" ", "")
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except Exception:
        return None


def build_query(params: Dict[str, str], *, exclude: Optional[List[str]] = None) -> str:
    if exclude:
        for key in exclude:
            params.pop(key, None)
    if not params:
        return ""
    return f"?{urlencode(params)}"


def parse_page(value: Optional[str]) -> int:
    try:
        page = int(value or 1)
    except Exception:
        page = 1
    return max(page, 1)


def page_window(current: int, total: int, span: int = 2) -> List[Optional[int]]:
    if total <= 1:
        return [1]
    start = max(1, current - span)
    end = min(total, current + span)
    pages: List[Optional[int]] = []
    if start > 1:
        pages.append(1)
        if start > 2:
            pages.append(None)
    pages.extend(range(start, end + 1))
    if end < total:
        if end < total - 1:
            pages.append(None)
        pages.append(total)
    return pages


def render_admin_error(exc: Exception) -> HTMLResponse:
    logging.getLogger("app.admin").exception("Admin panel error")
    message = html.escape(str(exc) or exc.__class__.__name__)
    tb = html.escape("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
    return HTMLResponse(
        f"""
        <html>
          <head><meta charset="utf-8"><title>Admin error</title></head>
          <body style="background:#0b0f10;color:#e9eef0;font-family:monospace;padding:24px;">
            <h2>Ошибка админки</h2>
            <p>{message}</p>
            <details open>
              <summary>Traceback</summary>
              <pre style="white-space:pre-wrap;word-break:break-word;">{tb}</pre>
            </details>
          </body>
        </html>
        """,
        status_code=500,
    )


def bucket_counts(items: List[Dict[str, Any]], bucket: str, periods: int) -> List[Dict[str, Any]]:
    today = date.today()
    results = []
    if bucket == "week":
        current_start = today - timedelta(days=today.weekday())
        weeks = [current_start - timedelta(days=7 * i) for i in range(periods - 1, -1, -1)]
        counts = {start: 0 for start in weeks}
        for item in items:
            ts = item.get("timestamp")
            if not ts:
                continue
            try:
                d = datetime.fromtimestamp(safe_int(ts)).date()
            except Exception:
                continue
            start = d - timedelta(days=d.weekday())
            if start in counts:
                counts[start] += 1
        max_count = max(counts.values()) if counts else 1
        if max_count == 0:
            max_count = 1
        for start in weeks:
            label = f"{start.strftime('%d.%m')}"
            results.append({"label": label, "count": counts[start], "pct": round((counts[start] / max_count) * 100, 1)})
        return results

    if bucket == "month":
        months = []
        year = today.year
        month = today.month
        for _ in range(periods):
            months.append((year, month))
            month -= 1
            if month == 0:
                month = 12
                year -= 1
        months = list(reversed(months))
        counts = {m: 0 for m in months}
        for item in items:
            ts = item.get("timestamp")
            if not ts:
                continue
            try:
                d = datetime.fromtimestamp(safe_int(ts)).date()
            except Exception:
                continue
            key = (d.year, d.month)
            if key in counts:
                counts[key] += 1
        max_count = max(counts.values()) if counts else 1
        if max_count == 0:
            max_count = 1
        for year, month in months:
            label = f"{month:02d}.{str(year)[-2:]}"
            results.append({"label": label, "count": counts[(year, month)], "pct": round((counts[(year, month)] / max_count) * 100, 1)})
        return results
    return results


@router.get("/admin", include_in_schema=False)
async def admin_panel(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    try:
        return _admin_panel_impl(request)
    except Exception as exc:
        return render_admin_error(exc)


def _admin_panel_impl(request: Request):
    metrics = load_metrics()
    leads_all = load_leads()
    agreements_all = load_agreements()
    users_data = load_json(USERS_FILE, {})
    if not isinstance(users_data, dict):
        users_data = {}

    view = request.query_params.get("view") or "overview"
    allowed_views = {"overview", "leads", "agreements", "users", "whitelist", "referrals"}
    if view not in allowed_views:
        view = "overview"
    course = request.query_params.get("course") or ""
    date_from_value = request.query_params.get("date_from", "")
    date_to_value = request.query_params.get("date_to", "")
    query = (request.query_params.get("q") or "").strip()
    status_filter = request.query_params.get("status") or ""
    source_filter = request.query_params.get("source") or ""
    agreement_status_filter = request.query_params.get("agreement_status") or ""
    contract_status_filter = request.query_params.get("contract_status") or ""
    sort = request.query_params.get("sort") or "date"
    order = request.query_params.get("order") or "desc"
    limit_raw = request.query_params.get("limit") or "20"
    leads_page = parse_page(request.query_params.get("leads_page"))
    agreements_page = parse_page(request.query_params.get("agreements_page"))
    date_from = parse_date(date_from_value)
    date_to = parse_date(date_to_value)

    leads = filter_items(leads_all, course, date_from, date_to)
    agreements = filter_items(agreements_all, course, date_from, date_to)

    leads = apply_search(leads, query, ["name", "contact", "course", "page"])
    agreements = apply_search(agreements, query, ["full_name", "phone", "email", "telegram", "course"])

    leads = [{**item, "_source": extract_source(item.get("page", ""))} for item in leads]
    leads_base_count = len(leads)
    agreements_base_count = len(agreements)

    status_counts = {key: 0 for key in STATUS_META}
    source_counts: Dict[str, int] = {}
    for item in leads:
        status_key, _, _ = status_from_item(item)
        status_counts[status_key] = status_counts.get(status_key, 0) + 1
        source = item.get("_source") or "Прямой"
        source_counts[source] = source_counts.get(source, 0) + 1

    agreement_status_counts = {key: 0 for key in AGREEMENT_STATUS_META}
    contract_status_counts = {key: 0 for key in core.CONTRACT_STATUS_META}
    for item in agreements:
        agreement_key, _, _ = agreement_status_from_item(item)
        agreement_status_counts[agreement_key] = agreement_status_counts.get(agreement_key, 0) + 1
        contract_key, _, _ = contract_status_from_item(item)
        contract_status_counts[contract_key] = contract_status_counts.get(contract_key, 0) + 1

    if status_filter and status_filter not in STATUS_META:
        status_filter = ""
    if status_filter:
        leads = [item for item in leads if status_from_item(item)[0] == status_filter]
    if source_filter:
        leads = [item for item in leads if item.get("_source") == source_filter]

    if agreement_status_filter and agreement_status_filter not in AGREEMENT_STATUS_META:
        agreement_status_filter = ""
    if contract_status_filter and contract_status_filter not in core.CONTRACT_STATUS_META:
        contract_status_filter = ""
    if agreement_status_filter:
        agreements = [item for item in agreements if agreement_status_from_item(item)[0] == agreement_status_filter]
    if contract_status_filter:
        agreements = [item for item in agreements if contract_status_from_item(item)[0] == contract_status_filter]

    pipeline_order = ["new", "contacted", "qualified", "call_scheduled", "paid", "lost", "archived"]
    pipeline_labels = {
        "new": "Новый",
        "contacted": "Связались",
        "qualified": "Квалифицирован",
        "call_scheduled": "Созвон",
        "paid": "Оплатил",
        "lost": "Потерян",
        "archived": "Архив",
    }
    pipeline_counts = {key: 0 for key in pipeline_order}
    for item in leads:
        status_key, _, _ = status_from_item(item)
        if status_key == "in_progress":
            status_key = "contacted"
        if status_key == "closed":
            status_key = "lost"
        if status_key not in pipeline_counts:
            status_key = "archived"
        pipeline_counts[status_key] += 1
    pipeline_max = max(pipeline_counts.values()) if pipeline_counts else 1
    if pipeline_max == 0:
        pipeline_max = 1
    pipeline_steps = [
        {
            "key": key,
            "label": pipeline_labels.get(key, key),
            "count": pipeline_counts.get(key, 0),
            "pct": round((pipeline_counts.get(key, 0) / pipeline_max) * 100, 1),
        }
        for key in pipeline_order
    ]

    utm_source_counts: Dict[str, int] = {}
    utm_medium_counts: Dict[str, int] = {}
    utm_campaign_counts: Dict[str, int] = {}
    for item in leads:
        utm = extract_utm(item.get("page", ""))
        if utm.get("utm_source"):
            utm_source_counts[utm["utm_source"]] = utm_source_counts.get(utm["utm_source"], 0) + 1
        if utm.get("utm_medium"):
            utm_medium_counts[utm["utm_medium"]] = utm_medium_counts.get(utm["utm_medium"], 0) + 1
        if utm.get("utm_campaign"):
            utm_campaign_counts[utm["utm_campaign"]] = utm_campaign_counts.get(utm["utm_campaign"], 0) + 1
    utm_sources_sorted = sorted(utm_source_counts.items(), key=lambda item: item[1], reverse=True)[:6]
    utm_mediums_sorted = sorted(utm_medium_counts.items(), key=lambda item: item[1], reverse=True)[:6]
    utm_campaigns_sorted = sorted(utm_campaign_counts.items(), key=lambda item: item[1], reverse=True)[:6]

    stale_leads = []
    response_times = []
    now_dt = datetime.now()
    for item in leads_all:
        ts = item.get("timestamp")
        if not ts:
            continue
        created_at = datetime.fromtimestamp(safe_int(ts))
        status_key, _, _ = status_from_item(item)
        if status_key == "new":
            age_hours = (now_dt - created_at).total_seconds() / 3600
            if age_hours >= 24:
                stale_leads.append(
                    {
                        "name": item.get("name") or "Без имени",
                        "course": item.get("course") or "—",
                        "age": round(age_hours),
                    }
                )
        updated_at = item.get("status_updated_at")
        if updated_at:
            delta_minutes = (datetime.fromtimestamp(safe_int(updated_at)) - created_at).total_seconds() / 60
            if delta_minutes >= 0:
                response_times.append(delta_minutes)
    stale_leads = sorted(stale_leads, key=lambda item: item.get("age", 0), reverse=True)[:6]
    avg_response = round(sum(response_times) / len(response_times), 1) if response_times else 0

    def lead_sort_key(item: Dict[str, Any]):
        status_key, _, _ = status_from_item(item)
        order_map = {
            "new": 0,
            "contacted": 1,
            "qualified": 2,
            "call_scheduled": 3,
            "paid": 4,
            "lost": 5,
            "in_progress": 2,
            "closed": 6,
            "archived": 7,
        }
        return (order_map.get(status_key, 3), safe_int(item.get("timestamp", 0)))

    def agreement_status_sort_key(item: Dict[str, Any]):
        status_key, _, _ = agreement_status_from_item(item)
        order_map = {
            "signed": 0,
            "paid": 1,
            "review": 2,
            "canceled": 3,
        }
        return (order_map.get(status_key, 4), safe_int(item.get("timestamp", 0)))

    def contract_status_sort_key(item: Dict[str, Any]):
        status_key, _, _ = contract_status_from_item(item)
        order_map = {
            "draft": 0,
            "sent": 1,
            "signed": 2,
        }
        return (order_map.get(status_key, 3), safe_int(item.get("timestamp", 0)))

    lead_key_map = {
        "date": lambda item: safe_int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
        "status": lead_sort_key,
    }
    agreement_key_map = {
        "date": lambda item: safe_int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("full_name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
        "status": agreement_status_sort_key,
        "contract": contract_status_sort_key,
    }

    leads = sort_items(leads, sort, order, lead_key_map)
    agreements = sort_items(agreements, sort, order, agreement_key_map)

    limit_value = None
    if limit_raw and limit_raw.lower() not in {"all", "0"}:
        try:
            limit_value = max(int(limit_raw), 1)
        except Exception:
            limit_value = 20

    def paginate(items: List[Dict[str, Any]], page: int, per_page: Optional[int]):
        if not per_page:
            return items, 1, 1
        total_pages = max(1, math.ceil(len(items) / per_page))
        page = max(1, min(page, total_pages))
        start = (page - 1) * per_page
        end = start + per_page
        return items[start:end], page, total_pages

    leads_display, leads_page, leads_pages = paginate(leads, leads_page, limit_value)
    agreements_display, agreements_page, agreements_pages = paginate(agreements, agreements_page, limit_value)

    courses = sorted(
        {item.get("course") for item in (leads_all + agreements_all) if item.get("course")}
    )

    leads_total = len(leads_all)
    agreements_total = len(agreements_all)
    users_total = len(users_data)
    leads_count = len(leads)
    agreements_count = len(agreements)

    leads_view = []
    for item in leads_display:
        status_key, status_label, status_class = status_from_item(item)
        manual_status = (item.get("status") or "").strip()
        tags = normalize_tags(item.get("tags"))
        note = str(item.get("note") or "").strip()
        next_contact = format_date_input(item.get("next_contact"))
        leads_view.append(
            {
                **item,
                "display_time": format_ts(item.get("timestamp")),
                "status_label": status_label,
                "status_class": status_class,
                "status_key": status_key,
                "manual_status": manual_status,
                "source": item.get("_source") or extract_source(item.get("page", "")),
                "tags": tags,
                "note": note,
                "next_contact": next_contact,
            }
        )

    agreements_view = []
    for item in agreements_display:
        status_key, status_label, status_class = agreement_status_from_item(item)
        contract_key, contract_label, contract_class = contract_status_from_item(item)
        manual_status = (item.get("status") or "").strip()
        amount_display = format_amount(item.get("amount"))
        total_lessons = safe_int(item.get("total_lessons"), 0) if item.get("total_lessons") is not None else None
        paid_lessons = safe_int(item.get("paid_lessons"), 0) if item.get("paid_lessons") is not None else None
        remaining_lessons = None
        if total_lessons is not None and paid_lessons is not None:
            remaining_lessons = max(total_lessons - paid_lessons, 0)
        materials = normalize_materials(item.get("materials"))
        agreements_view.append(
            {
                **item,
                "display_time": format_ts(item.get("timestamp")),
                "status_label": status_label,
                "status_class": status_class,
                "status_key": status_key,
                "manual_status": manual_status,
                "amount_display": amount_display,
                "contract_number": item.get("contract_number") or "—",
                "contract_status_key": contract_key,
                "contract_status_label": contract_label,
                "contract_status_class": contract_class,
                "contract_channel_label": contract_channel_label(item.get("contract_channel")),
                "contract_url": build_contract_url(item.get("contract_token"), request),
                "contract_pdf_url": item.get("contract_pdf_url"),
                "contract_sent_at": format_ts(item.get("contract_sent_at")),
                "contract_signed_at": format_ts(item.get("contract_signed_at")),
                "total_lessons": total_lessons,
                "paid_lessons": paid_lessons,
                "remaining_lessons": remaining_lessons,
                "current_module": item.get("current_module") or "",
                "materials_text": materials_to_text(materials),
                "materials_count": len(materials),
            }
        )

    users_view = []
    for user in users_data.values():
        if not isinstance(user, dict):
            continue
        users_view.append(
            {
                "id": user.get("id"),
                "provider": user.get("provider", "—"),
                "email": user.get("email") or "—",
                "name": user.get("name") or user.get("email") or user.get("id") or "—",
            }
        )
    users_view.sort(key=lambda item: (str(item.get("provider")), str(item.get("name"))))

    filter_bits = []
    if course:
        filter_bits.append(f"Курс: {course}")
    if status_filter:
        status_label = STATUS_META.get(status_filter, (status_filter, ""))[0]
        filter_bits.append(f"Статус: {status_label}")
    if source_filter:
        filter_bits.append(f"Источник: {source_filter}")
    if agreement_status_filter:
        agreement_label = AGREEMENT_STATUS_META.get(agreement_status_filter, (agreement_status_filter, ""))[0]
        filter_bits.append(f"Статус покупки: {agreement_label}")
    if contract_status_filter:
        contract_label = CONTRACT_STATUS_LABELS.get(contract_status_filter, contract_status_filter)
        filter_bits.append(f"Статус договора: {contract_label}")
    if query:
        filter_bits.append(f"Поиск: {query}")
    if date_from_value or date_to_value:
        def fmt_date(value: str) -> str:
            if not value:
                return "…"
            try:
                return datetime.strptime(value, "%Y-%m-%d").strftime("%d.%m.%Y")
            except Exception:
                return value
        filter_bits.append(f"Период: {fmt_date(date_from_value)} — {fmt_date(date_to_value)}")
    filters_label = " · ".join(filter_bits) if filter_bits else "Все данные"

    params = {}
    if view != "overview":
        params["view"] = view
    if course:
        params["course"] = course
    if date_from_value:
        params["date_from"] = date_from_value
    if date_to_value:
        params["date_to"] = date_to_value
    if query:
        params["q"] = query
    if status_filter:
        params["status"] = status_filter
    if source_filter:
        params["source"] = source_filter
    if agreement_status_filter:
        params["agreement_status"] = agreement_status_filter
    if contract_status_filter:
        params["contract_status"] = contract_status_filter
    if sort:
        params["sort"] = sort
    if order:
        params["order"] = order
    if limit_raw:
        params["limit"] = limit_raw
    if leads_page > 1:
        params["leads_page"] = str(leads_page)
    if agreements_page > 1:
        params["agreements_page"] = str(agreements_page)

    filters_query = build_query(dict(params))
    export_query = build_query(dict(params), exclude=["limit", "leads_page", "agreements_page"])

    status_filter_options = [("", "Все", leads_base_count)]
    for key, (label, _) in STATUS_META.items():
        status_filter_options.append((key, label, status_counts.get(key, 0)))

    source_items = sorted(source_counts.items(), key=lambda item: item[1], reverse=True)
    source_filter_options = [("", "Все", leads_base_count)]
    for source, count in source_items:
        source_filter_options.append((source, source, count))

    agreement_status_filter_options = [("", "Все", agreements_base_count)]
    for key, (label, _) in AGREEMENT_STATUS_META.items():
        agreement_status_filter_options.append((key, label, agreement_status_counts.get(key, 0)))

    contract_status_filter_options = [("", "Все", agreements_base_count)]
    for key, label in CONTRACT_STATUS_OPTIONS:
        contract_status_filter_options.append((key, label, contract_status_counts.get(key, 0)))

    def build_filter_links(options, param_name: str, active_value: str):
        links = []
        for key, label, count in options:
            link_params = dict(params)
            if key:
                link_params[param_name] = key
            else:
                link_params.pop(param_name, None)
            if param_name in {"status", "source"}:
                link_params.pop("leads_page", None)
            if param_name in {"agreement_status", "contract_status"}:
                link_params.pop("agreements_page", None)
            url = f"/admin{build_query(link_params)}"
            links.append(
                {
                    "label": label,
                    "count": count,
                    "url": url,
                    "active": (key == active_value) or (not key and not active_value),
                }
            )
        return links

    status_filters = build_filter_links(status_filter_options, "status", status_filter)
    source_filters = build_filter_links(source_filter_options, "source", source_filter)
    agreement_status_filters = build_filter_links(
        agreement_status_filter_options, "agreement_status", agreement_status_filter
    )
    contract_status_filters = build_filter_links(
        contract_status_filter_options, "contract_status", contract_status_filter
    )

    def build_pagination(current_page: int, total_pages: int, page_param: str):
        if total_pages <= 1:
            return {
                "page": current_page,
                "total_pages": total_pages,
                "links": [],
                "has_prev": False,
                "has_next": False,
                "prev_url": "",
                "next_url": "",
            }
        base_params = dict(params)

        def make_page_url(page: int) -> str:
            link_params = dict(base_params)
            if page <= 1:
                link_params.pop(page_param, None)
            else:
                link_params[page_param] = str(page)
            return f"/admin{build_query(link_params)}"

        links = []
        for page in page_window(current_page, total_pages):
            if page is None:
                links.append({"ellipsis": True})
            else:
                links.append({"page": page, "url": make_page_url(page), "active": page == current_page})
        return {
            "page": current_page,
            "total_pages": total_pages,
            "links": links,
            "has_prev": current_page > 1,
            "has_next": current_page < total_pages,
            "prev_url": make_page_url(max(1, current_page - 1)),
            "next_url": make_page_url(min(total_pages, current_page + 1)),
        }

    leads_pagination = build_pagination(leads_page, leads_pages, "leads_page")
    agreements_pagination = build_pagination(agreements_page, agreements_pages, "agreements_page")

    path_counts = metrics.get("path_counts", {})
    path_counts_sorted = sorted(path_counts.items(), key=lambda item: item[1], reverse=True)

    def pct(part: int, total: int) -> float:
        if not total:
            return 0.0
        return round((part / total) * 100, 1)

    funnel = metrics.get("funnel", {})
    funnel_home = int(funnel.get("home", 0) or 0)
    funnel_login = int(funnel.get("login", 0) or 0)
    funnel_apply = int(funnel.get("apply", 0) or 0)
    funnel_enroll = int(funnel.get("enroll", 0) or 0)
    funnel_max = max(funnel_home, funnel_login, funnel_apply, funnel_enroll, 1)
    funnel_steps = [
        {
            "label": "Главная",
            "count": funnel_home,
            "pct": pct(funnel_home, funnel_max),
            "rate": "100%",
        },
        {
            "label": "Логин",
            "count": funnel_login,
            "pct": pct(funnel_login, funnel_max),
            "rate": f"{pct(funnel_login, funnel_home)}%",
        },
        {
            "label": "Заявки",
            "count": funnel_apply,
            "pct": pct(funnel_apply, funnel_max),
            "rate": f"{pct(funnel_apply, funnel_login)}%",
        },
        {
            "label": "Покупки",
            "count": funnel_enroll,
            "pct": pct(funnel_enroll, funnel_max),
            "rate": f"{pct(funnel_enroll, funnel_apply)}%",
        },
    ]

    today = date.today()
    days = [today - timedelta(days=offset) for offset in range(6, -1, -1)]
    lead_counts = {d: 0 for d in days}
    enroll_counts = {d: 0 for d in days}
    for item in leads_all:
        ts = item.get("timestamp")
        if ts:
            d = datetime.fromtimestamp(safe_int(ts)).date()
            if d in lead_counts:
                lead_counts[d] += 1
    for item in agreements_all:
        ts = item.get("timestamp")
        if ts:
            d = datetime.fromtimestamp(safe_int(ts)).date()
            if d in enroll_counts:
                enroll_counts[d] += 1
    lead_max = max(lead_counts.values()) if lead_counts else 1
    enroll_max = max(enroll_counts.values()) if enroll_counts else 1
    lead_chart = [
        {"label": d.strftime("%d.%m"), "count": lead_counts[d], "pct": pct(lead_counts[d], lead_max)}
        for d in days
    ]
    enroll_chart = [
        {"label": d.strftime("%d.%m"), "count": enroll_counts[d], "pct": pct(enroll_counts[d], enroll_max)}
        for d in days
    ]

    weekly_leads = bucket_counts(leads_all, "week", 8)
    weekly_enrolls = bucket_counts(agreements_all, "week", 8)
    monthly_leads = bucket_counts(leads_all, "month", 6)
    monthly_enrolls = bucket_counts(agreements_all, "month", 6)

    last_ts = 0
    for item in (leads_all + agreements_all):
        ts = item.get("timestamp")
        if ts:
            ts_value = safe_int(ts)
            if ts_value > last_ts:
                last_ts = ts_value
    last_activity = format_ts(last_ts) if last_ts else "—"

    def count_recent(items: List[Dict[str, Any]], hours: int) -> int:
        if not items:
            return 0
        since = datetime.now() - timedelta(hours=hours)
        total = 0
        for item in items:
            ts = item.get("timestamp")
            if not ts:
                continue
            try:
                if datetime.fromtimestamp(safe_int(ts)) >= since:
                    total += 1
            except Exception:
                continue
        return total

    leads_24h = count_recent(leads_all, 24)
    enroll_24h = count_recent(agreements_all, 24)
    leads_7d = sum(lead_counts.values())
    enroll_7d = sum(enroll_counts.values())

    agreement_amounts = []
    paid_count = 0
    for item in agreements_all:
        amount = parse_amount(item.get("amount"))
        if amount is not None:
            agreement_amounts.append(amount)
        if agreement_status_from_item(item)[0] == "paid":
            paid_count += 1
    revenue_total = round(sum(agreement_amounts), 2) if agreement_amounts else 0
    revenue_avg = round((revenue_total / len(agreement_amounts)), 2) if agreement_amounts else 0

    kpis = [
        {
            "label": "Посещения",
            "value": metrics.get("total_visits", 0),
            "note": "Все визиты сайта",
        },
        {
            "label": "Уникальные",
            "value": metrics.get("unique_visits", 0),
            "note": "Сессии пользователей",
        },
        {
            "label": "Заявки",
            "value": funnel_apply,
            "note": f"За 24ч: {leads_24h} · 7 дней: {leads_7d}",
        },
        {
            "label": "Покупки",
            "value": funnel_enroll,
            "note": f"За 24ч: {enroll_24h} · 7 дней: {enroll_7d}",
        },
        {
            "label": "Ответ на лид",
            "value": f"{avg_response} мин" if avg_response else "—",
            "note": f"Просрочено (>24ч): {len(stale_leads)}",
        },
        {
            "label": "Выручка",
            "value": f"{format_amount(revenue_total)} ₽" if revenue_total else "—",
            "note": f"Средний чек: {format_amount(revenue_avg)} ₽ · Оплат: {paid_count}",
        },
        {
            "label": "Конверсия в заявку",
            "value": f"{pct(funnel_apply, metrics.get('unique_visits', 0))}%",
            "note": "От уникальных визитов",
        },
        {
            "label": "Конверсия в покупку",
            "value": f"{pct(funnel_enroll, funnel_apply)}%",
            "note": "От заявок",
        },
    ]

    course_counts = {}
    agreement_counts = {}
    for item in leads_all:
        course_name = item.get("course")
        if course_name:
            course_counts[course_name] = course_counts.get(course_name, 0) + 1
    for item in agreements_all:
        course_name = item.get("course")
        if course_name:
            agreement_counts[course_name] = agreement_counts.get(course_name, 0) + 1
    top_courses = sorted(course_counts.items(), key=lambda item: item[1], reverse=True)[:6]
    top_agreements = sorted(agreement_counts.items(), key=lambda item: item[1], reverse=True)[:6]

    source_counts = {}
    for item in leads_all:
        source = extract_source(item.get("page", ""))
        source_counts[source] = source_counts.get(source, 0) + 1
    sources_sorted = sorted(source_counts.items(), key=lambda item: item[1], reverse=True)[:6]

    recent_leads = []
    for item in leads_all[:6]:
        _, label, cls = status_from_item(item)
        recent_leads.append(
            {
                "name": item.get("name") or "Без имени",
                "time": format_ts(item.get("timestamp")),
                "course": item.get("course") or "—",
                "status_label": label,
                "status_class": cls,
            }
        )

    referral_message = request.query_params.get("ref_message") or ""
    referral_error = request.query_params.get("ref_error") or ""
    referral_current_month = referral_month_key()
    referral_participants = []
    referral_students = []
    referral_top = []
    referral_stats = {
        "participants": 0,
        "referrals": 0,
        "confirmed_months": 0,
        "earned": 0,
        "applied": 0,
        "balance": 0,
        "overflow": 0,
    }
    if view == "referrals":
        referrals_data = load_referrals()
        students = referrals_data.get("students") or {}
        if not isinstance(students, dict):
            students = {}
        participants = [
            item for item in students.values()
            if normalize_referral_code(item.get("referral_code", ""))
        ]
        for item in sorted(participants, key=lambda s: (s.get("name") or "", s.get("id") or 0)):
            stats = referral_stats_for_referrer(item, students)
            applied_list = item.get("discount_applied") if isinstance(item.get("discount_applied"), list) else []
            last_applied = format_ts(applied_list[-1].get("ts")) if applied_list else "—"
            referral_participants.append(
                {
                    "id": item.get("id"),
                    "name": item.get("name") or "—",
                    "phone": item.get("phone") or "—",
                    "code": item.get("referral_code") or "—",
                    "referrals_count": stats["referrals_count"],
                    "confirmed_months": stats["confirmed_months"],
                    "earned": stats["earned"],
                    "applied": stats["applied"],
                    "balance": stats["balance"],
                    "overflow": stats["overflow"],
                    "last_applied": last_applied,
                }
            )
            referral_stats["participants"] += 1
            referral_stats["referrals"] += stats["referrals_count"]
            referral_stats["confirmed_months"] += stats["confirmed_months"]
            referral_stats["earned"] += stats["earned"]
            referral_stats["applied"] += stats["applied"]
            referral_stats["balance"] += stats["balance"]
            referral_stats["overflow"] += stats["overflow"]

        referral_top = sorted(
            referral_participants,
            key=lambda item: (item.get("confirmed_months", 0), item.get("referrals_count", 0)),
            reverse=True,
        )[:6]

        for item in sorted(students.values(), key=lambda s: (s.get("name") or "", s.get("id") or 0)):
            if not item.get("referrer_id"):
                continue
            referrer = students.get(str(item.get("referrer_id"))) if students else None
            month_paid, month_attended = referral_month_status(item, referral_current_month)
            referral_students.append(
                {
                    "id": item.get("id"),
                    "name": item.get("name") or "—",
                    "phone": item.get("phone") or "—",
                    "group": item.get("group") or "",
                    "referrer_name": (referrer or {}).get("name") or "—",
                    "referrer_code": (referrer or {}).get("referral_code") or "—",
                    "referrer_id": (referrer or {}).get("id"),
                    "confirmed_months": referral_confirmed_months(item),
                    "month_paid": month_paid,
                    "month_attended": month_attended,
                }
            )

    next_url = f"/admin{request.url.query and ('?' + request.url.query) or ''}"

    return render(
        request,
        "admin.html",
        {
            "metrics": metrics,
            "leads": leads_view,
            "agreements": agreements_view,
            "users": users_view,
            "whitelist": core.WHITELIST_IDS,
            "admin_ids": get_admin_ids(),
            "path_counts_sorted": path_counts_sorted,
            "user": request.session.get("user"),
            "courses": courses,
            "view": view,
            "filters": {
                "course": course,
                "date_from": date_from_value,
                "date_to": date_to_value,
                "query": query,
                "status": status_filter,
                "source": source_filter,
                "agreement_status": agreement_status_filter,
                "contract_status": contract_status_filter,
                "sort": sort,
                "order": order,
                "limit": limit_raw,
                "leads_page": leads_page,
                "agreements_page": agreements_page,
            },
            "lead_chart": lead_chart,
            "enroll_chart": enroll_chart,
            "weekly_leads": weekly_leads,
            "weekly_enrolls": weekly_enrolls,
            "monthly_leads": monthly_leads,
            "monthly_enrolls": monthly_enrolls,
            "funnel_steps": funnel_steps,
            "pipeline_steps": pipeline_steps,
            "kpis": kpis,
            "filters_label": filters_label,
            "filters_query": filters_query,
            "export_query": export_query,
            "leads_total": leads_total,
            "agreements_total": agreements_total,
            "users_total": users_total,
            "leads_count": leads_count,
            "agreements_count": agreements_count,
            "last_activity": last_activity,
            "top_courses": top_courses,
            "top_agreements": top_agreements,
            "sources_sorted": sources_sorted,
            "status_options": STATUS_OPTIONS,
            "agreement_status_options": AGREEMENT_STATUS_OPTIONS,
            "contract_status_options": CONTRACT_STATUS_OPTIONS,
            "status_filters": status_filters,
            "source_filters": source_filters,
            "agreement_status_filters": agreement_status_filters,
            "contract_status_filters": contract_status_filters,
            "leads_pagination": leads_pagination,
            "agreements_pagination": agreements_pagination,
            "next_url": next_url,
            "recent_leads": recent_leads,
            "utm_sources": utm_sources_sorted,
            "utm_mediums": utm_mediums_sorted,
            "utm_campaigns": utm_campaigns_sorted,
            "stale_leads": stale_leads,
            "avg_response": avg_response,
            "referral_message": referral_message,
            "referral_error": referral_error,
            "referral_current_month": referral_current_month,
            "referral_participants": referral_participants,
            "referral_students": referral_students,
            "referral_stats": referral_stats,
            "referral_top": referral_top,
        },
    )


@router.get("/admin/leads/statuses", include_in_schema=False)
async def admin_lead_statuses(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    files_raw = request.query_params.get("files") or ""
    files = [item.strip() for item in files_raw.split(",") if item.strip()]
    items = []
    for file_name in files:
        path = core.LEADS_DIR / file_name
        if not path.exists():
            continue
        data = load_json(path, {})
        if not isinstance(data, dict):
            continue
        status_key, status_label, status_class = status_from_item(data)
        manual_status = (data.get("status") or "").strip()
        items.append(
            {
                "file": file_name,
                "status_key": status_key,
                "status_label": status_label,
                "status_class": status_class,
                "manual_status": manual_status,
            }
        )
    return JSONResponse({"items": items})


@router.post("/admin/metrics/reset", include_in_schema=False)
async def admin_reset_metrics(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    metrics = {
        "total_visits": 0,
        "unique_visits": 0,
        "unique_ids": {},
        "path_counts": {},
        "funnel": {"home": 0, "login": 0, "apply": 0, "enroll": 0},
    }
    core.save_metrics(metrics)
    return RedirectResponse("/admin?view=overview", status_code=HTTP_302_FOUND)


@router.post("/admin/leads/clear", include_in_schema=False)
async def admin_clear_leads(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    deleted = 0
    for path in core.LEADS_DIR.glob("lead_*.json"):
        try:
            path.unlink()
            deleted += 1
        except Exception:
            continue
    if telegram_is_configured():
        try:
            await send_lead_message(f"🗑 Удалены все заявки: {deleted}")
        except Exception:
            logging.getLogger("app.telegram").warning("Telegram lead clear sync failed.", exc_info=True)
    return RedirectResponse("/admin?view=leads", status_code=HTTP_302_FOUND)


@router.post("/admin/leads/delete", include_in_schema=False)
async def admin_delete_lead(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    next_url = str(form.get("next") or "/admin?view=leads")
    if not file_name:
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    path = core.LEADS_DIR / file_name
    lead_name = ""
    lead_contact = ""
    if path.exists():
        data = load_json(path, {})
        if isinstance(data, dict):
            lead_name = str(data.get("name") or "").strip()
            lead_contact = str(data.get("contact") or "").strip()
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass
    if telegram_is_configured():
        try:
            label = lead_name or file_name
            contact = f" ({lead_contact})" if lead_contact else ""
            await send_lead_message(f"🗑 Заявка удалена: {label}{contact}")
        except Exception:
            logging.getLogger("app.telegram").warning("Telegram lead delete sync failed.", exc_info=True)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/agreements/clear", include_in_schema=False)
async def admin_clear_agreements(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    deleted = 0
    for path in core.AGREEMENTS_DIR.glob("agreement_*.json"):
        try:
            path.unlink()
            deleted += 1
        except Exception:
            continue
    if telegram_is_configured():
        try:
            await send_lead_message(f"🗑 Удалены все договоры: {deleted}")
        except Exception:
            logging.getLogger("app.telegram").warning("Telegram agreements clear sync failed.", exc_info=True)
    return RedirectResponse("/admin?view=agreements", status_code=HTTP_302_FOUND)


@router.post("/admin/agreements/delete", include_in_schema=False)
async def admin_delete_agreement(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    next_url = str(form.get("next") or "/admin?view=agreements")
    if not file_name:
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    path = core.AGREEMENTS_DIR / file_name
    agreement_name = ""
    agreement_course = ""
    if path.exists():
        data = load_json(path, {})
        if isinstance(data, dict):
            agreement_name = str(data.get("full_name") or "").strip()
            agreement_course = str(data.get("course") or "").strip()
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass
    if telegram_is_configured():
        try:
            label = agreement_name or file_name
            course = f" — {agreement_course}" if agreement_course else ""
            await send_lead_message(f"🗑 Договор удалён: {label}{course}")
        except Exception:
            logging.getLogger("app.telegram").warning("Telegram agreement delete sync failed.", exc_info=True)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/leads/status", include_in_schema=False)
async def admin_update_lead_status(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    status = str(form.get("status") or "").strip()
    next_url = str(form.get("next") or "/admin")
    if status == "auto":
        status = ""
    update_lead_status(file_name, status)
    if telegram_is_configured():
        try:
            await send_lead_message("Обновление статуса", lead_file=file_name)
        except Exception:
            logging.getLogger("app.telegram").warning("Telegram status sync failed.", exc_info=True)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/leads/meta", include_in_schema=False)
async def admin_update_lead_meta(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    next_url = str(form.get("next") or "/admin")
    tags = normalize_tags(form.get("tags"))
    note = str(form.get("note") or "").strip()
    next_contact = str(form.get("next_contact") or "").strip()
    if not file_name:
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    path = core.LEADS_DIR / file_name
    if not path.exists():
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    data = load_json(path, {})
    if not isinstance(data, dict):
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    if tags:
        data["tags"] = tags
    else:
        data.pop("tags", None)
    if note:
        data["note"] = note
    else:
        data.pop("note", None)
    if next_contact and re.match(r"^\d{4}-\d{2}-\d{2}$", next_contact):
        data["next_contact"] = next_contact
    else:
        data.pop("next_contact", None)
    save_json(path, data)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/agreements/status", include_in_schema=False)
async def admin_update_agreement_status(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    status = str(form.get("status") or "").strip()
    next_url = str(form.get("next") or "/admin")
    if status == "auto":
        status = ""
    update_agreement_status(file_name, status)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/agreements/contract-status", include_in_schema=False)
async def admin_update_agreement_contract_status(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    status = str(form.get("contract_status") or "").strip()
    next_url = str(form.get("next") or "/admin")
    update_agreement_contract_status(file_name, status)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/agreements/progress", include_in_schema=False)
async def admin_update_agreement_progress(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    next_url = str(form.get("next") or "/admin")
    total_lessons_raw = str(form.get("total_lessons") or "").strip()
    paid_lessons_raw = str(form.get("paid_lessons") or "").strip()
    current_module = str(form.get("current_module") or "").strip()
    materials_raw = str(form.get("materials") or "").strip()
    if not file_name:
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    path = core.AGREEMENTS_DIR / file_name
    if not path.exists():
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    data = load_json(path, {})
    if not isinstance(data, dict):
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)

    try:
        total_lessons = int(total_lessons_raw) if total_lessons_raw else None
    except Exception:
        total_lessons = None
    try:
        paid_lessons = int(paid_lessons_raw) if paid_lessons_raw else None
    except Exception:
        paid_lessons = None

    if total_lessons is not None:
        data["total_lessons"] = total_lessons
    else:
        data.pop("total_lessons", None)

    if paid_lessons is not None:
        data["paid_lessons"] = paid_lessons
    else:
        data.pop("paid_lessons", None)

    if current_module:
        data["current_module"] = current_module
    else:
        data.pop("current_module", None)

    materials = normalize_materials(materials_raw)
    if materials:
        data["materials"] = materials
    else:
        data.pop("materials", None)

    save_json(path, data)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/agreements/amount", include_in_schema=False)
async def admin_update_agreement_amount(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    file_name = str(form.get("file") or "").strip()
    next_url = str(form.get("next") or "/admin")
    amount_raw = str(form.get("amount") or "").strip()
    if not file_name:
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    path = core.AGREEMENTS_DIR / file_name
    if not path.exists():
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    data = load_json(path, {})
    if not isinstance(data, dict):
        return RedirectResponse(next_url, status_code=HTTP_302_FOUND)
    amount = parse_amount(amount_raw)
    if amount is not None:
        data["amount"] = amount
    else:
        data.pop("amount", None)
    save_json(path, data)
    return RedirectResponse(next_url, status_code=HTTP_302_FOUND)


@router.post("/admin/whitelist", include_in_schema=False)
async def admin_update_whitelist(request: Request):
    guard = admin_required(request)
    if guard:
        return guard

    form = await request.form()
    raw = str(form.get("whitelist", "")).strip()
    ids = []
    for part in re.split(r"[,\n ]+", raw):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except Exception:
            continue
    if ids:
        core.WHITELIST_IDS = ids
        save_whitelist(ids)
    return RedirectResponse("/admin?view=whitelist", status_code=HTTP_302_FOUND)


@router.post("/admin/whitelist/add", include_in_schema=False)
async def admin_add_whitelist(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    raw_id = str(form.get("id") or "").strip()
    role = str(form.get("role") or "admin").strip().lower()
    try:
        new_id = int(raw_id)
    except Exception:
        return RedirectResponse("/admin?view=whitelist", status_code=HTTP_302_FOUND)

    ids = [item for item in core.WHITELIST_IDS if item != new_id]
    if role == "broadcast":
        ids.append(new_id)
    else:
        if len(ids) >= 2:
            last = ids[-1]
            ids = ids[:-1] + [new_id, last]
        else:
            ids.append(new_id)

    core.WHITELIST_IDS = ids
    save_whitelist(ids)
    return RedirectResponse("/admin?view=whitelist", status_code=HTTP_302_FOUND)


@router.post("/admin/whitelist/remove", include_in_schema=False)
async def admin_remove_whitelist(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    try:
        target = int(form.get("id"))
    except Exception:
        return RedirectResponse("/admin", status_code=HTTP_302_FOUND)
    ids = [item for item in core.WHITELIST_IDS if item != target]
    if ids:
        core.WHITELIST_IDS = ids
        save_whitelist(ids)
    return RedirectResponse("/admin?view=whitelist", status_code=HTTP_302_FOUND)


@router.post("/admin/referrals/code", include_in_schema=False)
async def admin_referral_code(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    name = str(form.get("name") or "").strip()
    phone = str(form.get("phone") or "").strip()
    group = str(form.get("group") or "").strip()
    code_raw = str(form.get("code") or "").strip()
    code = normalize_referral_code(code_raw)
    if not code:
        return referral_redirect(error="Укажите реферальный код")
    if len(code) > 16:
        return referral_redirect(error="Код не должен превышать 16 символов")
    if not phone:
        return referral_redirect(error="Укажите телефон участника")

    data = load_referrals()
    students = data.get("students") or {}
    if not isinstance(students, dict):
        students = {}
    code_owner = find_student_by_code(students, code)
    if code_owner and normalize_phone(code_owner.get("phone", "")) != normalize_phone(phone):
        return referral_redirect(error="Этот код уже занят")

    student = find_student_by_phone(students, phone)
    now_ts = int(time.time())
    if not student:
        student_id = next_student_id(students)
        student = {
            "id": student_id,
            "created_at": now_ts,
            "months": {},
            "discount_applied": [],
        }
        students[str(student_id)] = student

    if name:
        student["name"] = name
    if group:
        student["group"] = group
    student["phone"] = phone
    student["phone_norm"] = normalize_phone(phone)
    student["updated_at"] = now_ts
    student["referral_code"] = code
    student["referral_code_created_at"] = now_ts

    audit = data.get("audit") if isinstance(data.get("audit"), list) else []
    actor = (request.session.get("user") or {}).get("id") or ""
    audit.append(
        {
            "ts": now_ts,
            "action": "referral_code",
            "student_id": student.get("id"),
            "code": code,
            "actor": actor,
        }
    )
    data["students"] = students
    data["audit"] = audit
    save_referrals(data)

    await notify_admins(
        f"🏷 <b>Код участника</b>\n"
        f"👤 <b>Участник:</b> {student.get('name') or '—'}\n"
        f"📞 <b>Телефон:</b> {student.get('phone') or '—'}\n"
        f"🔗 <b>Код:</b> {code}"
    )
    return referral_redirect(message="Код сохранён")


@router.post("/admin/referrals/assign", include_in_schema=False)
async def admin_referral_assign(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    student_id_raw = str(form.get("student_id") or "").strip()
    phone = str(form.get("phone") or "").strip()
    code_raw = str(form.get("code") or "").strip()
    code = normalize_referral_code(code_raw)
    if not code:
        return referral_redirect(error="Укажите код участника")

    data = load_referrals()
    students = data.get("students") or {}
    if not isinstance(students, dict):
        students = {}
    referrer = find_student_by_code(students, code)
    if not referrer:
        return referral_redirect(error="Код участника не найден")

    student = None
    if student_id_raw:
        student = students.get(str(student_id_raw))
    if not student and phone:
        student = find_student_by_phone(students, phone)
    if not student:
        return referral_redirect(error="Ученик не найден")

    if str(student.get("id")) == str(referrer.get("id")):
        return referral_redirect(error="Нельзя назначить участника самому себе")

    now_ts = int(time.time())
    student["referrer_id"] = referrer.get("id")
    student["referrer_code"] = code
    student["referrer_assigned_at"] = now_ts
    student["updated_at"] = now_ts

    audit = data.get("audit") if isinstance(data.get("audit"), list) else []
    actor = (request.session.get("user") or {}).get("id") or ""
    audit.append(
        {
            "ts": now_ts,
            "action": "referral_assign",
            "student_id": student.get("id"),
            "referrer_id": referrer.get("id"),
            "code": code,
            "actor": actor,
        }
    )
    data["students"] = students
    data["audit"] = audit
    save_referrals(data)

    await notify_admins(
        f"🤝 <b>Назначен реферал</b>\n"
        f"👤 <b>Реферал:</b> {student.get('name') or '—'}\n"
        f"📞 <b>Телефон:</b> {student.get('phone') or '—'}\n"
        f"🔗 <b>Код:</b> {code}\n"
        f"🏷 <b>Участник:</b> {referrer.get('name') or '—'}"
    )
    return referral_redirect(message="Реферал привязан")


@router.post("/admin/referrals/month", include_in_schema=False)
async def admin_referral_month(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    student_id = str(form.get("student_id") or "").strip()
    month = str(form.get("month") or "").strip()
    group = str(form.get("group") or "").strip()
    paid = bool(form.get("paid"))
    attended = bool(form.get("attended"))
    if not student_id:
        return referral_redirect(error="Не указан ученик")
    if not re.match(r"^\\d{4}-\\d{2}$", month):
        return referral_redirect(error="Некорректный месяц")

    data = load_referrals()
    students = data.get("students") or {}
    if not isinstance(students, dict):
        students = {}
    student = students.get(str(student_id))
    if not student:
        return referral_redirect(error="Ученик не найден")

    months = student.get("months") if isinstance(student.get("months"), dict) else {}
    entry = months.get(month) if isinstance(months.get(month), dict) else {}
    was_confirmed = bool(entry.get("paid")) and bool(entry.get("attended"))
    now_ts = int(time.time())
    if paid or attended:
        entry["paid"] = paid
        entry["attended"] = attended
        entry["updated_at"] = now_ts
        months[month] = entry
    else:
        months.pop(month, None)
    student["months"] = months
    if group:
        student["group"] = group
    student["updated_at"] = now_ts

    audit = data.get("audit") if isinstance(data.get("audit"), list) else []
    actor = (request.session.get("user") or {}).get("id") or ""
    audit.append(
        {
            "ts": now_ts,
            "action": "referral_month",
            "student_id": student.get("id"),
            "month": month,
            "paid": paid,
            "attended": attended,
            "actor": actor,
        }
    )
    data["students"] = students
    data["audit"] = audit
    save_referrals(data)

    is_confirmed = paid and attended
    if is_confirmed and not was_confirmed:
        referrer = students.get(str(student.get("referrer_id")))
        balance_note = ""
        if referrer:
            stats = referral_stats_for_referrer(referrer, students)
            balance_note = f"\n🧮 <b>Баланс:</b> {stats['balance']}%"
        await notify_admins(
            f"✅ <b>Месяц подтверждён</b>\n"
            f"👤 <b>Реферал:</b> {student.get('name') or '—'}\n"
            f"📞 <b>Телефон:</b> {student.get('phone') or '—'}\n"
            f"📅 <b>Месяц:</b> {month}\n"
            f"🏷 <b>Участник:</b> {(referrer or {}).get('name') or '—'}"
            f"{balance_note}"
        )

    return referral_redirect(message="Данные обновлены")


@router.post("/admin/referrals/discount", include_in_schema=False)
async def admin_referral_discount(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    form = await request.form()
    student_id = str(form.get("student_id") or "").strip()
    percent_raw = str(form.get("percent") or "").strip()
    note = str(form.get("note") or "").strip()
    if not student_id:
        return referral_redirect(error="Не указан участник")

    data = load_referrals()
    students = data.get("students") or {}
    if not isinstance(students, dict):
        students = {}
    student = students.get(str(student_id))
    if not student:
        return referral_redirect(error="Участник не найден")

    stats = referral_stats_for_referrer(student, students)
    balance = stats["balance"]
    if balance <= 0:
        return referral_redirect(error="Нет доступной скидки")

    percent = balance
    if percent_raw:
        try:
            percent = int(percent_raw)
        except Exception:
            return referral_redirect(error="Некорректное значение скидки")
    if percent <= 0:
        return referral_redirect(error="Скидка должна быть больше 0")
    if percent > balance:
        percent = balance

    applied_list = student.get("discount_applied") if isinstance(student.get("discount_applied"), list) else []
    now_ts = int(time.time())
    applied_list.append({"ts": now_ts, "percent": percent, "note": note})
    student["discount_applied"] = applied_list
    student["updated_at"] = now_ts

    audit = data.get("audit") if isinstance(data.get("audit"), list) else []
    actor = (request.session.get("user") or {}).get("id") or ""
    audit.append(
        {
            "ts": now_ts,
            "action": "referral_discount",
            "student_id": student.get("id"),
            "percent": percent,
            "note": note,
            "actor": actor,
        }
    )
    data["students"] = students
    data["audit"] = audit
    save_referrals(data)

    await notify_admins(
        f"💸 <b>Скидка применена</b>\n"
        f"👤 <b>Участник:</b> {student.get('name') or '—'}\n"
        f"📞 <b>Телефон:</b> {student.get('phone') or '—'}\n"
        f"🎯 <b>Скидка:</b> {percent}%\n"
        f"🧮 <b>Баланс:</b> {max(stats['balance'] - percent, 0)}%"
    )

    return referral_redirect(message="Скидка применена")


@router.get("/admin/export/leads.csv", include_in_schema=False)
async def export_leads(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    course = request.query_params.get("course") or ""
    date_from = parse_date(request.query_params.get("date_from"))
    date_to = parse_date(request.query_params.get("date_to"))
    query = (request.query_params.get("q") or "").strip()
    status_filter = request.query_params.get("status") or ""
    source_filter = request.query_params.get("source") or ""
    sort = request.query_params.get("sort") or "date"
    order = request.query_params.get("order") or "desc"

    leads = filter_items(load_leads(), course, date_from, date_to)
    leads = apply_search(leads, query, ["name", "contact", "course", "page"])
    leads = [{**item, "_source": extract_source(item.get("page", ""))} for item in leads]
    if status_filter and status_filter in STATUS_META:
        leads = [item for item in leads if status_from_item(item)[0] == status_filter]
    if source_filter:
        leads = [item for item in leads if item.get("_source") == source_filter]

    def lead_sort_key(item: Dict[str, Any]):
        status_key, _, _ = status_from_item(item)
        order_map = {
            "new": 0,
            "contacted": 1,
            "qualified": 2,
            "call_scheduled": 3,
            "paid": 4,
            "lost": 5,
            "in_progress": 2,
            "closed": 6,
            "archived": 7,
        }
        return (order_map.get(status_key, 3), safe_int(item.get("timestamp", 0)))

    lead_key_map = {
        "date": lambda item: safe_int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
        "status": lead_sort_key,
    }
    leads = sort_items(leads, sort, order, lead_key_map)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["timestamp", "name", "contact", "course", "page", "status"])
    for item in leads:
        _, status_label, _ = status_from_item(item)
        writer.writerow([
            item.get("timestamp"),
            item.get("name"),
            item.get("contact"),
            item.get("course"),
            item.get("page"),
            status_label,
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"},
    )


@router.get("/admin/export/agreements.csv", include_in_schema=False)
async def export_agreements(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    course = request.query_params.get("course") or ""
    date_from = parse_date(request.query_params.get("date_from"))
    date_to = parse_date(request.query_params.get("date_to"))
    query = (request.query_params.get("q") or "").strip()
    agreement_status_filter = request.query_params.get("agreement_status") or ""
    contract_status_filter = request.query_params.get("contract_status") or ""
    sort = request.query_params.get("sort") or "date"
    order = request.query_params.get("order") or "desc"

    agreements = filter_items(load_agreements(), course, date_from, date_to)
    agreements = apply_search(agreements, query, ["full_name", "phone", "email", "telegram", "course"])
    if agreement_status_filter and agreement_status_filter in AGREEMENT_STATUS_META:
        agreements = [item for item in agreements if agreement_status_from_item(item)[0] == agreement_status_filter]
    if contract_status_filter and contract_status_filter in core.CONTRACT_STATUS_META:
        agreements = [item for item in agreements if contract_status_from_item(item)[0] == contract_status_filter]

    def agreement_status_sort_key(item: Dict[str, Any]):
        status_key, _, _ = agreement_status_from_item(item)
        order_map = {
            "signed": 0,
            "paid": 1,
            "review": 2,
            "canceled": 3,
        }
        return (order_map.get(status_key, 4), safe_int(item.get("timestamp", 0)))

    def contract_status_sort_key(item: Dict[str, Any]):
        status_key, _, _ = contract_status_from_item(item)
        order_map = {
            "draft": 0,
            "sent": 1,
            "signed": 2,
        }
        return (order_map.get(status_key, 3), safe_int(item.get("timestamp", 0)))

    agreement_key_map = {
        "date": lambda item: safe_int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("full_name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
        "status": agreement_status_sort_key,
        "contract": contract_status_sort_key,
    }
    agreements = sort_items(agreements, sort, order, agreement_key_map)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "timestamp",
            "course",
            "full_name",
            "contract_number",
            "phone",
            "email",
            "telegram",
            "amount",
            "status",
            "contract_token",
            "contract_status",
            "contract_channel",
            "contract_sent_at",
            "contract_signed_at",
            "total_lessons",
            "paid_lessons",
            "current_module",
            "materials",
        ]
    )
    for item in agreements:
        _, status_label, _ = agreement_status_from_item(item)
        writer.writerow([
            item.get("timestamp"),
            item.get("course"),
            item.get("full_name"),
            item.get("contract_number"),
            item.get("phone"),
            item.get("email"),
            item.get("telegram"),
            item.get("amount"),
            status_label,
            item.get("contract_token"),
            item.get("contract_status"),
            item.get("contract_channel"),
            item.get("contract_sent_at"),
            item.get("contract_signed_at"),
            item.get("total_lessons"),
            item.get("paid_lessons"),
            item.get("current_module"),
            materials_to_text(item.get("materials")),
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=agreements.csv"},
    )


@router.get("/admin/export/referrers.csv", include_in_schema=False)
async def export_referrers(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    data = load_referrals()
    students = data.get("students") or {}
    if not isinstance(students, dict):
        students = {}

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "participant_id",
        "name",
        "phone",
        "code",
        "referrals",
        "confirmed_months",
        "earned_percent",
        "applied_percent",
        "balance_percent",
    ])
    for item in students.values():
        if not normalize_referral_code(item.get("referral_code", "")):
            continue
        stats = referral_stats_for_referrer(item, students)
        writer.writerow([
            item.get("id"),
            item.get("name"),
            item.get("phone"),
            item.get("referral_code"),
            stats["referrals_count"],
            stats["confirmed_months"],
            stats["earned"],
            stats["applied"],
            stats["balance"],
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=referrers.csv"},
    )


@router.get("/admin/export/referrals.csv", include_in_schema=False)
async def export_referrals(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    data = load_referrals()
    students = data.get("students") or {}
    if not isinstance(students, dict):
        students = {}

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "student_id",
        "name",
        "phone",
        "group",
        "referrer_id",
        "referrer_name",
        "referrer_code",
        "confirmed_months",
    ])
    for item in students.values():
        if not item.get("referrer_id"):
            continue
        referrer = students.get(str(item.get("referrer_id"))) if students else None
        writer.writerow([
            item.get("id"),
            item.get("name"),
            item.get("phone"),
            item.get("group"),
            (referrer or {}).get("id"),
            (referrer or {}).get("name"),
            (referrer or {}).get("referral_code"),
            referral_confirmed_months(item),
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=referrals.csv"},
    )


@router.get("/admin/export/users.csv", include_in_schema=False)
async def export_users(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "email", "name", "provider"])
    users = load_json(USERS_FILE, {})
    for item in users.values():
        if not isinstance(item, dict):
            continue
        writer.writerow([
            item.get("id"),
            item.get("email"),
            item.get("name"),
            item.get("provider"),
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=users.csv"},
    )
