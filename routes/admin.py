import csv
import html
import io
import logging
import math
import re
import traceback
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from starlette.status import HTTP_302_FOUND

import core
from core import (
    USERS_FILE,
    admin_required,
    filter_items,
    get_admin_ids,
    load_agreements,
    load_json,
    load_leads,
    load_metrics,
    parse_date,
    render,
    save_whitelist,
    update_agreement_status,
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
def admin_panel(request: Request):
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
    allowed_views = {"overview", "leads", "agreements", "users", "whitelist"}
    if view not in allowed_views:
        view = "overview"
    course = request.query_params.get("course") or ""
    date_from_value = request.query_params.get("date_from", "")
    date_to_value = request.query_params.get("date_to", "")
    query = (request.query_params.get("q") or "").strip()
    status_filter = request.query_params.get("status") or ""
    source_filter = request.query_params.get("source") or ""
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

    status_counts = {key: 0 for key in STATUS_META}
    source_counts: Dict[str, int] = {}
    for item in leads:
        status_key, _, _ = status_from_item(item)
        status_counts[status_key] = status_counts.get(status_key, 0) + 1
        source = item.get("_source") or "Прямой"
        source_counts[source] = source_counts.get(source, 0) + 1

    if status_filter and status_filter not in STATUS_META:
        status_filter = ""
    if status_filter:
        leads = [item for item in leads if status_from_item(item)[0] == status_filter]
    if source_filter:
        leads = [item for item in leads if item.get("_source") == source_filter]

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
        manual_status = (item.get("status") or "").strip()
        amount_display = format_amount(item.get("amount"))
        agreements_view.append(
            {
                **item,
                "display_time": format_ts(item.get("timestamp")),
                "status_label": status_label,
                "status_class": status_class,
                "status_key": status_key,
                "manual_status": manual_status,
                "amount_display": amount_display,
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
            "status_filters": status_filters,
            "source_filters": source_filters,
            "leads_pagination": leads_pagination,
            "agreements_pagination": agreements_pagination,
            "next_url": next_url,
            "recent_leads": recent_leads,
            "utm_sources": utm_sources_sorted,
            "utm_mediums": utm_mediums_sorted,
            "utm_campaigns": utm_campaigns_sorted,
            "stale_leads": stale_leads,
            "avg_response": avg_response,
        },
    )


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
    return RedirectResponse("/admin", status_code=HTTP_302_FOUND)


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
    return RedirectResponse("/admin", status_code=HTTP_302_FOUND)


@router.get("/admin/export/leads.csv", include_in_schema=False)
def export_leads(request: Request):
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
def export_agreements(request: Request):
    guard = admin_required(request)
    if guard:
        return guard
    course = request.query_params.get("course") or ""
    date_from = parse_date(request.query_params.get("date_from"))
    date_to = parse_date(request.query_params.get("date_to"))
    query = (request.query_params.get("q") or "").strip()
    sort = request.query_params.get("sort") or "date"
    order = request.query_params.get("order") or "desc"

    agreements = filter_items(load_agreements(), course, date_from, date_to)
    agreements = apply_search(agreements, query, ["full_name", "phone", "email", "telegram", "course"])

    agreement_key_map = {
        "date": lambda item: safe_int(item.get("timestamp", 0)),
        "name": lambda item: (item.get("full_name") or "").lower(),
        "course": lambda item: (item.get("course") or "").lower(),
    }
    agreements = sort_items(agreements, sort, order, agreement_key_map)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["timestamp", "course", "full_name", "phone", "email", "telegram", "amount", "status"])
    for item in agreements:
        _, status_label, _ = agreement_status_from_item(item)
        writer.writerow([
            item.get("timestamp"),
            item.get("course"),
            item.get("full_name"),
            item.get("phone"),
            item.get("email"),
            item.get("telegram"),
            item.get("amount"),
            status_label,
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=agreements.csv"},
    )


@router.get("/admin/export/users.csv", include_in_schema=False)
def export_users(request: Request):
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
